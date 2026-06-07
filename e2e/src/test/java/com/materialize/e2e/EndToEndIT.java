package com.materialize.e2e;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.redpanda.RedpandaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Full pipeline: Materialize -> Redpanda (Kafka + Schema Registry) -> Kafka Connect with this SMT
 * -> Elasticsearch, with an in-JVM mock OpenAI embeddings endpoint. The test drives Materialize
 * table writes over JDBC and asserts Elasticsearch state, mirroring the assertions the old Python
 * verifier made.
 */
class EndToEndIT {

  private static final Logger LOG = LoggerFactory.getLogger(EndToEndIT.class);

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final HttpClient HTTP = HttpClient.newHttpClient();

  private static final String TOPIC = "articles-cdc";
  private static final String CONNECTOR = "articles-elasticsearch-sink";

  // Internal (in-network) addresses.
  private static final String REDPANDA_INTERNAL = "redpanda:29092";
  private static final String SCHEMA_REGISTRY_INTERNAL = "http://redpanda:8081";
  private static final String ES_INTERNAL = "http://elasticsearch:9200";

  private static Network network;
  private static MockEmbeddingsServer embeddings;
  private static RedpandaContainer redpanda;
  private static GenericContainer<?> materialize;
  private static ElasticsearchContainer elasticsearch;
  private static GenericContainer<?> connect;

  @BeforeAll
  static void startStack() throws Exception {
    network = Network.newNetwork();

    embeddings = new MockEmbeddingsServer();
    embeddings.start();
    org.testcontainers.Testcontainers.exposeHostPorts(embeddings.port());

    redpanda =
        new RedpandaContainer("docker.redpanda.com/redpandadata/redpanda:v24.2.7")
            .withListener(REDPANDA_INTERNAL)
            .withNetwork(network);
    redpanda.start();

    materialize =
        new GenericContainer<>(DockerImageName.parse("materialize/materialized:v26.27.0"))
            .withNetwork(network)
            .withNetworkAliases("materialize")
            .withExposedPorts(6875)
            .waitingFor(Wait.forListeningPort().withStartupTimeout(ofMinutes(3)));
    materialize.start();

    elasticsearch =
        new ElasticsearchContainer(
                DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch:8.15.3"))
            .withNetwork(network)
            .withNetworkAliases("elasticsearch")
            .withEnv("discovery.type", "single-node")
            .withEnv("xpack.security.enabled", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m");
    elasticsearch.start();

    Path repoRoot = Paths.get("..").toAbsolutePath().normalize();
    ImageFromDockerfile connectImage =
        new ImageFromDockerfile()
            .withFileFromPath("pom.xml", repoRoot.resolve("pom.xml"))
            .withFileFromPath("LICENSE", repoRoot.resolve("LICENSE"))
            .withFileFromPath("NOTICE", repoRoot.resolve("NOTICE"))
            .withFileFromPath("README.md", repoRoot.resolve("README.md"))
            .withFileFromPath("perfect-embeddings-spi", repoRoot.resolve("perfect-embeddings-spi"))
            .withFileFromPath("perfect-embeddings-smt", repoRoot.resolve("perfect-embeddings-smt"))
            .withFileFromPath("Dockerfile", repoRoot.resolve("e2e/connect/Dockerfile"));

    connect =
        new GenericContainer<>(connectImage)
            .withNetwork(network)
            .withNetworkAliases("connect")
            .withExposedPorts(8083)
            .withEnv("CONNECT_BOOTSTRAP_SERVERS", REDPANDA_INTERNAL)
            .withEnv("CONNECT_GROUP_ID", "embedding-diff-example")
            .withEnv("CONNECT_CONFIG_STORAGE_TOPIC", "_connect-configs")
            .withEnv("CONNECT_OFFSET_STORAGE_TOPIC", "_connect-offsets")
            .withEnv("CONNECT_STATUS_STORAGE_TOPIC", "_connect-status")
            .withEnv("CONNECT_CONFIG_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_OFFSET_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_STATUS_STORAGE_REPLICATION_FACTOR", "1")
            .withEnv("CONNECT_KEY_CONVERTER", "io.confluent.connect.avro.AvroConverter")
            .withEnv("CONNECT_VALUE_CONVERTER", "io.confluent.connect.avro.AvroConverter")
            .withEnv("CONNECT_KEY_CONVERTER_SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INTERNAL)
            .withEnv("CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL", SCHEMA_REGISTRY_INTERNAL)
            .withEnv(
                "CONNECT_INTERNAL_KEY_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv(
                "CONNECT_INTERNAL_VALUE_CONVERTER", "org.apache.kafka.connect.json.JsonConverter")
            .withEnv("CONNECT_INTERNAL_KEY_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_INTERNAL_VALUE_CONVERTER_SCHEMAS_ENABLE", "false")
            .withEnv("CONNECT_REST_ADVERTISED_HOST_NAME", "connect")
            .withEnv("CONNECT_REST_PORT", "8083")
            .withEnv(
                "CONNECT_PLUGIN_PATH",
                "/usr/share/java,/usr/share/confluent-hub-components,"
                    + "/usr/share/local-connect-plugins")
            .withEnv("CONNECT_LOG4J_ROOT_LOGLEVEL", "INFO")
            .waitingFor(
                Wait.forHttp("/connectors")
                    .forPort(8083)
                    .forStatusCode(200)
                    .withStartupTimeout(ofMinutes(5)));
    connect.start();
  }

  @AfterAll
  static void stopStack() {
    closeQuietly(connect);
    closeQuietly(elasticsearch);
    closeQuietly(materialize);
    closeQuietly(redpanda);
    closeQuietly(embeddings);
    closeQuietly(network);
  }

  private static void closeQuietly(AutoCloseable resource) {
    if (resource == null) {
      return;
    }
    try {
      resource.close();
    } catch (Exception e) {
      LOG.warn("failed to close {}", resource, e);
    }
  }

  @Test
  void diffTransformPreservesUnchangedEmbeddings() throws Exception {
    applyMaterializeSetup();
    registerConnector();
    await().atMost(ofMinutes(2)).pollInterval(ofSeconds(2)).until(EndToEndIT::connectorRunning);

    // 1. Insert: full document with both embeddings.
    runSql("INSERT INTO articles VALUES (1, 'Hello world', 'First body text', 10);");
    JsonNode inserted =
        await()
            .atMost(ofMinutes(2))
            .pollInterval(ofSeconds(2))
            .until(
                EndToEndIT::fetchDoc,
                doc ->
                    doc != null
                        && "Hello world".equals(text(doc, "title"))
                        && "First body text".equals(text(doc, "body"))
                        && doc.path("views").asInt() == 10
                        && vector(doc, "title_embedding").equals(List.of(11.0, 87.0, 3.0))
                        && vector(doc, "body_embedding").equals(List.of(15.0, 470.0, 3.0)));

    List<Double> titleEmbedding = vector(inserted, "title_embedding");
    List<Double> bodyEmbedding = vector(inserted, "body_embedding");

    // 2. views-only update preserves both embeddings.
    runSql("UPDATE articles SET views = 42 WHERE id = 1;");
    await()
        .atMost(ofMinutes(2))
        .pollInterval(ofSeconds(2))
        .untilAsserted(
            () -> {
              JsonNode doc = fetchDoc();
              assertThat(doc).isNotNull();
              assertThat(doc.path("views").asInt()).isEqualTo(42);
              assertThat(vector(doc, "title_embedding")).isEqualTo(titleEmbedding);
              assertThat(vector(doc, "body_embedding")).isEqualTo(bodyEmbedding);
            });

    // 3. body update refreshes only the body embedding.
    runSql("UPDATE articles SET body = 'First body text updated' WHERE id = 1;");
    await()
        .atMost(ofMinutes(2))
        .pollInterval(ofSeconds(2))
        .untilAsserted(
            () -> {
              JsonNode doc = fetchDoc();
              assertThat(doc).isNotNull();
              assertThat(text(doc, "body")).isEqualTo("First body text updated");
              assertThat(vector(doc, "body_embedding")).isEqualTo(List.of(23.0, 248.0, 6.0));
              assertThat(vector(doc, "title_embedding")).isEqualTo(titleEmbedding);
            });
  }

  // --- Materialize ---------------------------------------------------------------

  private static String materializeJdbcUrl() {
    return "jdbc:postgresql://"
        + materialize.getHost()
        + ":"
        + materialize.getMappedPort(6875)
        + "/materialize";
  }

  private static void applyMaterializeSetup() {
    String[] statements = {
      "CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA ("
          + " BROKER '"
          + REDPANDA_INTERNAL
          + "', SECURITY PROTOCOL = 'PLAINTEXT');",
      "CREATE CONNECTION IF NOT EXISTS csr_connection TO CONFLUENT SCHEMA REGISTRY ("
          + " URL '"
          + SCHEMA_REGISTRY_INTERNAL
          + "');",
      "CREATE TABLE IF NOT EXISTS articles (id INT, title TEXT, body TEXT, views INT);",
      "CREATE SINK IF NOT EXISTS articles_sink FROM articles"
          + " INTO KAFKA CONNECTION kafka_connection (TOPIC '"
          + TOPIC
          + "')"
          + " KEY (id) NOT ENFORCED"
          + " FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection"
          + " ENVELOPE DEBEZIUM;"
    };
    // Materialize accepts connections as user "materialize", no password.
    await()
        .atMost(ofMinutes(2))
        .pollInterval(ofSeconds(2))
        .ignoreExceptions()
        .untilAsserted(
            () -> {
              try (Connection conn =
                  DriverManager.getConnection(materializeJdbcUrl(), "materialize", "")) {
                try (Statement stmt = conn.createStatement()) {
                  for (String sql : statements) {
                    stmt.execute(sql);
                  }
                }
              }
            });
  }

  private static void runSql(String sql) throws SQLException {
    try (Connection conn = DriverManager.getConnection(materializeJdbcUrl(), "materialize", "");
        Statement stmt = conn.createStatement()) {
      stmt.execute(sql);
    }
  }

  // --- Kafka Connect -------------------------------------------------------------

  private static String connectUrl() {
    return "http://" + connect.getHost() + ":" + connect.getMappedPort(8083);
  }

  private static void registerConnector() throws Exception {
    ObjectNode config = MAPPER.createObjectNode();
    config.put("connector.class", "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector");
    config.put("tasks.max", "1");
    config.put("topics", TOPIC);
    config.put("connection.url", ES_INTERNAL);
    config.put("key.ignore", "false");
    config.put("schema.ignore", "false");
    config.put("behavior.on.null.values", "delete");
    config.put("write.method", "UPSERT");
    config.put("transforms", "extractKey,embed");
    config.put(
        "transforms.extractKey.type", "org.apache.kafka.connect.transforms.ExtractField$Key");
    config.put("transforms.extractKey.field", "id");
    config.put(
        "transforms.embed.type", "com.materialize.connect.smt.embedding.EmbeddingDiffTransform");
    config.put("transforms.embed.embedded.columns", "title,body");
    config.put("transforms.embed.provider", "openai");
    config.put("transforms.embed.openai.api.key", "example-api-key");
    config.put(
        "transforms.embed.openai.endpoint",
        "http://host.testcontainers.internal:" + embeddings.port() + "/v1/embeddings");

    ObjectNode body = MAPPER.createObjectNode();
    body.put("name", CONNECTOR);
    body.set("config", config);

    HttpRequest request =
        HttpRequest.newBuilder(URI.create(connectUrl() + "/connectors"))
            .header("Content-Type", "application/json")
            .POST(
                HttpRequest.BodyPublishers.ofString(
                    MAPPER.writeValueAsString(body), StandardCharsets.UTF_8))
            .build();
    HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
    assertThat(response.statusCode())
        .as("connector registration response: %s", response.body())
        .isIn(200, 201, 409); // 409 == already exists on a retried run
  }

  private static boolean connectorRunning() {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(URI.create(connectUrl() + "/connectors/" + CONNECTOR + "/status"))
              .GET()
              .build();
      HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() != 200) {
        return false;
      }
      JsonNode status = MAPPER.readTree(response.body());
      if (!"RUNNING".equals(status.path("connector").path("state").asText())) {
        return false;
      }
      for (JsonNode task : status.path("tasks")) {
        if (!"RUNNING".equals(task.path("state").asText())) {
          return false;
        }
      }
      return status.path("tasks").size() > 0;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (IOException e) {
      return false;
    }
  }

  // --- Elasticsearch -------------------------------------------------------------

  private static JsonNode fetchDoc() {
    try {
      HttpRequest request =
          HttpRequest.newBuilder(
                  URI.create(
                      "http://"
                          + elasticsearch.getHost()
                          + ":"
                          + elasticsearch.getMappedPort(9200)
                          + "/"
                          + TOPIC
                          + "/_doc/1"))
              .GET()
              .build();
      HttpResponse<String> response = HTTP.send(request, HttpResponse.BodyHandlers.ofString());
      if (response.statusCode() == 404) {
        return null;
      }
      if (response.statusCode() != 200) {
        return null;
      }
      JsonNode body = MAPPER.readTree(response.body());
      return body.has("_source") ? body.get("_source") : null;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    } catch (IOException e) {
      return null;
    }
  }

  private static String text(JsonNode doc, String field) {
    JsonNode value = doc.get(field);
    return value == null || value.isNull() ? null : value.asText();
  }

  private static List<Double> vector(JsonNode doc, String field) {
    JsonNode array = doc.get(field);
    if (array == null || !array.isArray()) {
      return List.of();
    }
    Double[] values = new Double[array.size()];
    for (int i = 0; i < array.size(); i++) {
      values[i] = array.get(i).asDouble();
    }
    return List.of(values);
  }
}
