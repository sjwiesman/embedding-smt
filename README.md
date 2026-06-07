# Perfect Embeddings

Perfect Embeddings is a Kafka Connect SMT that keeps search indexes and vector databases
perfectly synchronized with the search documents maintained by Materialize.

Materialize emits change events that identify exactly which columns changed. The transform
uses that information to recompute embeddings only for modified text fields while preserving
existing embeddings for everything else.

The result is an always fresh search index that remains correct as your data changes,
without repeatedly paying to re-embed unchanged content.

---

## Compatibility

| | |
|---|---|
| **Java** | 11+ (compiled to `release 11`) |
| **Kafka / Connect** | built against `connect-api` 3.8.1; works on any Connect runtime with [KIP-146](https://cwiki.apache.org/confluence/display/KAFKA/KIP-146+-+Classloading+Isolation+in+Connect) classloader isolation (Apache Kafka 2.3+ / Confluent Platform 5.3+) |
| **Packaging** | self-contained plugin folder; bundles Jackson, Connect API is `provided` by the worker |

---

## Example

The transform reads a topic produced by a Materialize Kafka sink.

```sql
CREATE CONNECTION kafka_connection TO KAFKA (BROKER 'redpanda:9092', SECURITY PROTOCOL = 'PLAINTEXT');
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (URL 'http://redpanda:8081');

-- Article content, ingested from your operational database.
CREATE TABLE article_content (id INT, title TEXT, body TEXT);

-- A high-volume stream of page-view events.
CREATE TABLE page_views (article_id INT);

-- The search document Materialize keeps up to date: article content plus a
-- live view count computed from the page-view stream.
CREATE MATERIALIZED VIEW articles AS
    SELECT c.id, c.title, c.body, count(pv.article_id) AS views
    FROM article_content c
    LEFT JOIN page_views pv ON pv.article_id = c.id
    GROUP BY c.id, c.title, c.body;

CREATE SINK articles_sink
    FROM articles
    INTO KAFKA CONNECTION kafka_connection (TOPIC 'articles-cdc')
    KEY (id) NOT ENFORCED    -- becomes the document ID
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
```

Materialize maintains `articles` incrementally. Each page-view event updates the `views`
count and produces a change event for that article carrying its current values. Its `title`
and `body` are untouched by a view, so the transform re-embeds nothing for those changes and
recomputes an embedding only when the text itself changes.

---

## Install

1. **Download** the plugin archive from the
   [Releases page](https://github.com/sjwiesman/embedding-smt/releases) —
   `perfect-embeddings-smt-<version>.zip` — and (optionally) verify its checksum:

   ```bash
   sha256sum -c perfect-embeddings-smt-<version>.zip.sha256
   ```

2. **Extract** the `perfect-embeddings-smt/` folder into a directory on the Connect worker's
   `plugin.path`:

   ```bash
   unzip perfect-embeddings-smt-<version>.zip -d /usr/local/share/kafka/plugins/
   ```

   This yields `/usr/local/share/kafka/plugins/perfect-embeddings-smt/lib/…`. Ensure the
   worker config includes that root:

   ```properties
   plugin.path=/usr/local/share/kafka/plugins
   ```

3. **Restart** the Connect worker(s) so the plugin is discovered.

4. **Add the transform** to your sink connector config (see below).

## Connector configuration

Add the SMT to the **sink** connector that reads the Materialize topic. A complete
Elasticsearch sink config for the `articles-cdc` topic above looks like this (JSON form,
as registered by the end-to-end test's `registerConnector()` in
[`EndToEndIT.java`](e2e/src/test/java/com/materialize/e2e/EndToEndIT.java)):

```json
{
  "name": "articles-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "articles-cdc",
    "connection.url": "http://elasticsearch:9200",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://redpanda:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://redpanda:8081",

    "key.ignore": "false",
    "schema.ignore": "false",
    "write.method": "UPSERT",
    "behavior.on.null.values": "delete",

    "transforms": "extractKey,embed",

    "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractKey.field": "id",

    "transforms.embed.type": "com.materialize.connect.smt.embedding.EmbeddingDiffTransform",
    "transforms.embed.embedded.columns": "title,body",
    "transforms.embed.provider": "openai",
    "transforms.embed.openai.api.key": "${file:/opt/secrets/connect.properties:openai_api_key}",
    "transforms.embed.openai.model": "text-embedding-3-small"
  }
}
```

**Transform ordering matters.** Materialize emits a *composite* Avro key (a struct of the
`KEY (…)` columns, e.g. `{ "id": 1 }`). `ExtractField$Key` unwraps it to the scalar `id`
so the sink uses a stable, flat document ID; the `embed` transform then passes that key
through unchanged. List `extractKey` before `embed`.

The equivalent `.properties` form for the SMT portion alone:

```properties
transforms=extractKey,embed

transforms.extractKey.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.extractKey.field=id

transforms.embed.type=com.materialize.connect.smt.embedding.EmbeddingDiffTransform
transforms.embed.embedded.columns=title,body
transforms.embed.provider=openai
transforms.embed.openai.api.key=${file:/opt/secrets/connect.properties:openai_api_key}
transforms.embed.openai.model=text-embedding-3-small
```

### Configuration reference

| Key | Default | Description |
|---|---|---|
| `embedded.columns` | *(required)* | Comma-separated string columns to embed |
| `embedding.field.suffix` | `_embedding` | Output field = `<col>` + suffix |
| `provider` | `openai` | `EmbeddingProvider` name (selected via `ServiceLoader`) |
| `request.timeout.ms` | `30000` | Per-request timeout for the embedding call |
| `max.retries` | `5` | Retries for transient (429/5xx/IO) failures |
| `retry.backoff.ms` | `500` | Base backoff (exponential, capped) between retries |
| `openai.api.key` | *(required)* | OpenAI API key (Bearer token); use a [config provider](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets) for secrets |
| `openai.model` | `text-embedding-3-small` | OpenAI embedding model |
| `openai.endpoint` | `https://api.openai.com/v1/embeddings` | Override for proxies / Azure / gateways |
| `openai.dimensions` | *(unset)* | Optional output-dimension override |
| `metrics.id` | *(auto)* | Identifier used in the metrics MBean `ObjectName` (`id=...`). Defaults to an auto-assigned per-instance sequence; set it to a stable, readable value (e.g. the connector/transform alias) when running more than one instance in a worker |

---

## Metrics

The SMT registers a JMX MBean reporting how many embedding calls it avoided versus a naive
pipeline that re-embeds every configured column on every change. It's a plain MBean on the
platform MBeanServer, scraped like any other Kafka Connect JMX metric (Prometheus
[`jmx_exporter`](https://github.com/prometheus/jmx_exporter), JConsole, etc.).

**ObjectName:** `com.materialize.connect.smt.embedding:type=EmbeddingDiff,id=<metrics.id>`

| Attribute | Meaning |
|---|---|
| `EmbeddingsComputed` | Embedding API calls actually made |
| `EmbeddingsSkipped` | Embedding API calls avoided: dropped (unchanged) records, unchanged columns within a changed record, and changed-to-null columns |
| `EmbeddingsPossible` | Calls a naive re-embed-everything pipeline would have made (`= EmbeddingsComputed + EmbeddingsSkipped`) |
| `SkipRatio` | `EmbeddingsSkipped / EmbeddingsPossible` (0.0 when idle): e.g. `0.83` means 83% of embedding calls were avoided |

`EmbeddingsPossible` counts every configured embedded column present in each insert and
update. Deletes, tombstones, and empty records embed nothing, so `SkipRatio` measures the
share of embeddable work the transform avoided.

A Connect SMT has no access to the connector name or task id, so each instance gets a unique
`id` automatically. Set `metrics.id` to give it a stable, readable name when a worker runs
more than one instance (`tasks.max > 1`, or the SMT used by multiple connectors).

---

## Sink requirements

The synchronization guarantee holds only if the sink applies each change as a partial update
(UPSERT) keyed by the document ID, and the Kafka record key is that document ID. With a full
replace, the omitted unchanged columns would be lost.

**Elasticsearch sink:**

```properties
write.method=UPSERT
behavior.on.null.values=delete
```

**OpenSearch sink:**

```properties
index.write.method=UPSERT
behavior.on.null.values=delete
```

---

## Embedding SPI

OpenAI is the built-in provider. To use a different backend, implement the embedding
service-provider interface (SPI), a small, dependency-free artifact you can depend on
without pulling in the SMT.

```xml
<dependency>
  <groupId>com.materialize</groupId>
  <artifactId>perfect-embeddings-spi</artifactId>
  <version>0.1.0</version>
</dependency>
```

Implement `com.materialize.embedding.spi.EmbeddingProvider` and register it for
`java.util.ServiceLoader` by adding its fully-qualified class name to
`META-INF/services/com.materialize.embedding.spi.EmbeddingProvider`. Select it at runtime
with `transforms.embed.provider=<your name()>`. Throw `RetriableEmbeddingException` for
transient failures (timeouts, 429, 5xx) and `FatalEmbeddingException` for permanent ones;
the SMT retries the former and fails fast on the latter.

This repository is a Maven reactor: `perfect-embeddings-spi/` (the published SPI) and
`perfect-embeddings-smt/` (the Connect plugin, which bundles the SPI). `mvn clean package`
at the root builds both.