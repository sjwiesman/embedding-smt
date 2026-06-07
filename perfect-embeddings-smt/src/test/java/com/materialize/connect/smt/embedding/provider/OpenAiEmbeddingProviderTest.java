package com.materialize.connect.smt.embedding.provider;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.materialize.embedding.spi.FatalEmbeddingException;
import com.materialize.embedding.spi.RetriableEmbeddingException;
import java.util.List;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class OpenAiEmbeddingProviderTest {

  private MockWebServer server;

  @BeforeEach
  void setUp() throws Exception {
    server = new MockWebServer();
    server.start();
  }

  @AfterEach
  void tearDown() throws Exception {
    server.shutdown();
  }

  private OpenAiEmbeddingProvider provider() {
    OpenAiEmbeddingProvider p = new OpenAiEmbeddingProvider();
    p.configure(
        Map.of(
            "openai.api.key", "sk-test",
            "openai.model", "text-embedding-3-small",
            "openai.endpoint", server.url("/v1/embeddings").toString(),
            "request.timeout.ms", "5000"));
    return p;
  }

  @Test
  void nameIsOpenai() {
    assertThat(new OpenAiEmbeddingProvider().name()).isEqualTo("openai");
  }

  @Test
  void missingApiKeyFailsFastAtConfigureTime() {
    OpenAiEmbeddingProvider provider = new OpenAiEmbeddingProvider();
    assertThatThrownBy(
            () ->
                provider.configure(
                    Map.of(
                        "openai.model", "text-embedding-3-small",
                        "openai.endpoint", server.url("/v1/embeddings").toString(),
                        "request.timeout.ms", "5000")))
        .isInstanceOf(ConfigException.class);
  }

  @Test
  void sendsExpectedRequestAndParsesEmbedding() throws Exception {
    server.enqueue(
        new MockResponse()
            .setHeader("Content-Type", "application/json")
            .setBody("{\"data\":[{\"embedding\":[0.5,0.25,-1.0]}]}"));

    List<Float> vector = provider().embed("hello world");

    assertThat(vector).containsExactly(0.5f, 0.25f, -1.0f);

    RecordedRequest request = server.takeRequest();
    assertThat(request.getMethod()).isEqualTo("POST");
    assertThat(request.getHeader("Authorization")).isEqualTo("Bearer sk-test");
    assertThat(request.getHeader("Content-Type")).contains("application/json");
    String body = request.getBody().readUtf8();
    assertThat(body).contains("\"model\":\"text-embedding-3-small\"");
    assertThat(body).contains("\"input\":\"hello world\"");
  }

  @Test
  void rateLimitIsRetriable() {
    server.enqueue(new MockResponse().setResponseCode(429).setBody("{}"));
    assertThatThrownBy(() -> provider().embed("x")).isInstanceOf(RetriableEmbeddingException.class);
  }

  @Test
  void serverErrorIsRetriable() {
    server.enqueue(new MockResponse().setResponseCode(503).setBody("{}"));
    assertThatThrownBy(() -> provider().embed("x")).isInstanceOf(RetriableEmbeddingException.class);
  }

  @Test
  void badRequestIsFatal() {
    server.enqueue(new MockResponse().setResponseCode(400).setBody("{}"));
    assertThatThrownBy(() -> provider().embed("x")).isInstanceOf(FatalEmbeddingException.class);
  }

  @Test
  void unauthorizedIsFatal() {
    server.enqueue(new MockResponse().setResponseCode(401).setBody("{}"));
    assertThatThrownBy(() -> provider().embed("x")).isInstanceOf(FatalEmbeddingException.class);
  }
}
