package com.materialize.connect.smt.embedding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

class EmbeddingDiffConfigTest {

  private static Map<String, String> minimal() {
    Map<String, String> m = new HashMap<>();
    m.put("embedded.columns", "title,body");
    m.put("openai.api.key", "sk-test");
    return m;
  }

  @Test
  void appliesDefaults() {
    EmbeddingDiffConfig config = new EmbeddingDiffConfig(minimal());
    assertThat(config.embeddingFieldSuffix()).isEqualTo("_embedding");
    assertThat(config.providerName()).isEqualTo("openai");
    assertThat(config.maxRetries()).isEqualTo(5);
    assertThat(config.retryBackoffMs()).isEqualTo(500L);
    assertThat(config.embeddedColumns()).containsExactly("title", "body");
    assertThat(config.metricsId()).isNull();
  }

  @Test
  void metricsIdRoundTrips() {
    Map<String, String> m = minimal();
    m.put("metrics.id", "articles-embed");
    assertThat(new EmbeddingDiffConfig(m).metricsId()).isEqualTo("articles-embed");
  }

  @Test
  void embeddedColumnsIsRequired() {
    Map<String, String> m = new HashMap<>();
    m.put("openai.api.key", "sk-test");
    assertThatThrownBy(() -> new EmbeddingDiffConfig(m)).isInstanceOf(ConfigException.class);
  }

  @Test
  void overridesAreParsed() {
    Map<String, String> m = minimal();
    m.put("max.retries", "9");
    m.put("retry.backoff.ms", "250");
    EmbeddingDiffConfig config = new EmbeddingDiffConfig(m);
    assertThat(config.maxRetries()).isEqualTo(9);
    assertThat(config.retryBackoffMs()).isEqualTo(250L);
  }
}
