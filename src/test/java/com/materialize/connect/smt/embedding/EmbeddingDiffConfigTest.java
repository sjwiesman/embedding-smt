package com.materialize.connect.smt.embedding;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(config.beforeField()).isEqualTo("before");
        assertThat(config.afterField()).isEqualTo("after");
        assertThat(config.embeddingFieldSuffix()).isEqualTo("_embedding");
        assertThat(config.providerName()).isEqualTo("openai");
        assertThat(config.maxRetries()).isEqualTo(5);
        assertThat(config.retryBackoffMs()).isEqualTo(500L);
        assertThat(config.embeddedColumns()).containsExactly("title", "body");
    }

    @Test
    void embeddedColumnsIsRequired() {
        Map<String, String> m = new HashMap<>();
        m.put("openai.api.key", "sk-test");
        assertThatThrownBy(() -> new EmbeddingDiffConfig(m))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void overridesAreParsed() {
        Map<String, String> m = minimal();
        m.put("before.field", "old");
        m.put("after.field", "new");
        m.put("max.retries", "9");
        EmbeddingDiffConfig config = new EmbeddingDiffConfig(m);
        assertThat(config.beforeField()).isEqualTo("old");
        assertThat(config.afterField()).isEqualTo("new");
        assertThat(config.maxRetries()).isEqualTo(9);
    }
}
