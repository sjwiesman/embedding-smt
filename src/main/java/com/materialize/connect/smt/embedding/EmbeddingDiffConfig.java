package com.materialize.connect.smt.embedding;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Typed view over the SMT configuration. */
public final class EmbeddingDiffConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("before.field", Type.STRING, "before", Importance.MEDIUM,
                    "Envelope field holding the prior row state.")
            .define("after.field", Type.STRING, "after", Importance.MEDIUM,
                    "Envelope field holding the new row state.")
            .define("embedded.columns", Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "Comma-separated string columns to embed.")
            .define("embedding.field.suffix", Type.STRING, "_embedding", Importance.LOW,
                    "Suffix appended to a column name to form its embedding field.")
            .define("provider", Type.STRING, "openai", Importance.MEDIUM,
                    "EmbeddingProvider name selected via ServiceLoader.")
            .define("request.timeout.ms", Type.INT, 30000, Importance.LOW,
                    "Per-request timeout for the embedding service call.")
            .define("max.retries", Type.INT, 5, Importance.LOW,
                    "Maximum retries for transient embedding failures.")
            .define("retry.backoff.ms", Type.LONG, 500L, Importance.LOW,
                    "Base backoff (exponential) between retries.")
            .define("openai.api.key", Type.PASSWORD, null, Importance.HIGH,
                    "OpenAI API key (Bearer token).")
            .define("openai.model", Type.STRING, "text-embedding-3-small", Importance.MEDIUM,
                    "OpenAI embedding model.")
            .define("openai.endpoint", Type.STRING, "https://api.openai.com/v1/embeddings",
                    Importance.LOW, "OpenAI embeddings endpoint URL.")
            .define("openai.dimensions", Type.INT, null, Importance.LOW,
                    "Optional output-dimension override.");

    public EmbeddingDiffConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String beforeField() { return getString("before.field"); }

    public String afterField() { return getString("after.field"); }

    public String embeddingFieldSuffix() { return getString("embedding.field.suffix"); }

    public String providerName() { return getString("provider"); }

    public int maxRetries() { return getInt("max.retries"); }

    public long retryBackoffMs() { return getLong("retry.backoff.ms"); }

    public Set<String> embeddedColumns() {
        return new LinkedHashSet<>(getList("embedded.columns"));
    }
}
