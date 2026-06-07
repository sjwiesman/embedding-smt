package com.materialize.connect.smt.embedding;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

/**
 * The plugin's Kafka Connect Single Message Transform — the class a Connect worker instantiates and
 * drives. It is the orchestrator of the embedding-diff pipeline: it turns a Materialize
 * Debezium-style {@code before}/{@code after} change envelope into the minimal, embedding-enriched
 * diff delivered to an UPSERT-mode sink, delegating the diff, output-schema construction,
 * embedding, and metrics to its collaborators. One instance exists per Connect task.
 *
 * @param <R> the {@link ConnectRecord} subtype (source or sink) the worker applies this transform
 *     to
 */
public class EmbeddingDiffTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String BEFORE_FIELD = "before";

  private static final String AFTER_FIELD = "after";

  private String suffix;
  private Set<String> embeddedColumns;
  private OutputSchemaCache schemaCache;
  private RetryingEmbeddingClient client;
  private EmbeddingProvider provider;
  private EmbeddingDiffMetrics metrics;

  @Override
  public void configure(Map<String, ?> configs) {
    var config = new EmbeddingDiffConfig(configs);
    this.suffix = config.embeddingFieldSuffix();
    this.embeddedColumns = config.embeddedColumns();
    this.schemaCache = new OutputSchemaCache(embeddedColumns, suffix);
    this.provider = createProvider(config.providerName());
    this.provider.configure(config.originals());
    this.client =
        new RetryingEmbeddingClient(
            provider, config.maxRetries(), config.retryBackoffMs(), Thread::sleep);
    this.metrics = new EmbeddingDiffMetrics();
    this.metrics.register(config.metricsId());
  }

  /** Resolves the embedding provider by name via ServiceLoader. Overridable for tests. */
  protected EmbeddingProvider createProvider(String providerName) {
    var loader = ServiceLoader.load(EmbeddingProvider.class, getClass().getClassLoader());
    for (var candidate : loader) {
      if (candidate.name().equals(providerName)) {
        return candidate;
      }
    }
    throw new ConfigException("No EmbeddingProvider registered for provider=" + providerName);
  }

  @Override
  public R apply(R record) {
    if (isTombstone(record)) {
      return record;
    }
    var envelope = requireStruct(record.value());
    var before = envelope.getStruct(BEFORE_FIELD);
    var after = envelope.getStruct(AFTER_FIELD);

    if (after == null) {
      // delete -> tombstone; but a record with neither before nor after is a no-op
      return before == null ? null : tombstone(record);
    }

    // Embedding calls a naive re-embed-everything pipeline would make for this record.
    int possible = embeddedColumnsIn(after.schema());

    var changed = RecordDiffer.changedColumns(before, after);
    if (changed.isEmpty()) {
      metrics.record(0, possible);
      return null;
    }

    Schema outSchema =
        schemaCache.schemaFor(before == null ? null : before.schema(), after.schema(), changed);
    Struct outValue = new Struct(outSchema);
    int computed = 0;
    for (Field field : after.schema().fields()) {
      String name = field.name();
      if (!changed.contains(name)) {
        continue;
      }
      Object value = after.get(field);
      outValue.put(name, value);
      if (embeddedColumns.contains(name)) {
        outValue.put(name + suffix, embedColumn(name, value));
        if (value != null) {
          computed++; // embedColumn calls the provider exactly when the value is non-null
        }
      }
    }
    if (before != null) {
      for (Field field : before.schema().fields()) {
        String name = field.name();
        if (after.schema().field(name) != null || !changed.contains(name)) {
          continue;
        }
        outValue.put(name, null);
        if (embeddedColumns.contains(name)) {
          outValue.put(name + suffix, null);
        }
      }
    }

    metrics.record(computed, possible);
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        outSchema,
        outValue,
        record.timestamp());
  }

  /** Number of configured embedded columns present in the given schema. */
  private int embeddedColumnsIn(Schema afterSchema) {
    int n = 0;
    for (String column : embeddedColumns) {
      if (afterSchema.field(column) != null) {
        n++;
      }
    }
    return n;
  }

  private List<Float> embedColumn(String column, Object value) {
    if (value == null) {
      return null; // changed-to-null: emit null vector (clears downstream vector)
    }
    if (!(value instanceof String)) {
      throw new ConnectException(
          "Embedded column '"
              + column
              + "' must be a string but was "
              + value.getClass().getName());
    }
    return client.embed((String) value);
  }

  private boolean isTombstone(R record) {
    return record.value() == null;
  }

  private R tombstone(R record) {
    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        null,
        null,
        record.timestamp());
  }

  private static Struct requireStruct(Object value) {
    if (!(value instanceof Struct)) {
      throw new ConnectException(
          "EmbeddingDiffTransform requires a Struct value but got "
              + (value == null ? "null" : value.getClass().getName()));
    }
    return (Struct) value;
  }

  @Override
  public ConfigDef config() {
    return EmbeddingDiffConfig.CONFIG_DEF;
  }

  @Override
  public void close() {
    if (metrics != null) {
      metrics.unregister();
    }
    if (provider != null) {
      try {
        provider.close();
      } catch (Exception e) {
        throw new ConnectException("Failed to close embedding provider", e);
      }
    }
  }

  /** Exposes the metrics for same-package tests. */
  EmbeddingDiffMetrics metrics() {
    return metrics;
  }
}
