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
 * SMT that diffs before/after, drops unchanged records, embeds changed text columns, and emits a
 * flat struct of only the changed columns plus their embeddings.
 */
public class EmbeddingDiffTransform<R extends ConnectRecord<R>> implements Transformation<R> {

  private EmbeddingDiffConfig config;
  private String beforeField;
  private String afterField;
  private String suffix;
  private Set<String> embeddedColumns;
  private OutputSchemaCache schemaCache;
  private RetryingEmbeddingClient client;
  private EmbeddingProvider provider;

  @Override
  public void configure(Map<String, ?> configs) {
    this.config = new EmbeddingDiffConfig(configs);
    this.beforeField = config.beforeField();
    this.afterField = config.afterField();
    this.suffix = config.embeddingFieldSuffix();
    this.embeddedColumns = config.embeddedColumns();
    this.schemaCache = new OutputSchemaCache(embeddedColumns, suffix);
    this.provider = createProvider(config.providerName());
    this.provider.configure(config.originals());
    this.client =
        new RetryingEmbeddingClient(
            provider, config.maxRetries(), config.retryBackoffMs(), Thread::sleep);
  }

  /** Resolves the embedding provider by name via ServiceLoader. Overridable for tests. */
  protected EmbeddingProvider createProvider(String providerName) {
    ServiceLoader<EmbeddingProvider> loader =
        ServiceLoader.load(EmbeddingProvider.class, getClass().getClassLoader());
    for (EmbeddingProvider candidate : loader) {
      if (candidate.name().equals(providerName)) {
        return candidate;
      }
    }
    throw new ConfigException("No EmbeddingProvider registered for provider=" + providerName);
  }

  @Override
  public R apply(R record) {
    if (record.value() == null) {
      return record; // already a tombstone
    }
    Struct envelope = requireStruct(record.value());
    Struct before = envelope.getStruct(beforeField);
    Struct after = envelope.getStruct(afterField);

    if (after == null) {
      // delete -> tombstone; but a record with neither before nor after is a no-op
      return before == null ? null : tombstone(record);
    }

    Set<String> changed = RecordDiffer.changedColumns(before, after);
    if (changed.isEmpty()) {
      return null; // nothing changed -> drop
    }

    Schema outSchema =
        schemaCache.schemaFor(before == null ? null : before.schema(), after.schema(), changed);
    Struct outValue = new Struct(outSchema);
    for (Field field : after.schema().fields()) {
      String name = field.name();
      if (!changed.contains(name)) {
        continue;
      }
      Object value = after.get(field);
      outValue.put(name, value);
      if (embeddedColumns.contains(name)) {
        outValue.put(name + suffix, embedColumn(name, value));
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

    return record.newRecord(
        record.topic(),
        record.kafkaPartition(),
        record.keySchema(),
        record.key(),
        outSchema,
        outValue,
        record.timestamp());
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
    if (provider != null) {
      try {
        provider.close();
      } catch (Exception e) {
        throw new ConnectException("Failed to close embedding provider", e);
      }
    }
  }
}
