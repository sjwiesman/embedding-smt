package com.materialize.connect.smt.embedding;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

/**
 * The factory and cache for the Connect schemas of the diff records the SMT emits. Each output
 * schema describes the flat struct of changed columns (plus their embedding fields) for a given
 * input shape; reusing cached instances keeps the transform's hot path allocation-free for repeated
 * record shapes.
 */
public final class OutputSchemaCache {

  private static final Schema EMBEDDING_SCHEMA =
      SchemaBuilder.array(Schema.FLOAT32_SCHEMA).optional().build();

  private final Set<String> embeddedColumns;
  private final String suffix;
  private final Map<List<Object>, Schema> cache = new ConcurrentHashMap<>();

  public OutputSchemaCache(Set<String> embeddedColumns, String suffix) {
    this.embeddedColumns = embeddedColumns;
    this.suffix = suffix;
  }

  public Schema schemaFor(Schema beforeSchema, Schema afterSchema, Set<String> changedColumns) {
    List<Object> key = Arrays.asList(beforeSchema, afterSchema, new TreeSet<>(changedColumns));
    return cache.computeIfAbsent(key, k -> build(beforeSchema, afterSchema, changedColumns));
  }

  private Schema build(Schema beforeSchema, Schema afterSchema, Set<String> changedColumns) {
    SchemaBuilder builder = SchemaBuilder.struct();
    for (Field field : afterSchema.fields()) {
      if (!changedColumns.contains(field.name())) {
        continue;
      }
      builder.field(field.name(), field.schema());
      if (embeddedColumns.contains(field.name())) {
        builder.field(field.name() + suffix, EMBEDDING_SCHEMA);
      }
    }
    if (beforeSchema != null) {
      for (Field field : beforeSchema.fields()) {
        if (afterSchema.field(field.name()) != null || !changedColumns.contains(field.name())) {
          continue;
        }
        builder.field(field.name(), nullableCopy(field.schema()));
        if (embeddedColumns.contains(field.name())) {
          builder.field(field.name() + suffix, EMBEDDING_SCHEMA);
        }
      }
    }
    return builder.build();
  }

  private static Schema nullableCopy(Schema schema) {
    SchemaBuilder builder;
    switch (schema.type()) {
      case ARRAY:
        builder = SchemaBuilder.array(schema.valueSchema());
        break;
      case MAP:
        builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
        break;
      default:
        builder = SchemaBuilder.type(schema.type());
        break;
    }
    if (schema.name() != null) {
      builder.name(schema.name());
    }
    if (schema.version() != null) {
      builder.version(schema.version());
    }
    if (schema.doc() != null) {
      builder.doc(schema.doc());
    }
    if (schema.parameters() != null) {
      builder.parameters(schema.parameters());
    }
    if (schema.type() == Schema.Type.STRUCT) {
      for (Field field : schema.fields()) {
        builder.field(field.name(), field.schema());
      }
    }
    return builder.optional().build();
  }
}
