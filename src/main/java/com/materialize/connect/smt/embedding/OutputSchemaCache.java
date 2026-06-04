package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

/** Builds and caches pruned output schemas keyed by (afterSchema, changedColumns). */
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

    public Schema schemaFor(Schema afterSchema, Set<String> changedColumns) {
        List<Object> key = Arrays.asList(afterSchema, new TreeSet<>(changedColumns));
        return cache.computeIfAbsent(key, k -> build(afterSchema, changedColumns));
    }

    private Schema build(Schema afterSchema, Set<String> changedColumns) {
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
        return builder.build();
    }
}
