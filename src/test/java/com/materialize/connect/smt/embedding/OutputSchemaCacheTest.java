package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

class OutputSchemaCacheTest {

    private static final Schema AFTER = SchemaBuilder.struct()
            .field("title", Schema.STRING_SCHEMA)
            .field("body", Schema.STRING_SCHEMA)
            .field("views", Schema.INT32_SCHEMA)
            .build();

    private static Set<String> set(String... values) {
        return new LinkedHashSet<>(java.util.Arrays.asList(values));
    }

    @Test
    void prunedSchemaContainsOnlyChangedColumnsPlusEmbeddings() {
        OutputSchemaCache cache = new OutputSchemaCache(set("title", "body"), "_embedding");
        Schema schema = cache.schemaFor(AFTER, AFTER, set("body", "views"));

        // changed source columns: body, views
        assertThat(schema.field("body")).isNotNull();
        assertThat(schema.field("views")).isNotNull();
        assertThat(schema.field("title")).isNull(); // unchanged -> omitted

        // body is embedded and changed -> body_embedding present, optional, ARRAY<FLOAT32>
        Schema embeddingField = schema.field("body_embedding").schema();
        assertThat(embeddingField.type()).isEqualTo(Schema.Type.ARRAY);
        assertThat(embeddingField.valueSchema().type()).isEqualTo(Schema.Type.FLOAT32);
        assertThat(embeddingField.isOptional()).isTrue();

        // views is not embedded -> no views_embedding
        assertThat(schema.field("views_embedding")).isNull();
    }

    @Test
    void sameChangedSetReturnsCachedInstance() {
        OutputSchemaCache cache = new OutputSchemaCache(set("title", "body"), "_embedding");
        Schema first = cache.schemaFor(AFTER, AFTER, set("body"));
        Schema second = cache.schemaFor(AFTER, AFTER, set("body"));
        assertThat(first).isSameAs(second);
    }

    @Test
    void differentAfterSchemasProduceDistinctSchemas() {
        OutputSchemaCache cache = new OutputSchemaCache(set("title", "body"), "_embedding");
        Schema evolved = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .field("views", Schema.INT64_SCHEMA) // type evolved INT32 -> INT64
                .build();

        Schema fromOriginal = cache.schemaFor(AFTER, AFTER, set("views"));
        Schema fromEvolved = cache.schemaFor(evolved, evolved, set("views"));

        assertThat(fromOriginal.field("views").schema().type()).isEqualTo(Schema.Type.INT32);
        assertThat(fromEvolved.field("views").schema().type()).isEqualTo(Schema.Type.INT64);
        assertThat(fromOriginal).isNotSameAs(fromEvolved);
    }

    @Test
    void removedColumnsUseNullableSchemaFromBefore() {
        OutputSchemaCache cache = new OutputSchemaCache(set("body"), "_embedding");
        Schema before = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .build();
        Schema after = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Schema schema = cache.schemaFor(before, after, set("body"));

        assertThat(schema.field("body")).isNotNull();
        assertThat(schema.field("body").schema().isOptional()).isTrue();
        assertThat(schema.field("body_embedding")).isNotNull();
    }
}
