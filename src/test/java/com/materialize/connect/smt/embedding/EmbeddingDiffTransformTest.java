package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingDiffTransformTest {

    private static final Schema ROW = SchemaBuilder.struct().name("Row")
            .field("title", Schema.STRING_SCHEMA)
            .field("body", Schema.STRING_SCHEMA)
            .field("views", Schema.INT32_SCHEMA)
            .optional()
            .build();

    private static final Schema ENVELOPE = SchemaBuilder.struct().name("Envelope")
            .field("before", ROW)
            .field("after", ROW)
            .build();

    /** Records embed() inputs and returns a fixed vector. */
    private static final class StubProvider implements EmbeddingProvider {
        final List<String> inputs = new java.util.ArrayList<>();
        public String name() { return "stub"; }
        public void configure(Map<String, ?> configs) { }
        public List<Float> embed(String text) { inputs.add(text); return List.of(1.0f, 2.0f); }
    }

    /** Transform that injects a stub provider instead of using ServiceLoader. */
    private static final class TestableTransform extends EmbeddingDiffTransform<SourceRecord> {
        final StubProvider stub;
        TestableTransform(StubProvider stub) { this.stub = stub; }
        @Override
        protected EmbeddingProvider createProvider(String providerName) { return stub; }
    }

    private StubProvider stub;
    private TestableTransform transform;

    @BeforeEach
    void setUp() {
        stub = new StubProvider();
        transform = new TestableTransform(stub);
        Map<String, String> config = new HashMap<>();
        config.put("embedded.columns", "title,body");
        config.put("openai.api.key", "sk-test");
        transform.configure(config);
    }

    @AfterEach
    void tearDown() {
        transform.close();
    }

    private static Struct row(String title, String body, int views) {
        return new Struct(ROW).put("title", title).put("body", body).put("views", views);
    }

    private SourceRecord record(Struct before, Struct after) {
        Struct envelope = new Struct(ENVELOPE);
        if (before != null) envelope.put("before", before);
        if (after != null) envelope.put("after", after);
        return new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "doc-1", ENVELOPE, envelope);
    }

    private SourceRecord record(Schema envelopeSchema, Struct before, Struct after) {
        Struct envelope = new Struct(envelopeSchema);
        if (before != null) envelope.put("before", before);
        if (after != null) envelope.put("after", after);
        return new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "doc-1", envelopeSchema, envelope);
    }

    @Test
    void dropsRecordWhenNothingChanged() {
        SourceRecord out = transform.apply(record(row("a", "b", 1), row("a", "b", 1)));
        assertThat(out).isNull();
        assertThat(stub.inputs).isEmpty();
    }

    @Test
    void deleteBecomesTombstone() {
        SourceRecord out = transform.apply(record(row("a", "b", 1), null));
        assertThat(out).isNotNull();
        assertThat(out.value()).isNull();
        assertThat(out.valueSchema()).isNull();
        assertThat(out.key()).isEqualTo("doc-1");
        assertThat(stub.inputs).isEmpty();
    }

    @Test
    void bothNullIsDropped() {
        SourceRecord out = transform.apply(record(null, null));
        assertThat(out).isNull();
        assertThat(stub.inputs).isEmpty();
    }

    @Test
    void nullChangedEmbeddedColumnEmitsNullVectorWithoutEmbedding() {
        Schema optRow = SchemaBuilder.struct().name("OptRow")
                .field("body", SchemaBuilder.string().optional().build())
                .optional()
                .build();
        Schema optEnv = SchemaBuilder.struct().name("OptEnv")
                .field("before", optRow)
                .field("after", optRow)
                .build();

        Struct before = new Struct(optRow).put("body", "had-text");
        Struct after = new Struct(optRow).put("body", null);
        Struct env = new Struct(optEnv).put("before", before).put("after", after);
        SourceRecord in = new SourceRecord(null, null, "topic", 0,
                Schema.STRING_SCHEMA, "doc-1", optEnv, env);

        SourceRecord out = transform.apply(in);

        // body changed (text -> null), so the record flows
        assertThat(out).isNotNull();
        Struct value = (Struct) out.value();
        assertThat(value.schema().field("body")).isNotNull();
        assertThat(value.get("body")).isNull();
        // body is embedded, but value is null -> null vector, and NO embed() call made
        assertThat(value.schema().field("body_embedding")).isNotNull();
        assertThat(value.get("body_embedding")).isNull();
        assertThat(stub.inputs).isEmpty();
    }

    @Test
    void changedEmbeddedColumnIsEmbeddedAndEmittedFlat() {
        SourceRecord out = transform.apply(record(row("a", "b", 1), row("a", "B2", 1)));

        // only body changed; only body embedded
        assertThat(stub.inputs).containsExactly("B2");

        Struct value = (Struct) out.value();
        assertThat(value.schema().field("body")).isNotNull();
        assertThat(value.getString("body")).isEqualTo("B2");
        assertThat(value.schema().field("title")).isNull();   // unchanged -> omitted
        assertThat(value.schema().field("views")).isNull();   // unchanged -> omitted
        assertThat(value.getArray("body_embedding")).containsExactly(1.0f, 2.0f);
        assertThat(out.key()).isEqualTo("doc-1");
    }

    @Test
    void changedNonEmbeddedColumnEmittedWithoutEmbedding() {
        SourceRecord out = transform.apply(record(row("a", "b", 1), row("a", "b", 99)));
        assertThat(stub.inputs).isEmpty(); // views is not embedded
        Struct value = (Struct) out.value();
        assertThat(value.getInt32("views")).isEqualTo(99);
        assertThat(value.schema().field("views_embedding")).isNull();
    }

    @Test
    void createEmbedsAllEmbeddedColumns() {
        SourceRecord out = transform.apply(record(null, row("a", "b", 1)));
        assertThat(stub.inputs).containsExactlyInAnyOrder("a", "b");
        Struct value = (Struct) out.value();
        assertThat(value.getArray("title_embedding")).containsExactly(1.0f, 2.0f);
        assertThat(value.getArray("body_embedding")).containsExactly(1.0f, 2.0f);
    }

    @Test
    void addedColumnDoesNotCrashAndIsEmitted() {
        Schema beforeRow = SchemaBuilder.struct().name("BeforeRow")
                .field("title", Schema.STRING_SCHEMA)
                .build();
        Schema afterRow = SchemaBuilder.struct().name("AfterRow")
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .build();
        Schema envelope = SchemaBuilder.struct().name("AddedColumnEnvelope")
                .field("before", beforeRow)
                .field("after", afterRow)
                .build();

        Struct before = new Struct(beforeRow).put("title", "same");
        Struct after = new Struct(afterRow).put("title", "same").put("body", "new text");

        SourceRecord out = transform.apply(record(envelope, before, after));

        assertThat(stub.inputs).containsExactly("new text");
        Struct value = (Struct) out.value();
        assertThat(value.getString("body")).isEqualTo("new text");
        assertThat(value.getArray("body_embedding")).containsExactly(1.0f, 2.0f);
    }

    @Test
    void removedColumnEmitsNullToClearDownstreamValue() {
        Schema beforeRow = SchemaBuilder.struct().name("BeforeRowRemoved")
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .build();
        Schema afterRow = SchemaBuilder.struct().name("AfterRowRemoved")
                .field("title", Schema.STRING_SCHEMA)
                .build();
        Schema envelope = SchemaBuilder.struct().name("RemovedColumnEnvelope")
                .field("before", beforeRow)
                .field("after", afterRow)
                .build();

        Struct before = new Struct(beforeRow).put("title", "same").put("body", "old text");
        Struct after = new Struct(afterRow).put("title", "same");

        SourceRecord out = transform.apply(record(envelope, before, after));

        assertThat(stub.inputs).isEmpty();
        Struct value = (Struct) out.value();
        assertThat(value.schema().field("body")).isNotNull();
        assertThat(value.schema().field("body").schema().isOptional()).isTrue();
        assertThat(value.get("body")).isNull();
        assertThat(value.get("body_embedding")).isNull();
    }

    @Test
    void nonStringEmbeddedColumnThrows() {
        // reconfigure so an int column ("views") is embedded
        transform.close();
        transform = new TestableTransform(stub);
        Map<String, String> config = new HashMap<>();
        config.put("embedded.columns", "views");
        config.put("openai.api.key", "sk-test");
        transform.configure(config);

        assertThatThrownBy(() -> transform.apply(record(row("a", "b", 1), row("a", "b", 2))))
                .isInstanceOf(ConnectException.class);
    }
}
