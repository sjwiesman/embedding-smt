package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RecordDifferTest {

    private static final Schema ROW = SchemaBuilder.struct()
            .field("title", Schema.STRING_SCHEMA)
            .field("body", Schema.STRING_SCHEMA)
            .field("views", Schema.INT32_SCHEMA)
            .build();

    private static Struct row(String title, String body, int views) {
        return new Struct(ROW).put("title", title).put("body", body).put("views", views);
    }

    @Test
    void noChangeReturnsEmptySet() {
        Struct before = row("a", "b", 1);
        Struct after = row("a", "b", 1);
        assertThat(RecordDiffer.changedColumns(before, after)).isEmpty();
    }

    @Test
    void detectsSingleChangedColumn() {
        Struct before = row("a", "b", 1);
        Struct after = row("a", "B-CHANGED", 1);
        assertThat(RecordDiffer.changedColumns(before, after)).containsExactly("body");
    }

    @Test
    void detectsMultipleChangedColumns() {
        Struct before = row("a", "b", 1);
        Struct after = row("A2", "b", 2);
        assertThat(RecordDiffer.changedColumns(before, after)).containsExactlyInAnyOrder("title", "views");
    }

    @Test
    void nullBeforeMeansEveryColumnChanged() {
        Struct after = row("a", "b", 1);
        assertThat(RecordDiffer.changedColumns(null, after))
                .containsExactlyInAnyOrder("title", "body", "views");
    }

    @Test
    void newColumnsInAfterAreReportedAsChanged() {
        Schema beforeSchema = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .build();
        Schema afterSchema = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .build();

        Struct before = new Struct(beforeSchema).put("title", "a");
        Struct after = new Struct(afterSchema).put("title", "a").put("body", "b");

        assertThat(RecordDiffer.changedColumns(before, after)).containsExactly("body");
    }

    @Test
    void removedColumnsAreReportedAsChanged() {
        Schema beforeSchema = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .field("body", Schema.STRING_SCHEMA)
                .build();
        Schema afterSchema = SchemaBuilder.struct()
                .field("title", Schema.STRING_SCHEMA)
                .build();

        Struct before = new Struct(beforeSchema).put("title", "a").put("body", "b");
        Struct after = new Struct(afterSchema).put("title", "a");

        assertThat(RecordDiffer.changedColumns(before, after)).containsExactly("body");
    }
}
