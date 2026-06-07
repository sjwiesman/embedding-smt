package com.materialize.connect.smt.embedding;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

/**
 * Stateless utility that identifies which columns changed between the {@code before} and {@code
 * after} states of a CDC row. The column-level diff at the heart of the transform — the set it
 * produces decides what the SMT emits and what it re-embeds.
 */
public final class RecordDiffer {

  private RecordDiffer() {}

  /**
   * Returns the names of fields in {@code after} whose value differs from the same field in {@code
   * before}. If {@code before} is null (create/snapshot), every field in {@code after} is
   * considered changed.
   */
  public static Set<String> changedColumns(Struct before, Struct after) {
    Set<String> changed = new LinkedHashSet<>();
    for (Field field : after.schema().fields()) {
      String name = field.name();
      Object afterValue = after.get(field);
      if (before == null) {
        changed.add(name);
        continue;
      }
      Field beforeField = before.schema().field(name);
      if (beforeField == null) {
        changed.add(name);
        continue;
      }
      Object beforeValue = before.get(beforeField);
      if (!Objects.equals(beforeValue, afterValue)) {
        changed.add(name);
      }
    }
    if (before != null) {
      for (Field field : before.schema().fields()) {
        if (after.schema().field(field.name()) == null) {
          changed.add(field.name());
        }
      }
    }
    return changed;
  }
}
