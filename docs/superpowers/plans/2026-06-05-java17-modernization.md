# Java 17 Modernization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Refactor the embedding-diff SMT to idiomatic Java 17 with zero behavior change.

**Architecture:** This is a behavior-preserving refactor. The existing test suite is the
contract — there are NO new tests. The TDD rhythm is inverted: each task makes one focused
edit, then re-runs the suite to prove behavior is unchanged, then commits. Tasks are
ordered so each compiles and passes independently.

**Tech Stack:** Java 17, Maven, JUnit 5 + AssertJ, Kafka Connect API (provided), Jackson.

**Build note:** `java` is not on `PATH`. Every Maven command MUST set `JAVA_HOME`:
`JAVA_HOME=/opt/homebrew/opt/openjdk mvn test`

**Spec:** `docs/superpowers/specs/2026-06-05-java17-modernization-design.md`

---

## File Structure

No files change responsibility. One new file is added:

- **Create:** `src/main/java/com/materialize/connect/smt/embedding/EmbeddingException.java` —
  sealed base type for the two embedding-failure modes.
- **Modify:** `EmbeddingDiffTransform.java`, `OutputSchemaCache.java`,
  `RetriableEmbeddingException.java`, `FatalEmbeddingException.java`,
  `RetryingEmbeddingClient.java`, and the test
  `provider/OpenAiEmbeddingProviderTest.java`.

---

## Task 0: Capture green baseline

**Files:** none (verification only).

- [ ] **Step 1: Run the full suite to confirm a clean starting point**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn clean test`
Expected: `BUILD SUCCESS`, all tests pass. If anything fails here, STOP — the baseline
must be green before refactoring.

---

## Task 1: Pattern matching for `instanceof` + `var` — `EmbeddingDiffTransform`

**Files:**
- Modify: `src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransform.java`

- [ ] **Step 1: Rewrite `requireStruct` to use a binding pattern**

Replace this method:

```java
    private static Struct requireStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new ConnectException("EmbeddingDiffTransform requires a Struct value but got "
                    + (value == null ? "null" : value.getClass().getName()));
        }
        return (Struct) value;
    }
```

with:

```java
    private static Struct requireStruct(Object value) {
        if (value instanceof Struct struct) {
            return struct;
        }
        throw new ConnectException("EmbeddingDiffTransform requires a Struct value but got "
                + (value == null ? "null" : value.getClass().getName()));
    }
```

- [ ] **Step 2: Rewrite `embedColumn` to use a binding pattern**

Replace this method:

```java
    private List<Float> embedColumn(String column, Object value) {
        if (value == null) {
            return null;
        }
        if (!(value instanceof String)) {
            throw new ConnectException("Embedded column '" + column
                    + "' must be a string but was " + value.getClass().getName());
        }
        return client.embed((String) value);
    }
```

with:

```java
    private List<Float> embedColumn(String column, Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof String text) {
            return client.embed(text);
        }
        throw new ConnectException("Embedded column '" + column
                + "' must be a string but was " + value.getClass().getName());
    }
```

- [ ] **Step 3: Use `var` for the three obvious locals in `apply()`**

In `apply()`, change these three declarations (leave `Struct before`/`Struct after` and the
`for (Field field ...)` loops and `Object value` as-is — those stay explicit for clarity):

```java
        Set<String> changed = RecordDiffer.changedColumns(before, after);
```
→
```java
        var changed = RecordDiffer.changedColumns(before, after);
```

```java
        Schema outSchema = schemaCache.schemaFor(before == null ? null : before.schema(), after.schema(), changed);
        Struct outValue = new Struct(outSchema);
```
→
```java
        var outSchema = schemaCache.schemaFor(before == null ? null : before.schema(), after.schema(), changed);
        var outValue = new Struct(outSchema);
```

- [ ] **Step 4: Run tests**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test`
Expected: `BUILD SUCCESS`, all tests pass (behavior unchanged).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransform.java
git commit -m "refactor: use instanceof patterns and var in EmbeddingDiffTransform

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 2: Switch expression — `OutputSchemaCache.nullableCopy`

**Files:**
- Modify: `src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java`

- [ ] **Step 1: Convert the two `switch` statements in `nullableCopy`**

Replace the body of `nullableCopy` (the two `switch` blocks) so the method reads:

```java
    private static Schema nullableCopy(Schema schema) {
        SchemaBuilder builder = switch (schema.type()) {
            case ARRAY -> SchemaBuilder.array(schema.valueSchema());
            case MAP -> SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
            default -> SchemaBuilder.type(schema.type());
        };
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
```

Note: `case ARRAY`/`case MAP` in a switch over `schema.type()` (a `Schema.Type` enum) need
no qualifier. The STRUCT check uses the qualified constant `Schema.Type.STRUCT`.

- [ ] **Step 2: Run tests**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test -Dtest=OutputSchemaCacheTest`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java
git commit -m "refactor: switch expression in OutputSchemaCache.nullableCopy

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 3: Cache-key record — `OutputSchemaCache`

**Files:**
- Modify: `src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java`

- [ ] **Step 1: Change the cache field type**

Replace:

```java
    private final Map<List<Object>, Schema> cache = new ConcurrentHashMap<>();
```

with:

```java
    private final Map<SchemaKey, Schema> cache = new ConcurrentHashMap<>();
```

- [ ] **Step 2: Rewrite `schemaFor` to build a `SchemaKey`**

Replace:

```java
    public Schema schemaFor(Schema beforeSchema, Schema afterSchema, Set<String> changedColumns) {
        List<Object> key = Arrays.asList(beforeSchema, afterSchema, new TreeSet<>(changedColumns));
        return cache.computeIfAbsent(key, k -> build(beforeSchema, afterSchema, changedColumns));
    }
```

with:

```java
    public Schema schemaFor(Schema beforeSchema, Schema afterSchema, Set<String> changedColumns) {
        var key = new SchemaKey(beforeSchema, afterSchema, changedColumns);
        return cache.computeIfAbsent(key, k -> build(beforeSchema, afterSchema, changedColumns));
    }
```

- [ ] **Step 3: Add the private record at the end of the class**

Immediately before the closing brace of `OutputSchemaCache`, add:

```java

    /** Cache key. The compact constructor normalizes {@code changed} into a TreeSet so
     *  equality is order-independent (matches the prior Arrays.asList + TreeSet behavior). */
    private record SchemaKey(Schema before, Schema after, Set<String> changed) {
        SchemaKey {
            changed = new TreeSet<>(changed);
        }
    }
```

- [ ] **Step 4: Remove the now-unused `java.util.Arrays` and `java.util.List` imports**

Delete these two import lines (verify they are unused elsewhere in the file first — after
Steps 1–2 they are):

```java
import java.util.Arrays;
import java.util.List;
```

Keep `import java.util.TreeSet;` — it is now used by the record's compact constructor.

- [ ] **Step 5: Run tests**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test -Dtest=OutputSchemaCacheTest`
Expected: PASS. (If you see "cannot find symbol: List" or "Arrays", an import was removed
that is still referenced — re-add it.)

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java
git commit -m "refactor: type-safe record cache key in OutputSchemaCache

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 4: Sealed exception hierarchy + pattern switch

This task spans four files and must be done together so the project compiles.

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/EmbeddingException.java`
- Modify: `src/main/java/com/materialize/connect/smt/embedding/RetriableEmbeddingException.java`
- Modify: `src/main/java/com/materialize/connect/smt/embedding/FatalEmbeddingException.java`
- Modify: `src/main/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClient.java`

- [ ] **Step 1: Create the sealed base class**

Create `EmbeddingException.java` with exactly:

```java
package com.materialize.connect.smt.embedding;

/** Base type for embedding failures; sealed to the retriable/fatal pair. */
public sealed abstract class EmbeddingException extends RuntimeException
        permits RetriableEmbeddingException, FatalEmbeddingException {

    protected EmbeddingException(String message) {
        super(message);
    }

    protected EmbeddingException(String message, Throwable cause) {
        super(message, cause);
    }
}
```

- [ ] **Step 2: Make `RetriableEmbeddingException` final and extend the base**

Replace the entire file with:

```java
package com.materialize.connect.smt.embedding;

/** Transient embedding failure (timeout, 429, 5xx) — worth retrying. */
public final class RetriableEmbeddingException extends EmbeddingException {
    public RetriableEmbeddingException(String message) { super(message); }
    public RetriableEmbeddingException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 3: Make `FatalEmbeddingException` final and extend the base**

Replace the entire file with:

```java
package com.materialize.connect.smt.embedding;

/** Permanent embedding failure (malformed request, auth) — not worth retrying. */
public final class FatalEmbeddingException extends EmbeddingException {
    public FatalEmbeddingException(String message) { super(message); }
    public FatalEmbeddingException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 4: Rewrite `RetryingEmbeddingClient.embed` with an exhaustive pattern switch**

Replace the `embed` method:

```java
    public List<Float> embed(String text) {
        int attempt = 0;
        while (true) {
            try {
                return provider.embed(text);
            } catch (FatalEmbeddingException e) {
                throw new ConnectException("Embedding request failed permanently", e);
            } catch (RetriableEmbeddingException e) {
                if (attempt >= maxRetries) {
                    throw new RetriableException(
                            "Embedding request failed after " + attempt + " retries", e);
                }
                backoff(attempt);
                attempt++;
            }
        }
    }
```

with:

```java
    public List<Float> embed(String text) {
        int attempt = 0;
        while (true) {
            try {
                return provider.embed(text);
            } catch (EmbeddingException e) {
                switch (e) {
                    case FatalEmbeddingException f ->
                            throw new ConnectException("Embedding request failed permanently", f);
                    case RetriableEmbeddingException r -> {
                        if (attempt >= maxRetries) {
                            throw new RetriableException(
                                    "Embedding request failed after " + attempt + " retries", r);
                        }
                        backoff(attempt);
                        attempt++;
                    }
                }
            }
        }
    }
```

The switch is exhaustive because `EmbeddingException` is sealed to exactly these two
subtypes — no `default` branch is needed or wanted.

- [ ] **Step 5: Run tests**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test -Dtest=RetryingEmbeddingClientTest,OpenAiEmbeddingProviderTest`
Expected: PASS. These two classes exercise both exception types end to end.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/EmbeddingException.java \
        src/main/java/com/materialize/connect/smt/embedding/RetriableEmbeddingException.java \
        src/main/java/com/materialize/connect/smt/embedding/FatalEmbeddingException.java \
        src/main/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClient.java
git commit -m "refactor: sealed EmbeddingException hierarchy with pattern switch

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 5: Text block in `OpenAiEmbeddingProviderTest`

**Files:**
- Modify: `src/test/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProviderTest.java`

- [ ] **Step 1: Convert the escaped JSON response body to a text block**

In `sendsExpectedRequestAndParsesEmbedding`, replace:

```java
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"data\":[{\"embedding\":[0.5,0.25,-1.0]}]}"));
```

with:

```java
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("""
                        {"data":[{"embedding":[0.5,0.25,-1.0]}]}"""));
```

(Leave the substring assertions on `"\"model\":..."` and `"\"input\":..."` unchanged — they
are single-line fragments that text blocks would not improve. The `"{}"` bodies in the
4xx/5xx tests also stay as-is.)

- [ ] **Step 2: Run tests**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test -Dtest=OpenAiEmbeddingProviderTest`
Expected: PASS, including `sendsExpectedRequestAndParsesEmbedding` asserting the vector
`(0.5f, 0.25f, -1.0f)`.

- [ ] **Step 3: Commit**

```bash
git add src/test/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProviderTest.java
git commit -m "refactor: text block for JSON literal in OpenAiEmbeddingProviderTest

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Task 6: Final full build

**Files:** none (verification only).

- [ ] **Step 1: Clean package to run the whole suite and produce the shaded jar**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk mvn clean package`
Expected: `BUILD SUCCESS`, all tests pass, and
`target/embedding-diff-smt-0.1.0-SNAPSHOT.jar` is produced.

- [ ] **Step 2: Confirm the jar targets Java 17 (class file version 61)**

Run: `JAVA_HOME=/opt/homebrew/opt/openjdk $JAVA_HOME/bin/javap -v -cp target/classes com.materialize.connect.smt.embedding.EmbeddingException | grep "major version"`
Expected: `major version: 61` (Java 17 bytecode — confirms `release 17` still holds after
adding the sealed class).

---

## Self-Review

**Spec coverage:**
- Pattern matching for `instanceof` → Task 1 ✓
- `var` for obvious locals → Task 1 ✓
- Switch expressions in `nullableCopy` → Task 2 ✓
- Cache-key record → Task 3 ✓
- Sealed exception hierarchy + pattern switch → Task 4 ✓
- Text blocks in tests → Task 5 ✓
- Declined `parseEmbedding` stream rewrite → correctly absent ✓
- `release 17` unchanged, no API/config changes → verified in Task 6 ✓

**Type consistency:** `SchemaKey(Schema before, Schema after, Set<String> changed)` defined
in Task 3 is referenced only within Task 3. `EmbeddingException` defined in Task 4 Step 1 is
referenced consistently in Steps 2–4. Switch cases bind `f`/`r` consistently.

**Placeholder scan:** No TBD/TODO/"similar to"; every code step shows full before/after.
