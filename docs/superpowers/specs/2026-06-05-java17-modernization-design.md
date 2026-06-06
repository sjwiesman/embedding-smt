# Java 17 Modernization — Design

**Date:** 2026-06-05
**Status:** Approved
**Scope:** Behavior-preserving refactor of the embedding-diff SMT to idiomatic Java 17.

## Goal

Refactor the codebase to use Java 17 language idioms with **zero behavior change**. The
existing test suite is the contract: it stays green throughout, and most tests are
untouched. The `maven.compiler.release` stays at `17` — this is purely internal
modernization, not a version bump.

"Maximal" appetite is interpreted as *apply every modern idiom that genuinely improves the
code* — not change-for-change's-sake. One identified idiom is explicitly declined below
because it would reduce clarity.

## Baseline

Before any edit, capture a green build:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk mvn clean test
```

All subsequent steps re-run `mvn test` and must keep it green.

## Changes

### 1. Pattern matching for `instanceof` — `EmbeddingDiffTransform`

`requireStruct(Object)`: replace `instanceof`-then-cast with a binding pattern.

```java
private static Struct requireStruct(Object value) {
    if (value instanceof Struct struct) {
        return struct;
    }
    throw new ConnectException("EmbeddingDiffTransform requires a Struct value but got "
            + (value == null ? "null" : value.getClass().getName()));
}
```

`embedColumn(String, Object)`: null returns null; a `String` binding pattern guards the
embed call; anything else throws. Behavior preserved (null → null, non-String → throw,
String → embed).

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

### 2. `var` for obvious locals — `EmbeddingDiffTransform.apply()`

Use `var` where the right-hand side already names the type (e.g.
`var changed = RecordDiffer.changedColumns(before, after);`,
`var outValue = new Struct(outSchema);`). Leave `Struct before`/`Struct after` explicit if
clarity benefits — `var` is applied for readability, not blanket. `configure()` already
uses `var`.

### 3. Switch expressions — `OutputSchemaCache.nullableCopy`

First `switch` (builder selection by `schema.type()`) becomes a switch expression:

```java
SchemaBuilder builder = switch (schema.type()) {
    case ARRAY -> SchemaBuilder.array(schema.valueSchema());
    case MAP -> SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
    default -> SchemaBuilder.type(schema.type());
};
```

Second `switch` (STRUCT case + default no-op) collapses to a plain `if`:

```java
if (schema.type() == Schema.Type.STRUCT) {
    for (Field field : schema.fields()) {
        builder.field(field.name(), field.schema());
    }
}
```

### 4. Cache-key record — `OutputSchemaCache`

Replace the `Arrays.asList(beforeSchema, afterSchema, new TreeSet<>(changedColumns))`
ad-hoc tuple key with a private record. A compact constructor normalizes `changed` into a
`TreeSet` so equality remains order-independent (preserving current behavior).

```java
private record SchemaKey(Schema before, Schema after, Set<String> changed) {
    SchemaKey {
        changed = new TreeSet<>(changed);
    }
}
```

`schemaFor` builds a `SchemaKey` and uses it as the `computeIfAbsent` key. The `cache`
field type changes from `Map<List<Object>, Schema>` to `Map<SchemaKey, Schema>`.

### 5. Sealed exception hierarchy — `EmbeddingException` + `RetryingEmbeddingClient`

Introduce a sealed parent so the type system encodes the error-handling contract (these two
*are* the only embedding failure modes):

```java
public sealed abstract class EmbeddingException extends RuntimeException
        permits RetriableEmbeddingException, FatalEmbeddingException {
    protected EmbeddingException(String message) { super(message); }
    protected EmbeddingException(String message, Throwable cause) { super(message, cause); }
}
```

`RetriableEmbeddingException` and `FatalEmbeddingException` become `final` and extend
`EmbeddingException` (constructors delegate via `super`). Verified safe: the only test
reference (`RetryingEmbeddingClientTest`) constructs them with `new`, never subclasses.

`RetryingEmbeddingClient.embed()` catches the single `EmbeddingException` and dispatches
with an exhaustive pattern switch, preserving exact retry/rethrow behavior:

```java
catch (EmbeddingException e) {
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
```

### 6. Text blocks in tests

Replace escaped multi-line/JSON string literals with text blocks where it improves
readability, e.g. in `OpenAiEmbeddingProviderTest`:

```java
.setBody("""
    {"data":[{"embedding":[0.5,0.25,-1.0]}]}""")
```

Apply to other tests that contain escaped JSON or multi-line literals. Assertions on
substrings (`contains("\"model\":...")`) stay as-is unless a text block is clearly cleaner.

## Explicitly declined

`OpenAiEmbeddingProvider.parseEmbedding` iterates a Jackson `JsonNode` array into a
`List<Float>` with a `for` loop. A stream rewrite requires
`StreamSupport.stream(node.spliterator(), false)`, which is *less* readable than the loop.
**Left unchanged** despite the maximal appetite.

## Verification

- `JAVA_HOME=/opt/homebrew/opt/openjdk mvn test` green after every file change.
- Final `JAVA_HOME=/opt/homebrew/opt/openjdk mvn clean package` produces the shaded jar.
- No public API or config-key changes; `CONFIG_DEF` untouched.

## Out of scope

- Dependency / Kafka version bumps.
- Raising `maven.compiler.release` above 17.
- JPMS / `module-info.java`.
- Behavior or public-interface changes of any kind.
