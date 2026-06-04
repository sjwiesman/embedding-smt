# Embedding-Diff SMT Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Kafka Connect SMT that diffs a CDC `before`/`after` envelope, drops unchanged records, re-embeds changed text columns via a remote service, and emits only the changed columns (with vectors) so UPSERT-mode sinks merge them.

**Architecture:** A single `Transformation<R>` (`EmbeddingDiffTransform`) wires four focused collaborators: `RecordDiffer` (changed-column set), `OutputSchemaCache` (cached pruned output schema), an `EmbeddingProvider` discovered via `ServiceLoader` (OpenAI ships first), and `RetryingEmbeddingClient` (backoff + fail-fast). Output is a flat `Struct` of changed columns plus `<col>_embedding` vectors; the record key passes through as the document ID.

**Tech Stack:** Java 17, Maven, Kafka Connect API (`provided`), JDK `java.net.http`, Jackson; tests with JUnit 5, OkHttp MockWebServer, AssertJ.

Spec: `docs/superpowers/specs/2026-06-04-embedding-diff-smt-design.md`

---

### Task 1: Maven project scaffold

**Files:**
- Create: `pom.xml`
- Create (dir): `src/main/java`, `src/test/java`, `src/main/resources`

- [ ] **Step 1: Write `pom.xml`**

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.materialize.connect</groupId>
  <artifactId>embedding-diff-smt</artifactId>
  <version>0.1.0-SNAPSHOT</version>
  <packaging>jar</packaging>

  <properties>
    <maven.compiler.release>17</maven.compiler.release>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <kafka.version>3.8.1</kafka.version>
    <jackson.version>2.17.2</jackson.version>
    <junit.version>5.10.3</junit.version>
  </properties>

  <dependencies>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-api</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>org.apache.kafka</groupId>
      <artifactId>connect-transforms</artifactId>
      <version>${kafka.version}</version>
      <scope>provided</scope>
    </dependency>
    <dependency>
      <groupId>com.fasterxml.jackson.core</groupId>
      <artifactId>jackson-databind</artifactId>
      <version>${jackson.version}</version>
    </dependency>

    <dependency>
      <groupId>org.junit.jupiter</groupId>
      <artifactId>junit-jupiter</artifactId>
      <version>${junit.version}</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>org.assertj</groupId>
      <artifactId>assertj-core</artifactId>
      <version>3.26.3</version>
      <scope>test</scope>
    </dependency>
    <dependency>
      <groupId>com.squareup.okhttp3</groupId>
      <artifactId>mockwebserver</artifactId>
      <version>4.12.0</version>
      <scope>test</scope>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-surefire-plugin</artifactId>
        <version>3.2.5</version>
      </plugin>
      <plugin>
        <groupId>org.apache.maven.plugins</groupId>
        <artifactId>maven-shade-plugin</artifactId>
        <version>3.6.0</version>
        <executions>
          <execution>
            <phase>package</phase>
            <goals><goal>shade</goal></goals>
            <configuration>
              <createDependencyReducedPom>false</createDependencyReducedPom>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>
</project>
```

- [ ] **Step 2: Create source directories**

Run:
```bash
mkdir -p src/main/java/com/materialize/connect/smt/embedding/provider \
         src/test/java/com/materialize/connect/smt/embedding \
         src/main/resources/META-INF/services
```

- [ ] **Step 3: Verify the build resolves dependencies and compiles**

Run: `mvn -q clean compile`
Expected: `BUILD SUCCESS` (no sources yet, but POM resolves and compiles cleanly).

- [ ] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "build: scaffold Maven project for embedding-diff SMT"
```

---

### Task 2: EmbeddingProvider interface and exception types

These are foundational types with no behavior, so they are created directly (no test) and verified by compilation. Later tasks test the code that uses them.

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/EmbeddingProvider.java`
- Create: `src/main/java/com/materialize/connect/smt/embedding/RetriableEmbeddingException.java`
- Create: `src/main/java/com/materialize/connect/smt/embedding/FatalEmbeddingException.java`

- [ ] **Step 1: Write the provider interface**

```java
package com.materialize.connect.smt.embedding;

import java.util.List;
import java.util.Map;

/**
 * Pluggable embedding backend, discovered via {@link java.util.ServiceLoader}.
 * The implementation whose {@link #name()} matches the {@code provider} config is selected.
 */
public interface EmbeddingProvider extends AutoCloseable {

    /** Identifier matched against the {@code provider} config value (e.g. "openai"). */
    String name();

    /** Receives the connector's raw config map; reads its own provider-specific keys. */
    void configure(Map<String, ?> configs);

    /** Returns the embedding vector for the given text. Throws
     *  {@link RetriableEmbeddingException} for transient failures and
     *  {@link FatalEmbeddingException} for permanent ones. */
    List<Float> embed(String text);

    @Override
    default void close() { }
}
```

- [ ] **Step 2: Write the retriable exception**

```java
package com.materialize.connect.smt.embedding;

/** Transient embedding failure (timeout, 429, 5xx) — worth retrying. */
public class RetriableEmbeddingException extends RuntimeException {
    public RetriableEmbeddingException(String message) { super(message); }
    public RetriableEmbeddingException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 3: Write the fatal exception**

```java
package com.materialize.connect.smt.embedding;

/** Permanent embedding failure (malformed request, auth) — not worth retrying. */
public class FatalEmbeddingException extends RuntimeException {
    public FatalEmbeddingException(String message) { super(message); }
    public FatalEmbeddingException(String message, Throwable cause) { super(message, cause); }
}
```

- [ ] **Step 4: Verify compilation**

Run: `mvn -q clean compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/EmbeddingProvider.java \
        src/main/java/com/materialize/connect/smt/embedding/RetriableEmbeddingException.java \
        src/main/java/com/materialize/connect/smt/embedding/FatalEmbeddingException.java
git commit -m "feat: add EmbeddingProvider interface and exception types"
```

---

### Task 3: RecordDiffer

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/RecordDiffer.java`
- Test: `src/test/java/com/materialize/connect/smt/embedding/RecordDifferTest.java`

- [ ] **Step 1: Write the failing test**

```java
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
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=RecordDifferTest`
Expected: FAIL — `RecordDiffer` does not exist (compilation error).

- [ ] **Step 3: Write minimal implementation**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/** Computes which columns of {@code after} differ from {@code before}. */
public final class RecordDiffer {

    private RecordDiffer() { }

    /**
     * Returns the names of fields in {@code after} whose value differs from the
     * same field in {@code before}. If {@code before} is null (create/snapshot),
     * every field in {@code after} is considered changed.
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
            Object beforeValue = before.get(name);
            if (!Objects.equals(beforeValue, afterValue)) {
                changed.add(name);
            }
        }
        return changed;
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=RecordDifferTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/RecordDiffer.java \
        src/test/java/com/materialize/connect/smt/embedding/RecordDifferTest.java
git commit -m "feat: add RecordDiffer for before/after column diffing"
```

---

### Task 4: OutputSchemaCache

Builds the pruned output schema for a given changed-column set: each changed source column (same type) plus an optional `ARRAY<FLOAT32>` `<col><suffix>` field for each changed **embedded** column. Caches by (afterSchema, changedSet).

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java`
- Test: `src/test/java/com/materialize/connect/smt/embedding/OutputSchemaCacheTest.java`

- [ ] **Step 1: Write the failing test**

```java
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
        Schema schema = cache.schemaFor(AFTER, set("body", "views"));

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
        Schema first = cache.schemaFor(AFTER, set("body"));
        Schema second = cache.schemaFor(AFTER, set("body"));
        assertThat(first).isSameAs(second);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=OutputSchemaCacheTest`
Expected: FAIL — `OutputSchemaCache` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=OutputSchemaCacheTest`
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/OutputSchemaCache.java \
        src/test/java/com/materialize/connect/smt/embedding/OutputSchemaCacheTest.java
git commit -m "feat: add OutputSchemaCache for pruned embedding schemas"
```

---

### Task 5: RetryingEmbeddingClient

Wraps an `EmbeddingProvider`: retries `RetriableEmbeddingException` with exponential backoff up to `maxRetries`, then throws Connect's `RetriableException`; rethrows `FatalEmbeddingException` immediately as `ConnectException`. A `Sleeper` seam keeps backoff testable.

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClient.java`
- Test: `src/test/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClientTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RetryingEmbeddingClientTest {

    /** Provider whose embed() behavior is driven by a supplied function of attempt-count. */
    private static final class ScriptedProvider implements EmbeddingProvider {
        final AtomicInteger calls = new AtomicInteger();
        final java.util.function.IntFunction<List<Float>> script;
        ScriptedProvider(java.util.function.IntFunction<List<Float>> script) { this.script = script; }
        public String name() { return "scripted"; }
        public void configure(Map<String, ?> configs) { }
        public List<Float> embed(String text) { return script.apply(calls.incrementAndGet()); }
    }

    private static RetryingEmbeddingClient client(EmbeddingProvider p, int maxRetries) {
        // no-op sleeper: backoff does not actually pause the test
        return new RetryingEmbeddingClient(p, maxRetries, 1L, ms -> { });
    }

    @Test
    void returnsResultOnFirstSuccess() {
        ScriptedProvider p = new ScriptedProvider(attempt -> List.of(0.1f, 0.2f));
        assertThat(client(p, 3).embed("hi")).containsExactly(0.1f, 0.2f);
        assertThat(p.calls).hasValue(1);
    }

    @Test
    void retriesThenSucceeds() {
        ScriptedProvider p = new ScriptedProvider(attempt -> {
            if (attempt < 3) throw new RetriableEmbeddingException("429");
            return List.of(1.0f);
        });
        assertThat(client(p, 5).embed("hi")).containsExactly(1.0f);
        assertThat(p.calls).hasValue(3);
    }

    @Test
    void exhaustedRetriesThrowConnectRetriable() {
        ScriptedProvider p = new ScriptedProvider(attempt -> { throw new RetriableEmbeddingException("503"); });
        assertThatThrownBy(() -> client(p, 2).embed("hi"))
                .isInstanceOf(RetriableException.class);
        assertThat(p.calls).hasValue(3); // initial try + 2 retries
    }

    @Test
    void fatalFailsImmediatelyAsConnectException() {
        ScriptedProvider p = new ScriptedProvider(attempt -> { throw new FatalEmbeddingException("401"); });
        assertThatThrownBy(() -> client(p, 5).embed("hi"))
                .isInstanceOf(ConnectException.class)
                .isNotInstanceOf(RetriableException.class);
        assertThat(p.calls).hasValue(1); // no retries
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=RetryingEmbeddingClientTest`
Expected: FAIL — `RetryingEmbeddingClient` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;

import java.util.List;

/** Retries transient embedding failures with exponential backoff, then fails fast. */
public final class RetryingEmbeddingClient {

    /** Backoff seam so tests can avoid real sleeping. */
    @FunctionalInterface
    public interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }

    private final EmbeddingProvider provider;
    private final int maxRetries;
    private final long backoffMs;
    private final Sleeper sleeper;

    public RetryingEmbeddingClient(EmbeddingProvider provider, int maxRetries, long backoffMs, Sleeper sleeper) {
        this.provider = provider;
        this.maxRetries = maxRetries;
        this.backoffMs = backoffMs;
        this.sleeper = sleeper;
    }

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

    private void backoff(int attempt) {
        long delay = backoffMs * (1L << attempt); // exponential: base * 2^attempt
        try {
            sleeper.sleep(delay);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new ConnectException("Interrupted during embedding backoff", ie);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=RetryingEmbeddingClientTest`
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClient.java \
        src/test/java/com/materialize/connect/smt/embedding/RetryingEmbeddingClientTest.java
git commit -m "feat: add RetryingEmbeddingClient with backoff and fail-fast"
```

---

### Task 6: OpenAiEmbeddingProvider + ServiceLoader registration

`POST {endpoint}` with `{"model","input"[,"dimensions"]}` and `Authorization: Bearer <key>`; parses `data[0].embedding`. Maps 429/5xx/IO to `RetriableEmbeddingException`, other 4xx to `FatalEmbeddingException`.

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProvider.java`
- Create: `src/main/resources/META-INF/services/com.materialize.connect.smt.embedding.EmbeddingProvider`
- Test: `src/test/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProviderTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.materialize.connect.smt.embedding.provider;

import com.materialize.connect.smt.embedding.FatalEmbeddingException;
import com.materialize.connect.smt.embedding.RetriableEmbeddingException;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class OpenAiEmbeddingProviderTest {

    private MockWebServer server;

    @BeforeEach
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        server.shutdown();
    }

    private OpenAiEmbeddingProvider provider() {
        OpenAiEmbeddingProvider p = new OpenAiEmbeddingProvider();
        p.configure(Map.of(
                "openai.api.key", "sk-test",
                "openai.model", "text-embedding-3-small",
                "openai.endpoint", server.url("/v1/embeddings").toString(),
                "request.timeout.ms", "5000"));
        return p;
    }

    @Test
    void nameIsOpenai() {
        assertThat(new OpenAiEmbeddingProvider().name()).isEqualTo("openai");
    }

    @Test
    void sendsExpectedRequestAndParsesEmbedding() throws Exception {
        server.enqueue(new MockResponse()
                .setHeader("Content-Type", "application/json")
                .setBody("{\"data\":[{\"embedding\":[0.5,0.25,-1.0]}]}"));

        List<Float> vector = provider().embed("hello world");

        assertThat(vector).containsExactly(0.5f, 0.25f, -1.0f);

        RecordedRequest request = server.takeRequest();
        assertThat(request.getMethod()).isEqualTo("POST");
        assertThat(request.getHeader("Authorization")).isEqualTo("Bearer sk-test");
        assertThat(request.getHeader("Content-Type")).contains("application/json");
        String body = request.getBody().readUtf8();
        assertThat(body).contains("\"model\":\"text-embedding-3-small\"");
        assertThat(body).contains("\"input\":\"hello world\"");
    }

    @Test
    void rateLimitIsRetriable() {
        server.enqueue(new MockResponse().setResponseCode(429).setBody("{}"));
        assertThatThrownBy(() -> provider().embed("x"))
                .isInstanceOf(RetriableEmbeddingException.class);
    }

    @Test
    void serverErrorIsRetriable() {
        server.enqueue(new MockResponse().setResponseCode(503).setBody("{}"));
        assertThatThrownBy(() -> provider().embed("x"))
                .isInstanceOf(RetriableEmbeddingException.class);
    }

    @Test
    void badRequestIsFatal() {
        server.enqueue(new MockResponse().setResponseCode(400).setBody("{}"));
        assertThatThrownBy(() -> provider().embed("x"))
                .isInstanceOf(FatalEmbeddingException.class);
    }

    @Test
    void unauthorizedIsFatal() {
        server.enqueue(new MockResponse().setResponseCode(401).setBody("{}"));
        assertThatThrownBy(() -> provider().embed("x"))
                .isInstanceOf(FatalEmbeddingException.class);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=OpenAiEmbeddingProviderTest`
Expected: FAIL — `OpenAiEmbeddingProvider` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package com.materialize.connect.smt.embedding.provider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.materialize.connect.smt.embedding.EmbeddingProvider;
import com.materialize.connect.smt.embedding.FatalEmbeddingException;
import com.materialize.connect.smt.embedding.RetriableEmbeddingException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** OpenAI-compatible embeddings provider over java.net.http. */
public final class OpenAiEmbeddingProvider implements EmbeddingProvider {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String apiKey;
    private String model;
    private String endpoint;
    private Integer dimensions;
    private HttpClient httpClient;
    private Duration requestTimeout;

    @Override
    public String name() {
        return "openai";
    }

    @Override
    public void configure(Map<String, ?> configs) {
        this.apiKey = str(configs, "openai.api.key", null);
        this.model = str(configs, "openai.model", "text-embedding-3-small");
        this.endpoint = str(configs, "openai.endpoint", "https://api.openai.com/v1/embeddings");
        String dims = str(configs, "openai.dimensions", null);
        this.dimensions = (dims == null || dims.isBlank()) ? null : Integer.valueOf(dims.trim());
        long timeoutMs = Long.parseLong(str(configs, "request.timeout.ms", "30000"));
        this.requestTimeout = Duration.ofMillis(timeoutMs);
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(requestTimeout)
                .build();
    }

    @Override
    public List<Float> embed(String text) {
        String requestBody = buildRequestBody(text);
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(endpoint))
                .timeout(requestTimeout)
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + apiKey)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        HttpResponse<String> response;
        try {
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (IOException e) {
            throw new RetriableEmbeddingException("I/O error calling embedding service", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RetriableEmbeddingException("Interrupted calling embedding service", e);
        }

        int status = response.statusCode();
        if (status == 429 || status >= 500) {
            throw new RetriableEmbeddingException("Embedding service returned HTTP " + status);
        }
        if (status >= 400) {
            throw new FatalEmbeddingException("Embedding service returned HTTP " + status
                    + ": " + response.body());
        }
        return parseEmbedding(response.body());
    }

    private String buildRequestBody(String text) {
        ObjectNode root = MAPPER.createObjectNode();
        root.put("model", model);
        root.put("input", text);
        if (dimensions != null) {
            root.put("dimensions", dimensions);
        }
        try {
            return MAPPER.writeValueAsString(root);
        } catch (IOException e) {
            throw new FatalEmbeddingException("Failed to serialize embedding request", e);
        }
    }

    private List<Float> parseEmbedding(String body) {
        try {
            JsonNode embedding = MAPPER.readTree(body).path("data").path(0).path("embedding");
            if (!embedding.isArray()) {
                throw new FatalEmbeddingException("Embedding response missing data[0].embedding: " + body);
            }
            List<Float> vector = new ArrayList<>(embedding.size());
            for (JsonNode element : embedding) {
                vector.add(element.floatValue());
            }
            return vector;
        } catch (IOException e) {
            throw new FatalEmbeddingException("Failed to parse embedding response", e);
        }
    }

    private static String str(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return value == null ? defaultValue : value.toString();
    }
}
```

- [ ] **Step 4: Register the provider for ServiceLoader**

Create `src/main/resources/META-INF/services/com.materialize.connect.smt.embedding.EmbeddingProvider` with exactly this line:

```
com.materialize.connect.smt.embedding.provider.OpenAiEmbeddingProvider
```

- [ ] **Step 5: Run test to verify it passes**

Run: `mvn -q test -Dtest=OpenAiEmbeddingProviderTest`
Expected: PASS (6 tests).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProvider.java \
        src/main/resources/META-INF/services/com.materialize.connect.smt.embedding.EmbeddingProvider \
        src/test/java/com/materialize/connect/smt/embedding/provider/OpenAiEmbeddingProviderTest.java
git commit -m "feat: add OpenAiEmbeddingProvider with ServiceLoader registration"
```

---

### Task 7: EmbeddingDiffConfig (ConfigDef)

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffConfig.java`
- Test: `src/test/java/com/materialize/connect/smt/embedding/EmbeddingDiffConfigTest.java`

- [ ] **Step 1: Write the failing test**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class EmbeddingDiffConfigTest {

    private static Map<String, String> minimal() {
        Map<String, String> m = new HashMap<>();
        m.put("embedded.columns", "title,body");
        m.put("openai.api.key", "sk-test");
        return m;
    }

    @Test
    void appliesDefaults() {
        EmbeddingDiffConfig config = new EmbeddingDiffConfig(minimal());
        assertThat(config.beforeField()).isEqualTo("before");
        assertThat(config.afterField()).isEqualTo("after");
        assertThat(config.embeddingFieldSuffix()).isEqualTo("_embedding");
        assertThat(config.providerName()).isEqualTo("openai");
        assertThat(config.maxRetries()).isEqualTo(5);
        assertThat(config.retryBackoffMs()).isEqualTo(500L);
        assertThat(config.embeddedColumns()).containsExactly("title", "body");
    }

    @Test
    void embeddedColumnsIsRequired() {
        Map<String, String> m = new HashMap<>();
        m.put("openai.api.key", "sk-test");
        assertThatThrownBy(() -> new EmbeddingDiffConfig(m))
                .isInstanceOf(ConfigException.class);
    }

    @Test
    void overridesAreParsed() {
        Map<String, String> m = minimal();
        m.put("before.field", "old");
        m.put("after.field", "new");
        m.put("max.retries", "9");
        EmbeddingDiffConfig config = new EmbeddingDiffConfig(m);
        assertThat(config.beforeField()).isEqualTo("old");
        assertThat(config.afterField()).isEqualTo("new");
        assertThat(config.maxRetries()).isEqualTo(9);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=EmbeddingDiffConfigTest`
Expected: FAIL — `EmbeddingDiffConfig` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Typed view over the SMT configuration. */
public final class EmbeddingDiffConfig extends AbstractConfig {

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("before.field", Type.STRING, "before", Importance.MEDIUM,
                    "Envelope field holding the prior row state.")
            .define("after.field", Type.STRING, "after", Importance.MEDIUM,
                    "Envelope field holding the new row state.")
            .define("embedded.columns", Type.LIST, ConfigDef.NO_DEFAULT_VALUE, Importance.HIGH,
                    "Comma-separated string columns to embed.")
            .define("embedding.field.suffix", Type.STRING, "_embedding", Importance.LOW,
                    "Suffix appended to a column name to form its embedding field.")
            .define("provider", Type.STRING, "openai", Importance.MEDIUM,
                    "EmbeddingProvider name selected via ServiceLoader.")
            .define("request.timeout.ms", Type.INT, 30000, Importance.LOW,
                    "Per-request timeout for the embedding service call.")
            .define("max.retries", Type.INT, 5, Importance.LOW,
                    "Maximum retries for transient embedding failures.")
            .define("retry.backoff.ms", Type.LONG, 500L, Importance.LOW,
                    "Base backoff (exponential) between retries.")
            .define("openai.api.key", Type.PASSWORD, null, Importance.HIGH,
                    "OpenAI API key (Bearer token).")
            .define("openai.model", Type.STRING, "text-embedding-3-small", Importance.MEDIUM,
                    "OpenAI embedding model.")
            .define("openai.endpoint", Type.STRING, "https://api.openai.com/v1/embeddings",
                    Importance.LOW, "OpenAI embeddings endpoint URL.")
            .define("openai.dimensions", Type.INT, null, Importance.LOW,
                    "Optional output-dimension override.");

    public EmbeddingDiffConfig(Map<String, ?> originals) {
        super(CONFIG_DEF, originals);
    }

    public String beforeField() { return getString("before.field"); }

    public String afterField() { return getString("after.field"); }

    public String embeddingFieldSuffix() { return getString("embedding.field.suffix"); }

    public String providerName() { return getString("provider"); }

    public int maxRetries() { return getInt("max.retries"); }

    public long retryBackoffMs() { return getLong("retry.backoff.ms"); }

    public Set<String> embeddedColumns() {
        return new LinkedHashSet<>(getList("embedded.columns"));
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=EmbeddingDiffConfigTest`
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffConfig.java \
        src/test/java/com/materialize/connect/smt/embedding/EmbeddingDiffConfigTest.java
git commit -m "feat: add EmbeddingDiffConfig with ConfigDef"
```

---

### Task 8: EmbeddingDiffTransform (wiring)

The SMT itself. `configure()` builds the config, resolves the provider via a `createProvider` seam (overridable in tests), wraps it in `RetryingEmbeddingClient`, and builds `OutputSchemaCache`. `apply()` implements the data flow: tombstone on delete, drop on no-change, embed changed embedded columns, emit the flat pruned struct.

**Files:**
- Create: `src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransform.java`
- Test: `src/test/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransformTest.java`

- [ ] **Step 1: Write the failing test**

```java
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
    void changedEmbeddedColumnIsEmbeddedAndEmittedFlat() {
        SourceRecord out = transform.apply(record(row("a", "b", 1), row("a", "B2", 1)));

        // only body changed; only body embedded
        assertThat(stub.inputs).containsExactly("B2");

        Struct value = (Struct) out.value();
        assertThat(value.schema().field("body")).isNotNull();
        assertThat(value.getString("body")).isEqualTo("B2");
        assertThat(value.schema().field("title")).isNull();   // unchanged -> omitted
        assertThat(value.schema().field("views")).isNull();   // unchanged -> omitted
        assertThat((List<Float>) value.getArray("body_embedding")).containsExactly(1.0f, 2.0f);
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
        assertThat((List<Float>) value.getArray("title_embedding")).containsExactly(1.0f, 2.0f);
        assertThat((List<Float>) value.getArray("body_embedding")).containsExactly(1.0f, 2.0f);
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q test -Dtest=EmbeddingDiffTransformTest`
Expected: FAIL — `EmbeddingDiffTransform` does not exist.

- [ ] **Step 3: Write minimal implementation**

```java
package com.materialize.connect.smt.embedding;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;

/** SMT that diffs before/after, drops unchanged records, embeds changed text columns,
 *  and emits a flat struct of only the changed columns plus their embeddings. */
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
        this.client = new RetryingEmbeddingClient(
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
            return tombstone(record); // delete
        }

        Set<String> changed = RecordDiffer.changedColumns(before, after);
        if (changed.isEmpty()) {
            return null; // nothing changed -> drop
        }

        Schema outSchema = schemaCache.schemaFor(after.schema(), changed);
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

        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(), outSchema, outValue, record.timestamp());
    }

    private List<Float> embedColumn(String column, Object value) {
        if (value == null) {
            return null; // changed-to-null: emit null vector (clears downstream vector)
        }
        if (!(value instanceof String)) {
            throw new ConnectException("Embedded column '" + column
                    + "' must be a string but was " + value.getClass().getName());
        }
        return client.embed((String) value);
    }

    private R tombstone(R record) {
        return record.newRecord(record.topic(), record.kafkaPartition(),
                record.keySchema(), record.key(), null, null, record.timestamp());
    }

    private static Struct requireStruct(Object value) {
        if (!(value instanceof Struct)) {
            throw new ConnectException("EmbeddingDiffTransform requires a Struct value but got "
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q test -Dtest=EmbeddingDiffTransformTest`
Expected: PASS (6 tests).

- [ ] **Step 5: Run the full test suite**

Run: `mvn -q test`
Expected: PASS — all tests across all components green.

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransform.java \
        src/test/java/com/materialize/connect/smt/embedding/EmbeddingDiffTransformTest.java
git commit -m "feat: add EmbeddingDiffTransform wiring all collaborators"
```

---

### Task 9: Package the plugin uber-jar

**Files:**
- (none new — verifies `pom.xml` shade config from Task 1)

- [ ] **Step 1: Build the shaded plugin jar**

Run: `mvn -q clean package`
Expected: `BUILD SUCCESS`; all tests pass.

- [ ] **Step 2: Verify the jar contains the SMT, OpenAI provider, the ServiceLoader file, and a shaded Jackson (but NOT the `provided` connect-api)**

Run:
```bash
jar tf target/embedding-diff-smt-0.1.0-SNAPSHOT.jar | grep -E \
 'EmbeddingDiffTransform.class|provider/OpenAiEmbeddingProvider.class|META-INF/services/com.materialize.connect.smt.embedding.EmbeddingProvider|com/fasterxml/jackson'
```
Expected: shows `EmbeddingDiffTransform.class`, `provider/OpenAiEmbeddingProvider.class`, the `META-INF/services/...EmbeddingProvider` entry, and Jackson classes.

Run:
```bash
jar tf target/embedding-diff-smt-0.1.0-SNAPSHOT.jar | grep -c 'org/apache/kafka/connect/transforms/Transformation'
```
Expected: `0` (connect-api is `provided`, not bundled).

- [ ] **Step 3: Commit any pom adjustments (if needed)**

```bash
git add pom.xml
git commit -m "build: verify shaded plugin jar packaging"
```

(If no changes were needed, skip this commit.)

---

## Deployment notes (for the operator, not part of implementation)

Connector config snippet:

```properties
transforms=embed
transforms.embed.type=com.materialize.connect.smt.embedding.EmbeddingDiffTransform
transforms.embed.embedded.columns=title,body
transforms.embed.provider=openai
transforms.embed.openai.api.key=${file:/opt/secrets:openai_api_key}
transforms.embed.openai.model=text-embedding-3-small

# Required on the sink so omitted columns are preserved and deletes remove docs:
#   Elasticsearch:  write.method=UPSERT,  behavior.on.null.values=delete
#   OpenSearch:     index.write.method=UPSERT,  behavior.on.null.values=delete
# And the Kafka record key must be the document ID.
```
