# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A Kafka Connect Single Message Transform (SMT) that converts a CDC `before`/`after`
envelope into a minimal, embedding-enriched diff for sinks running in UPSERT mode. It
emits a flat struct containing **only the changed columns** (plus a `<col>_embedding`
vector for any changed column configured as embedded), so an UPSERT sink merges the diff
and leaves unchanged columns — including their embeddings — untouched. This packages as a
connector plugin jar; it is not a standalone app.

## Commands

```bash
mvn clean package   # run tests + produce shaded plugin jar in perfect-embeddings-smt/target/
mvn test            # tests only
mvn test -Dtest=RecordDifferTest                       # single test class
mvn test -Dtest=EmbeddingDiffTransformTest#methodName  # single test method
mvn spotless:apply  # auto-format (google-java-format)
mvn spotless:check  # fail if any file is unformatted (run in CI)
```

Code is formatted with [Spotless](https://github.com/diffplug/spotless) using
google-java-format; CI fails on unformatted code, so run `spotless:apply` before
committing. google-java-format is pinned to a version recent enough for the local JDK 26;
older versions fail with a javac internal-API `NoSuchMethodError`.

`java` is not on `PATH` here — Maven needs `JAVA_HOME` set explicitly:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk mvn clean package
```

The shaded jar (`perfect-embeddings-smt/target/perfect-embeddings-smt-0.1.0-SNAPSHOT.jar`)
bundles Jackson but **not** the Connect API (`provided` scope — the runtime supplies it).

This is a Maven reactor with two modules: **`perfect-embeddings-spi`** (the published,
dependency-free `EmbeddingProvider` SPI, package `com.materialize.embedding.spi`) and
**`perfect-embeddings-smt`** (the Connect plugin, which depends on and shades the SPI).
`mvn` at the repo root builds both.

## End-to-end test

`e2e/` is an opt-in reactor module (Maven profile `e2e`) whose Testcontainers JUnit test
`EndToEndIT` runs the full pipeline (Materialize → Redpanda → Kafka Connect + this SMT →
Elasticsearch, with an in-JVM mock OpenAI endpoint) and drives all table writes and
verification from Java. Docker must be running. From the repo root:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk mvn -Pe2e -pl e2e -am verify
```

The Connect image is built from `e2e/connect/Dockerfile`, and CI runs this on each
pull request. See `e2e/README.md`.

## Architecture

The transform pipeline lives in
`perfect-embeddings-smt/src/main/java/com/materialize/connect/smt/embedding/`.
`EmbeddingDiffTransform.apply()` is the orchestrator and the file to read first; it
delegates to focused collaborators:

- **EmbeddingDiffTransform** — the SMT entry point. Per record: null value → pass through
  (already a tombstone); `after == null` → tombstone if `before` existed, else drop
  (both-null is a no-op); otherwise diff, build the pruned output struct, and embed
  changed embedded columns. The key is always passed through unchanged (it is the
  downstream document ID).
- **RecordDiffer** — pure static diff: which fields of `after` differ from `before`.
  Null `before` (create/snapshot) ⇒ all fields changed. Fields present in `before` but
  dropped from `after` also count as changed (emitted as null).
- **OutputSchemaCache** — builds and caches pruned output schemas keyed by
  `(beforeSchema, afterSchema, changedColumns)`. Embedding fields are always
  `ARRAY<FLOAT32>` optional. Columns dropped between before/after are copied as nullable.
- **EmbeddingProvider** — pluggable backend resolved by `name()` via `java.util.ServiceLoader`.
  Lives in the **`perfect-embeddings-spi`** module (`com.materialize.embedding.spi`), along
  with `RetriableEmbeddingException`/`FatalEmbeddingException`. Registered impls are listed
  in `perfect-embeddings-smt/src/main/resources/META-INF/services/com.materialize.embedding.spi.EmbeddingProvider`.
  **OpenAiEmbeddingProvider** (`provider/`, in the SMT module) is the only shipped impl
  (`name() == "openai"`, OpenAI-compatible over `java.net.http`). To add a provider:
  depend on `com.materialize:perfect-embeddings-spi`, implement the interface, and register
  its FQCN via that services file.
- **RetryingEmbeddingClient** — wraps a provider with exponential backoff. `Sleeper` is a
  seam so tests avoid real sleeping.
- **EmbeddingDiffConfig** — typed `AbstractConfig` view; `CONFIG_DEF` is the single source
  of truth for config keys/defaults (mirrored in README's config table).

### Error-handling contract

Provider `embed()` distinguishes failure modes via exceptions, and this distinction drives
retry behavior — preserve it when editing providers or the client:

- **RetriableEmbeddingException** (transient: HTTP 429/5xx, IO) → retried, then rethrown as
  Connect `RetriableException` after `max.retries`.
- **FatalEmbeddingException** (permanent: 4xx, parse/serialize failures) → rethrown
  immediately as `ConnectException`, no retries.

## Conventions

- Java 11 (`release 11` — keep source at the Java 11 language/API level; e.g. classic
  `switch` statements, no text blocks/records), Maven, JUnit 5 + AssertJ. Provider HTTP is
  tested against `mockwebserver`.
- Collaborators are constructor-injected and seams are exposed (`createProvider` is
  `protected` and overridable; `Sleeper` is injectable) specifically for unit testing —
  follow that pattern rather than reaching for static state or real network/sleep calls.
