# Embedding-Diff SMT — Design

**Date:** 2026-06-04
**Status:** Approved (pending spec review)

## Summary

A Kafka Connect Single Message Transform (SMT) that consumes CDC-style records
carrying a `before` and `after` view of a row, computes which columns changed,
drops records where nothing changed, re-embeds changed text columns via a remote
embedding service, and emits a **minimal diff** (only the changed columns, plus a
vector field for each changed embedded column). Downstream sinks run in UPSERT
mode so the emitted diff is merged into the existing document — columns the SMT
omits keep their previous downstream values.

This mirrors the intent of Confluent Cloud's managed "create embedding action"
(per-column source → `<col>_embedding` output) but adds hash/diff-based skipping
and runs inline as an SMT rather than as a managed Flink job.

## Goals

- Skip work when a record's columns are unchanged (drop the record entirely).
- Re-embed only the embedded columns that actually changed.
- Emit only changed columns so UPSERT-mode sinks preserve unchanged downstream
  fields (including previously-computed vectors for unchanged columns).
- Pluggable embedding providers via `java.util.ServiceLoader`; ship OpenAI first.
- No silent data loss: embedding failures surface through Connect's standard
  error-handling machinery.

## Non-Goals

- Batching/async embedding for throughput (breaks SMT delivery semantics; the
  right tool for that scale is a stream-processing job, not an SMT).
- Native "Anthropic" embeddings (Anthropic has no first-party embeddings API;
  Voyage AI is its recommended partner and can be added later via ServiceLoader).
- Cross-record state or deduplication beyond the in-message `before`/`after`
  comparison.

## Context & Assumptions

- Records are typed via Schema Registry; the SMT sees a Connect `Struct` with a
  real schema (Debezium-style envelope with top-level `before`/`after`).
- The SMT operates on the record **value**. The record **key** is the document
  ID and is passed through unchanged.
- Sinks (Elasticsearch / OpenSearch) are configured with `write.method=UPSERT`
  (Elasticsearch uses `_update` + `doc_as_upsert`; OpenSearch
  `index.write.method=UPSERT`), giving partial-merge semantics: fields absent
  from the record are left untouched in the existing document.

## Architecture (single SMT, focused collaborators)

```
EmbeddingDiffTransform implements org.apache.kafka.connect.transforms.Transformation<R>
 ├── RecordDiffer            → changed-column set from before/after
 ├── OutputSchemaCache       → cached pruned Schema per changed-set
 ├── EmbeddingProvider       → interface (ServiceLoader); OpenAiEmbeddingProvider
 └── RetryingEmbeddingClient → backoff + fail-fast around the provider
```

Packaged as a Connect plugin (uber-jar on the plugin path).

### RecordDiffer
Compares each column in `after` against the same column in `before` using exact,
schema-aware **value equality** (not a hash — both values are in hand, so equality
is exact and collision-free). Returns the set of changed column names.

### OutputSchemaCache
Builds and caches a pruned Connect `Schema` keyed by the changed-column set. The
pruned schema contains the changed source columns plus one optional
`ARRAY<FLOAT32>` field named `<col><suffix>` for each changed **embedded** column.
Same changed-set → same cached `Schema` instance. For N configured columns there
are at most 2^N − 1 cached schemas (the all-unchanged case is dropped); N is small
in practice.

### EmbeddingProvider (interface) + OpenAiEmbeddingProvider
```java
public interface EmbeddingProvider {
    String name();                 // matched against the `provider` config
    void configure(Map<String, ?> configs);
    List<Float> embed(String text);
}
```
Discovered via `ServiceLoader<EmbeddingProvider>`; the implementation whose
`name()` matches `provider=` is selected. `OpenAiEmbeddingProvider` issues
`POST {openai.endpoint}` with `{"model": ..., "input": <text>, ["dimensions": ...]}`
and `Authorization: Bearer <openai.api.key>`, parsing `data[0].embedding`.

### RetryingEmbeddingClient
Wraps the provider. Retries transient failures with exponential backoff; maps
failures to Connect exception types (see Error Handling).

## Data Flow (per record, in `apply()`)

1. Read `before` and `after` structs from the value (field names configurable).
2. `RecordDiffer` computes the changed-column set.
3. **Empty changed-set → return `null`** (drop the record).
4. For each changed column that is configured as embedded, call
   `RetryingEmbeddingClient.embed(value)` → `List<Float>`.
5. `OutputSchemaCache` returns the pruned schema for this changed-set.
6. Build a **flat** output `Struct` (changed columns promoted to top level, plus
   the `<col><suffix>` vectors), pass the key through unchanged, return it.

Because output is flat (the pruned "new record state"), no separate
flatten/ExtractNewRecordState SMT is needed downstream of this one.

### Edge cases
- **Create / snapshot (`before` is null):** every `after` column is "changed";
  all embedded columns are embedded; full row emitted.
- **Delete (`after` is null):** emit a tombstone (null value, same key) so an
  UPSERT-mode sink with `behavior.on.null.values=delete` removes the document.
  No embedding calls.
- **Both null:** treated as no-op → drop.

## Configuration

| Key | Default | Notes |
|---|---|---|
| `before.field` | `before` | Envelope field holding prior state |
| `after.field` | `after` | Envelope field holding new state |
| `embedded.columns` | *(required)* | List of string columns to embed |
| `embedding.field.suffix` | `_embedding` | Output field = `<col>` + suffix |
| `provider` | `openai` | Selects `EmbeddingProvider` via ServiceLoader |
| `openai.api.key` | *(required, `Password` type)* | Secret; supports Connect config providers |
| `openai.model` | `text-embedding-3-small` | |
| `openai.endpoint` | `https://api.openai.com/v1/embeddings` | Override for proxies/Azure |
| `openai.dimensions` | *(unset)* | Optional output-dimension override |
| `request.timeout.ms` | `30000` | Per HTTP call |
| `max.retries` | `5` | Retry budget for transient failures |
| `retry.backoff.ms` | `500` | Exponential backoff base |

## Error Handling

- **Transient** (HTTP 429, 5xx, connect/read timeout): retry with exponential
  backoff up to `max.retries`. On exhaustion, throw `RetriableException` so
  Connect's retry/tolerance machinery engages.
- **Non-transient** (400 malformed, 401/403 auth): throw `ConnectException`
  immediately.
- Final disposition is controlled by standard `errors.tolerance` /
  `errors.deadletterqueue.*`. The SMT never silently drops a changed record or
  forwards it without a required embedding.

### Operational note
A synchronous remote call inline in the SMT couples connector throughput to
embedding-service latency. This is accepted for this design (fail-fast keeps it
honest). If throughput becomes a problem, migrate the embedding step to a
stream-processing job rather than batching inside the SMT.

## Testing Strategy

- `RecordDiffer` — no-change (drop), single/multi-column change, create
  (`before` null), delete (`after` null).
- `OutputSchemaCache` — pruned schema correctness; same changed-set returns the
  same cached `Schema` instance.
- `OpenAiEmbeddingProvider` — against a mock HTTP server (MockWebServer/WireMock):
  request body shape, response parsing, 429/5xx/400/401 mapping.
- `RetryingEmbeddingClient` — retry count, backoff timing, retriable-vs-fatal
  classification.
- `EmbeddingDiffTransform.apply()` — end-to-end with a stub provider: drop on
  no change, tombstone on delete, flat output shape, embeddings attached only for
  changed embedded columns.

## Build & Packaging

- **Maven**, **Java 17**.
- Uber-jar for the Connect plugin path.
- Dependencies: `connect-api` (provided scope), JDK `java.net.http` (HTTP),
  Jackson (JSON); tests: JUnit 5 + MockWebServer/WireMock.
