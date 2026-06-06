# embedding-smt

A Kafka Connect [Single Message Transform](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
(SMT) that turns a CDC `before`/`after` envelope into a minimal, embedding-enriched
diff for sinks running in UPSERT mode.

For each record it:

1. Reads the `before` and `after` structs from a CDC envelope (typed `Struct` via
   Schema Registry).
2. Computes which columns changed (per-column comparison).
3. **Drops** the record if nothing changed.
4. Emits a **flat struct containing only the changed columns**. For each changed
   column that is configured as *embedded*, it calls a remote embedding service and
   attaches a `<col>_embedding` vector field (optional `ARRAY<FLOAT32>`).
5. Passes the record **key** through unchanged (used as the downstream document ID).

Because unchanged columns are omitted from the output, an UPSERT-mode sink merges the
diff and leaves previously-stored values (including embeddings) untouched. Deletes
(`after == null`) become tombstones; envelopes with neither `before` nor `after` are
dropped.

Embedding providers are pluggable via `java.util.ServiceLoader`. **OpenAI** ships in
the box.

---

## Compatibility

| | |
|---|---|
| **Java** | 17+ (compiled to `release 17`) |
| **Kafka / Connect** | built against `connect-api` 3.8.1; works on any Connect runtime with [KIP-146](https://cwiki.apache.org/confluence/display/KAFKA/KIP-146+-+Classloading+Isolation+in+Connect) classloader isolation (Apache Kafka 2.3+ / Confluent Platform 5.3+) |
| **Packaging** | self-contained plugin folder; bundles Jackson, Connect API is `provided` by the worker |

---

## Install

### From a release (recommended)

1. **Download** the plugin archive from the
   [Releases page](https://github.com/sjwiesman/embedding-smt/releases) —
   `embedding-diff-smt-<version>.zip` — and (optionally) verify its checksum:

   ```bash
   sha256sum -c embedding-diff-smt-<version>.zip.sha256
   ```

2. **Extract** the `embedding-diff-smt/` folder into a directory on the Connect worker's
   `plugin.path`:

   ```bash
   unzip embedding-diff-smt-<version>.zip -d /usr/local/share/kafka/plugins/
   ```

   This yields `/usr/local/share/kafka/plugins/embedding-diff-smt/lib/…`. Ensure the worker
   config includes that root:

   ```properties
   plugin.path=/usr/local/share/kafka/plugins
   ```

3. **Restart** the Connect worker(s) so the plugin is discovered.

4. **Add the transform** to your sink connector config (see below).

### From source

```bash
mvn clean package
```

This runs the tests and produces both a shaded plugin jar and the distributable plugin
archive:

```
target/embedding-diff-smt-<version>.jar   # shaded jar (Jackson bundled)
target/embedding-diff-smt-<version>.zip   # plugin folder: extract into plugin.path
```

Install the `.zip` exactly as in the release flow above, or drop the shaded jar into a
`plugin.path/embedding-diff-smt/` directory yourself.

> **Note:** if `java` is not on your `PATH`, point Maven at a JDK explicitly:
>
> ```bash
> JAVA_HOME=/path/to/jdk mvn clean package
> ```

Run the tests only:

```bash
mvn test
```

---

## Connector configuration

```properties
transforms=embed
transforms.embed.type=com.materialize.connect.smt.embedding.EmbeddingDiffTransform

# Columns to embed (must be string columns)
transforms.embed.embedded.columns=title,body

# Embedding provider
transforms.embed.provider=openai
transforms.embed.openai.api.key=${file:/opt/secrets/connect.properties:openai_api_key}
transforms.embed.openai.model=text-embedding-3-small
```

### Configuration reference

| Key | Default | Description |
|---|---|---|
| `before.field` | `before` | Envelope field holding the prior row state |
| `after.field` | `after` | Envelope field holding the new row state |
| `embedded.columns` | *(required)* | Comma-separated string columns to embed |
| `embedding.field.suffix` | `_embedding` | Output field = `<col>` + suffix |
| `provider` | `openai` | `EmbeddingProvider` name (selected via `ServiceLoader`) |
| `request.timeout.ms` | `30000` | Per-request timeout for the embedding call |
| `max.retries` | `5` | Retries for transient (429/5xx/IO) failures |
| `retry.backoff.ms` | `500` | Base backoff (exponential, capped) between retries |
| `openai.api.key` | *(required)* | OpenAI API key (Bearer token); use a [config provider](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets) for secrets |
| `openai.model` | `text-embedding-3-small` | OpenAI embedding model |
| `openai.endpoint` | `https://api.openai.com/v1/embeddings` | Override for proxies / Azure / gateways |
| `openai.dimensions` | *(unset)* | Optional output-dimension override |

---

## Sink requirements (important)

The SMT's "leave unchanged columns alone" guarantee only holds if the sink performs a
**partial update (UPSERT)** keyed by the document ID, and the **Kafka record key is the
document ID**.

**Elasticsearch sink:**

```properties
write.method=UPSERT
behavior.on.null.values=delete
```

**OpenSearch sink:**

```properties
index.write.method=UPSERT
behavior.on.null.values=delete
```

With `INSERT` (the default), each record fully replaces the document — omitted columns
would be lost, defeating the diff. `behavior.on.null.values=delete` makes the tombstones
emitted for CDC deletes remove the document.

