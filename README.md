# embedding-smt

A Kafka Connect [Single Message Transform](https://docs.confluent.io/platform/current/connect/transforms/overview.html)
(SMT) that turns a [Materialize](https://materialize.com/) Kafka sink's Debezium-style
`before`/`after` envelope into a minimal, embedding-enriched diff for downstream sinks
running in UPSERT mode.

For each record it:

1. Reads the `before` and `after` structs from the Materialize Debezium envelope (typed
   `Struct` via Schema Registry).
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

## Source: Materialize Kafka sink

This SMT expects its input topic to be fed by a Materialize Kafka sink declared with
`ENVELOPE DEBEZIUM` and Avro encoding. That envelope is what gives the transform the
`before`/`after` row states it diffs, and `KEY (…)` is what becomes the downstream
document ID.

```sql
CREATE CONNECTION kafka_connection TO KAFKA (BROKER 'redpanda:9092', SECURITY PROTOCOL = 'PLAINTEXT');
CREATE CONNECTION csr_connection TO CONFLUENT SCHEMA REGISTRY (URL 'http://redpanda:8081');

CREATE TABLE articles (id INT, title TEXT, body TEXT, views INT);

CREATE SINK articles_sink
    FROM articles
    INTO KAFKA CONNECTION kafka_connection (TOPIC 'articles-cdc')
    KEY (id) NOT ENFORCED                                    -- becomes the document ID
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;                                       -- required: emits before/after
```

Requirements this places on the source:

- **`ENVELOPE DEBEZIUM`** — required. `ENVELOPE UPSERT` or `ENVELOPE NONE` do **not** carry
  the `before`/`after` pair the diff is computed from.
- **`FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY`** — the SMT operates on typed Connect
  `Struct`s, so the topic must carry a schema (use the Avro converter on the Connect side).
- **`KEY (…)`** — declare the primary key. It flows through as the Kafka record key and is
  used as the upsert document ID downstream (see [Sink requirements](#sink-requirements-important)).

The envelope's row states are read from the `before` and `after` fields that Materialize's
Debezium output always emits.

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

Add the SMT to the **sink** connector that reads the Materialize topic. A complete
Elasticsearch sink config for the `articles-cdc` topic above looks like this (JSON form,
as used by [`example/connect/connector-config.json`](example/connect/connector-config.json)):

```json
{
  "name": "articles-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "articles-cdc",
    "connection.url": "http://elasticsearch:9200",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://redpanda:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://redpanda:8081",

    "key.ignore": "false",
    "schema.ignore": "false",
    "write.method": "UPSERT",
    "behavior.on.null.values": "delete",

    "transforms": "extractKey,embed",

    "transforms.extractKey.type": "org.apache.kafka.connect.transforms.ExtractField$Key",
    "transforms.extractKey.field": "id",

    "transforms.embed.type": "com.materialize.connect.smt.embedding.EmbeddingDiffTransform",
    "transforms.embed.embedded.columns": "title,body",
    "transforms.embed.provider": "openai",
    "transforms.embed.openai.api.key": "${file:/opt/secrets/connect.properties:openai_api_key}",
    "transforms.embed.openai.model": "text-embedding-3-small"
  }
}
```

**Transform ordering matters.** Materialize emits a *composite* Avro key (a struct of the
`KEY (…)` columns, e.g. `{ "id": 1 }`). `ExtractField$Key` unwraps it to the scalar `id`
so the sink uses a stable, flat document ID; the `embed` transform then passes that key
through unchanged. List `extractKey` before `embed`.

The equivalent `.properties` form for the SMT portion alone:

```properties
transforms=extractKey,embed

transforms.extractKey.type=org.apache.kafka.connect.transforms.ExtractField$Key
transforms.extractKey.field=id

transforms.embed.type=com.materialize.connect.smt.embedding.EmbeddingDiffTransform
transforms.embed.embedded.columns=title,body
transforms.embed.provider=openai
transforms.embed.openai.api.key=${file:/opt/secrets/connect.properties:openai_api_key}
transforms.embed.openai.model=text-embedding-3-small
```

### Configuration reference

| Key | Default | Description |
|---|---|---|
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
| `metrics.id` | *(auto)* | Identifier used in the metrics MBean `ObjectName` (`id=...`). Defaults to an auto-assigned per-instance sequence; set it to a stable, readable value (e.g. the connector/transform alias) when running more than one instance in a worker |

---

## Metrics

The SMT registers a JMX MBean exposing how many embedding calls it **avoided** versus a
naive pipeline that re-embeds every configured column on every change event. It's a plain
MBean on the platform MBeanServer, so it's scraped like any other Kafka Connect JMX metric
(Prometheus [`jmx_exporter`](https://github.com/prometheus/jmx_exporter), JConsole, etc.).

**ObjectName:** `com.materialize.connect.smt.embedding:type=EmbeddingDiff,id=<metrics.id>`

| Attribute | Meaning |
|---|---|
| `EmbeddingsComputed` | Embedding API calls actually made |
| `EmbeddingsSkipped` | Embedding API calls avoided — dropped (unchanged) records, unchanged columns within a changed record, and changed-to-null columns |
| `EmbeddingsPossible` | Calls a naive re-embed-everything pipeline would have made (`= EmbeddingsComputed + EmbeddingsSkipped`) |
| `SkipRatio` | `EmbeddingsSkipped / EmbeddingsPossible` (0.0 when idle) — e.g. `0.83` means 83% of embedding calls were avoided |

**Baseline:** `EmbeddingsPossible` counts every configured embedded column present in a
record's `after` for each insert/update. Deletes, tombstones, and both-null records embed
nothing, so they don't contribute. This makes `SkipRatio` the share of *embeddable* work
the diff avoided.

Since a Connect SMT has no access to the connector name or task id, each instance gets a
unique `id` automatically. Set `metrics.id` to give it a stable, readable name when a
worker runs more than one instance (`tasks.max > 1`, or the SMT used by multiple
connectors).

---

## Sink requirements (important)

The SMT's "leave unchanged columns alone" guarantee only holds if the sink performs a
**partial update (UPSERT)** keyed by the document ID, and the **Kafka record key is the
document ID** — which is why the example unwraps Materialize's composite Avro key with
`ExtractField$Key` (see [Connector configuration](#connector-configuration)) before the
sink writes it.

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

---

## End-to-end example

[`example/`](example/) runs the whole intended pipeline with Docker Compose: Materialize
(`ENVELOPE DEBEZIUM` sink) → Redpanda (Kafka + Schema Registry) → Kafka Connect with this
SMT → Elasticsearch, plus a mock OpenAI endpoint. A `verify` container asserts the
diff/embedding-preservation behavior and exits 0. From the repo root:

```bash
docker compose -f example/docker-compose.yml up --build
```

See [`example/README.md`](example/README.md) for details.

