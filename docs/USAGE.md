# Usage (OpenAI)

How to run the Perfect Embeddings SMT with the bundled **OpenAI** embedder. No Maven
dependency or custom code is required — the OpenAI provider ships inside the plugin.

## 1. Prerequisites

- A **Kafka Connect** runtime (this is a connector plugin, not a standalone app).
- A source topic carrying a **Debezium `before`/`after` envelope, Avro-encoded via a
  Confluent-compatible Schema Registry** — e.g. a Materialize
  `CREATE SINK … KEY (…) FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY … ENVELOPE DEBEZIUM`.
  The SMT diffs the typed `before`/`after` structs, so the topic must carry a schema.
- A **sink that performs UPSERT keyed by the record key** (Elasticsearch / OpenSearch,
  etc.). This is what lets "write only changed columns" preserve everything else.
- An **OpenAI API key**.

## 2. Install the plugin

Download the release archive and extract it into the worker's `plugin.path`:

```bash
# from https://github.com/sjwiesman/embedding-smt/releases/tag/v0.1.0
unzip perfect-embeddings-smt-0.1.0.zip -d /usr/local/share/kafka/plugins/
# -> /usr/local/share/kafka/plugins/perfect-embeddings-smt/lib/…
```

Point the worker config at that root and **restart the worker(s)**:

```properties
plugin.path=/usr/local/share/kafka/plugins
```

## 3. Configure the transform on your sink connector

The SMT runs **on the sink connector**. Full Elasticsearch example:

```json
{
  "name": "articles-elasticsearch-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "topics": "articles-cdc",
    "connection.url": "http://elasticsearch:9200",

    "key.converter": "io.confluent.connect.avro.AvroConverter",
    "key.converter.schema.registry.url": "http://schema-registry:8081",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "http://schema-registry:8081",

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

Register it:

```bash
curl -s -X POST -H "Content-Type: application/json" \
  --data @connector-config.json \
  http://localhost:8083/connectors
```

**Transform order matters.** Materialize emits a *composite* Avro key (e.g. `{ "id": 1 }`).
`ExtractField$Key` unwraps it to the scalar `id` so the sink uses a stable document ID;
list `extractKey` **before** `embed`.

## 4. OpenAI configuration keys

| Key | Default | Notes |
|---|---|---|
| `transforms.embed.embedded.columns` | *(required)* | Comma-separated **string** columns to embed (e.g. `title,body`). |
| `transforms.embed.provider` | `openai` | Selects the bundled OpenAI provider; can be omitted since it is the default. |
| `transforms.embed.openai.api.key` | *(required)* | Bearer token. **Use a [config provider](https://docs.confluent.io/platform/current/connect/security.html#externalizing-secrets)** (`${file:…}` / `${vault:…}`) — do not inline it. |
| `transforms.embed.openai.model` | `text-embedding-3-small` | Any OpenAI embeddings model, e.g. `text-embedding-3-large`. |
| `transforms.embed.openai.endpoint` | `https://api.openai.com/v1/embeddings` | Override for Azure / proxies / gateways. |
| `transforms.embed.openai.dimensions` | *(unset)* | Optional output-dimension override (for models that support it). |
| `transforms.embed.embedding.field.suffix` | `_embedding` | Output field = `<col>` + suffix → `title_embedding`, `body_embedding` (`ARRAY<FLOAT32>`). |
| `transforms.embed.request.timeout.ms` | `30000` | Per-call timeout. |
| `transforms.embed.max.retries` | `5` | Retries on transient (429 / 5xx / IO) failures. |
| `transforms.embed.retry.backoff.ms` | `500` | Base backoff (exponential) between retries. |
| `transforms.embed.metrics.id` | *(auto)* | Optional readable name for the JMX metrics MBean. |

Permanent errors (HTTP 4xx, parse failures) fail fast; only transient errors are retried.

## 5. Two requirements that make it work

- **Sink must be UPSERT, keyed by the document id.** With the default `INSERT`, each record
  fully replaces the document and the omitted (unchanged) columns — including their stored
  embeddings — are lost, defeating the diff. Set `write.method=UPSERT` (Elasticsearch) or
  `index.write.method=UPSERT` (OpenSearch), and `behavior.on.null.values=delete` so CDC
  deletes (tombstones) remove the document.
- **Embedded columns must be string columns.** A non-string column listed in
  `embedded.columns` fails the record.

## 6. Verify

```bash
# connector + task RUNNING
curl -s localhost:8083/connectors/articles-elasticsearch-sink/status \
  | jq '.connector.state, .tasks[].state'
```

A freshly inserted row should carry `title_embedding` / `body_embedding` vectors, and a
later update that does **not** touch `title`/`body` must leave those embeddings untouched.

The transform exposes JMX metrics under
`com.materialize.connect.smt.embedding:type=EmbeddingDiff,id=*` —
`EmbeddingsComputed`, `EmbeddingsSkipped`, `EmbeddingsPossible`, `SkipRatio` — so you can
see how many OpenAI calls the diff is avoiding (scrape via JConsole or Prometheus
`jmx_exporter`).

## See also

- [`example/`](../example/) — a runnable end-to-end Docker Compose pipeline (Materialize →
  Redpanda → Kafka Connect + this SMT → Elasticsearch, with a mock OpenAI endpoint).
- [README](../README.md) — overview, install, and the source/sink requirements in full.
