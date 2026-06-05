# End-to-End Example

This example runs the SMT end to end with Docker Compose:

- Materialize Emulator produces row changes from an `articles` table.
- Materialize sinks those changes to Redpanda using `ENVELOPE DEBEZIUM`.
- Redpanda provides both the Kafka API and a Schema Registry-compatible API.
- Kafka Connect runs this SMT plus the Confluent Elasticsearch sink connector.
- Elasticsearch stores the upserted documents.
- A local mock embeddings service stands in for the OpenAI embeddings API.

## Run

From the repository root:

```bash
docker compose -f example/docker-compose.yml up --build
```

The stack auto-bootstraps itself:

1. Creates the Materialize connections, table, and Kafka sink.
2. Registers the Kafka Connect Elasticsearch sink connector with this SMT.
3. Inserts and updates one article row.
4. Verifies that Elasticsearch reflects the expected SMT behavior.

The `verify` container exits successfully after it confirms:

- the initial insert includes `title`, `body`, `views`, `title_embedding`, and `body_embedding`
- the `views`-only update preserves the existing embeddings
- the `body` update refreshes only `body` and `body_embedding`

## Service Endpoints

- Materialize SQL: `localhost:6875`
- Redpanda Kafka API: `localhost:19092`
- Redpanda Schema Registry: `localhost:18081`
- Kafka Connect REST: `localhost:8083`
- Elasticsearch: `localhost:9200`
- Mock embeddings API: `localhost:8000`

## Inspect the Running Demo

Check the connector status:

```bash
curl -s http://localhost:8083/connectors/articles-elasticsearch-sink/status | jq
```

Inspect the Elasticsearch index contents:

```bash
curl -s http://localhost:9200/articles-cdc/_search?pretty
```

Open a SQL session to Materialize:

```bash
psql postgresql://materialize@localhost:6875/materialize
```

## Notes

- The connector first extracts the `id` field from the Avro key so Elasticsearch uses a stable document ID.
- The example uses an OpenAI-compatible mock endpoint at `http://embeddings-mock:8000/v1/embeddings`.
- To switch to the real OpenAI API, update `example/connect/connector-config.json` with a real `openai.api.key` and endpoint.
