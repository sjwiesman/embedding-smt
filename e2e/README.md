# End-to-End Test

This module runs the SMT end to end as a Testcontainers-based JUnit integration test
(`EndToEndIT`). It orchestrates the full pipeline from Java:

- Materialize emulator produces row changes from an `articles` table.
- Materialize sinks those changes to Redpanda using `ENVELOPE DEBEZIUM`.
- Redpanda provides both the Kafka API and a Schema Registry-compatible API.
- Kafka Connect runs this SMT plus the Confluent Elasticsearch sink connector.
- Elasticsearch stores the upserted documents.
- An in-JVM mock OpenAI endpoint (`MockEmbeddingsServer`) stands in for the embeddings API.

The test writes rows to Materialize over JDBC and asserts Elasticsearch state directly,
so there are no shell scripts or separate verifier container.

## Run

This is an opt-in reactor module behind the `e2e` Maven profile, so a normal
`mvn package` does not build or run it. Docker must be running.

From the repository root:

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk mvn -Pe2e -pl e2e -am verify
```

The first run is slow: it builds the Kafka Connect image (which compiles the SMT and
installs the Elasticsearch sink connector) and starts four containers. CI runs this same
command on each pull request.

The test asserts:

- the initial insert includes `title`, `body`, `views`, `title_embedding`, and `body_embedding`
- the `views`-only update preserves the existing embeddings
- the `body` update refreshes only `body` and `body_embedding`

## How it works

- `EndToEndIT` — starts the containers on a shared Docker network, applies the
  Materialize DDL, registers the connector via the Connect REST API, performs the
  insert/update sequence, and asserts the indexed document with AssertJ + Awaitility.
- `MockEmbeddingsServer` — an in-JVM OpenAI-compatible endpoint whose embedding vector is
  a deterministic function of the input text (`[length, sum(ord) mod 997, vowel count]`),
  exposed to the containers via Testcontainers host-port exposure.
- `connect/Dockerfile` — builds the SMT from the reactor source and installs the
  Confluent Elasticsearch sink connector; reused by Testcontainers' `ImageFromDockerfile`.

To exercise the real OpenAI API instead of the mock, point
`transforms.embed.openai.endpoint` / `transforms.embed.openai.api.key` in
`registerConnector()` at the real service.
