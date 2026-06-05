CREATE CONNECTION IF NOT EXISTS kafka_connection TO KAFKA (
    BROKER 'redpanda:9092',
    SECURITY PROTOCOL = 'PLAINTEXT'
);

CREATE CONNECTION IF NOT EXISTS csr_connection TO CONFLUENT SCHEMA REGISTRY (
    URL 'http://redpanda:8081'
);

CREATE TABLE IF NOT EXISTS articles (
    id INT,
    title TEXT,
    body TEXT,
    views INT
);

CREATE SINK IF NOT EXISTS articles_sink
    FROM articles
    INTO KAFKA CONNECTION kafka_connection (
        TOPIC 'articles-cdc'
    )
    KEY (id) NOT ENFORCED
    FORMAT AVRO USING CONFLUENT SCHEMA REGISTRY CONNECTION csr_connection
    ENVELOPE DEBEZIUM;
