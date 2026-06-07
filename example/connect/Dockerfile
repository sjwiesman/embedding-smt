FROM maven:3.9.9-eclipse-temurin-11 AS smt-build

WORKDIR /build

# Reactor build: the parent pom aggregates the perfect-embeddings-spi and
# perfect-embeddings-smt modules. The assembly resolves LICENSE/NOTICE/README from the
# reactor root, so copy them too.
COPY pom.xml ./
COPY LICENSE NOTICE README.md ./
COPY perfect-embeddings-spi ./perfect-embeddings-spi
COPY perfect-embeddings-smt ./perfect-embeddings-smt

RUN mvn -q -DskipTests package

FROM confluentinc/cp-kafka-connect:7.9.7

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:15.1.0

RUN mkdir -p /usr/share/local-connect-plugins/perfect-embeddings-smt

COPY --from=smt-build /build/perfect-embeddings-smt/target/perfect-embeddings-smt-0.1.0-SNAPSHOT.jar \
    /usr/share/local-connect-plugins/perfect-embeddings-smt/

USER appuser
