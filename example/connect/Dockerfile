FROM maven:3.9.9-eclipse-temurin-17 AS smt-build

WORKDIR /build

# Reactor build: the parent pom aggregates the embedding-spi and embedding-diff-smt
# modules. The assembly resolves LICENSE/NOTICE/README from the reactor root, so copy
# them too.
COPY pom.xml ./
COPY LICENSE NOTICE README.md ./
COPY embedding-spi ./embedding-spi
COPY embedding-diff-smt ./embedding-diff-smt

RUN mvn -q -DskipTests package

FROM confluentinc/cp-kafka-connect:7.9.7

USER root

RUN confluent-hub install --no-prompt confluentinc/kafka-connect-elasticsearch:15.1.0

RUN mkdir -p /usr/share/local-connect-plugins/embedding-diff-smt

COPY --from=smt-build /build/embedding-diff-smt/target/embedding-diff-smt-0.1.0-SNAPSHOT.jar \
    /usr/share/local-connect-plugins/embedding-diff-smt/

USER appuser
