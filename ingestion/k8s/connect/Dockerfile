# Stage 1: Get the S3 sink connector from Confluent Hub
FROM confluentinc/cp-kafka-connect:7.9.0 AS cp
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:10.6.7

# Stage 2: Build the final Strimzi image
FROM quay.io/strimzi/kafka:1.0.0-kafka-4.2.0
USER root

# Debezium PostgreSQL source connector
RUN mkdir -p /opt/kafka/plugins/debezium-postgres
RUN curl -L https://repo1.maven.org/maven2/io/debezium/debezium-connector-postgres/3.4.3.Final/debezium-connector-postgres-3.4.3.Final-plugin.tar.gz \
    | tar -xvz -C /opt/kafka/plugins/debezium-postgres

# MinIO sink connector (S3-compatible via Confluent S3 sink)
RUN mkdir -p /opt/kafka/plugins/kafka-connect-s3
COPY --from=cp /usr/share/confluent-hub-components/confluentinc-kafka-connect-s3/ \
    /opt/kafka/plugins/kafka-connect-s3/

USER 1001