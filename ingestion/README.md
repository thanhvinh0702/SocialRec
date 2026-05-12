# Kafka CDC Ingestion Layer

This stack implements the ingestion pattern you described:

- Debezium continuously captures PostgreSQL changes
- Kafka stores the change event stream
- a raw-writer continuously appends those events into a dedicated batch MinIO
- batch jobs can then run on a schedule against the MinIO raw zone

## Flow

```text
PostgreSQL
  -> Debezium snapshot (first run only)
  -> Debezium CDC (continuous)
  -> Kafka topics
  -> raw-writer
  -> MinIO raw-cdc/
```

## CDC Topics

Debezium publishes these topics:

- `socialrec.cdc.public.users`
- `socialrec.cdc.public.posts`
- `socialrec.cdc.public.post_media`
- `socialrec.cdc.public.comments`
- `socialrec.cdc.public.interactions`

The first time the connector starts, `snapshot.mode=initial` makes Debezium emit the full current contents of those tables.
After that, it keeps streaming INSERT/UPDATE/DELETE changes only.

## Prerequisites

The datasource stack must be running first:

```bash
cd data_source
docker compose up -d
```

The Postgres service has been configured for logical replication in `data_source/docker-compose.yml`.

## Start the Ingestion Layer

```bash
cd ingestion
docker compose up -d kafka minio-batch connect kafka-ui minio-ready raw-writer connector-init
```

Services:

- Kafka broker: `localhost:9092`
- Kafka UI: `http://localhost:8080`
- Kafka Connect REST: `http://localhost:8083`
- Batch MinIO API: `http://localhost:9100`
- Batch MinIO Console: `http://localhost:9101`

## Raw Storage Output

The raw-writer stores Kafka CDC events into the dedicated batch MinIO bucket:

- `socialrec-batch-raw`

Object layout:

```text
raw-cdc/topic=socialrec_cdc_public_interactions/date=YYYY-MM-DD/hour=HH/*.jsonl
```

This is append-only object creation, not in-place update.

## Notes

- CDC ingestion runs continuously.
- Batch computation should run on a schedule against the MinIO raw zone.
- This is the correct split:
  - ingestion layer = continuous
  - batch layer = periodic processing
