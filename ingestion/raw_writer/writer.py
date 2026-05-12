import json
import os
import time
import uuid
from collections import defaultdict
from datetime import datetime, timezone

from kafka import KafkaConsumer
from minio import Minio


KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPICS = [topic.strip() for topic in os.environ.get("KAFKA_TOPICS", "").split(",") if topic.strip()]
KAFKA_GROUP_ID = os.environ.get("KAFKA_GROUP_ID", "socialrec-raw-writer")

MINIO_ENDPOINT = os.environ["MINIO_ENDPOINT"]
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET = os.environ.get("MINIO_BUCKET", "socialrec-batch-raw")
MINIO_SECURE = os.environ.get("MINIO_SECURE", "false").lower() == "true"
RAW_PREFIX = os.environ.get("RAW_PREFIX", "raw-cdc")

FLUSH_RECORDS = int(os.environ.get("FLUSH_RECORDS", "500"))
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "15"))


def now_utc():
    return datetime.now(timezone.utc)


def format_topic_path(topic_name, timestamp):
    safe_topic = topic_name.replace(".", "_")
    return (
        f"{RAW_PREFIX}/topic={safe_topic}/"
        f"date={timestamp:%Y-%m-%d}/hour={timestamp:%H}/"
    )


def ensure_bucket(minio_client):
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)


def flush_topic(minio_client, topic_name, records):
    if not records:
        return

    timestamp = now_utc()
    prefix = format_topic_path(topic_name, timestamp)
    object_name = f"{prefix}{timestamp:%Y%m%dT%H%M%S}_{uuid.uuid4().hex}.jsonl"
    payload = "\n".join(json.dumps(record, default=str) for record in records).encode("utf-8")

    from io import BytesIO

    minio_client.put_object(
        MINIO_BUCKET,
        object_name,
        BytesIO(payload),
        length=len(payload),
        content_type="application/x-ndjson",
    )
    print(f"Wrote {len(records)} records to {object_name}")


def build_raw_record(message):
    value = message.value or {}
    envelope = value.get("payload", {}) if isinstance(value, dict) else {}
    source = envelope.get("source") or {}

    return {
        "topic": message.topic,
        "partition": message.partition,
        "offset": message.offset,
        "kafka_timestamp": message.timestamp,
        "ingested_at": now_utc().isoformat(),
        "table": source.get("table"),
        "schema": source.get("schema"),
        "op": envelope.get("op"),
        "event_ts_ms": envelope.get("ts_ms"),
        "event_ts_us": envelope.get("ts_us"),
        "event_ts_ns": envelope.get("ts_ns"),
        "source_ts_ms": source.get("ts_ms"),
        "snapshot": source.get("snapshot"),
        "tx_id": source.get("txId"),
        "lsn": source.get("lsn"),
        "before": envelope.get("before"),
        "after": envelope.get("after"),
    }


def main():
    if not KAFKA_TOPICS:
        raise RuntimeError("KAFKA_TOPICS must not be empty")

    consumer = KafkaConsumer(
        *KAFKA_TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )

    minio_client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    ensure_bucket(minio_client)

    buffers = defaultdict(list)
    last_flush = time.time()

    while True:
        message_batch = consumer.poll(timeout_ms=1000, max_records=FLUSH_RECORDS)
        batch_count = 0

        for _, messages in message_batch.items():
            for message in messages:
                buffers[message.topic].append(build_raw_record(message))
                batch_count += 1

        should_flush = False
        if batch_count:
            should_flush = any(len(records) >= FLUSH_RECORDS for records in buffers.values())

        if time.time() - last_flush >= FLUSH_SECONDS:
            should_flush = should_flush or any(buffers.values())

        if should_flush:
            for topic_name, records in list(buffers.items()):
                if records:
                    flush_topic(minio_client, topic_name, records)
                    buffers[topic_name] = []
            consumer.commit()
            last_flush = time.time()


if __name__ == "__main__":
    main()
