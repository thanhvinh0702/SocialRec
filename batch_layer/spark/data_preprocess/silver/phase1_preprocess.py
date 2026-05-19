import os
from typing import Dict, List

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType


MINIO_BUCKET = os.getenv("MINIO_BUCKET", "socialrec-batch")
RAW_PREFIX = os.getenv("RAW_PREFIX", "bronze/")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-batch:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_PATH_STYLE = os.getenv("MINIO_PATH_STYLE", "true").lower() == "true"
INPUT_FORMAT = os.getenv("INPUT_FORMAT", "json")
WRITE_MODE = os.getenv("WRITE_MODE", "overwrite")
TABLE_FILTER = [value.strip() for value in os.getenv("TABLE_FILTER", "").split(",") if value.strip()]
TABLE_NAMES = TABLE_FILTER or ["users", "posts", "post_media", "comments", "interactions"]

TABLE_PK_HINTS: Dict[str, List[str]] = {
    "users": ["user_id"],
    "posts": ["post_id"],
    "comments": ["comment_id"],
    "interactions": ["interaction_id"],
    "post_media": ["media_id", "post_media_id"],
}


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("socialrec-phase1-preprocess")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
    hadoop_conf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
    hadoop_conf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
    hadoop_conf.set("fs.s3a.path.style.access", str(MINIO_PATH_STYLE).lower())
    hadoop_conf.set("fs.s3a.connection.ssl.enabled", str(MINIO_ENDPOINT.startswith("https://")).lower())
    hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    hadoop_conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    hadoop_conf.set("fs.s3a.endpoint.region", MINIO_REGION)
    return spark


def infer_table_pk_columns(df: DataFrame, table_name: str) -> List[str]:
    hinted_columns = TABLE_PK_HINTS.get(table_name, [])
    existing_hints = [column for column in hinted_columns if column in df.columns]
    if existing_hints:
        return existing_hints

    fallback_columns = [column for column in df.columns if column.endswith("_id")]
    return fallback_columns[:1]


def build_input_path(table_name: str) -> str:
    if RAW_PREFIX.startswith("topics/"):
        return f"s3a://{MINIO_BUCKET}/{RAW_PREFIX}postgres.public.{table_name}/"
    if RAW_PREFIX.startswith("raw-cdc/"):
        safe_topic = f"socialrec_cdc_public_{table_name}"
        return f"s3a://{MINIO_BUCKET}/{RAW_PREFIX}topic={safe_topic}/"
    return f"s3a://{MINIO_BUCKET}/{RAW_PREFIX}postgres.public.{table_name}/"


def load_raw_events(spark: SparkSession, table_name: str) -> DataFrame:
    input_path = build_input_path(table_name)

    if INPUT_FORMAT == "json":
        raw_df = (
            spark.read.option("multiLine", "false")
            .option("recursiveFileLookup", "true")
            .json(input_path)
        )
    else:
        raise ValueError(f"Unsupported INPUT_FORMAT: {INPUT_FORMAT}")

    return raw_df.withColumn("_object_name", F.regexp_replace(F.input_file_name(), r"^s3a://[^/]+/", ""))


def coerce_payload_columns(raw_df: DataFrame):
    before_dtype = raw_df.schema["before"].dataType
    after_dtype = raw_df.schema["after"].dataType

    before_col = F.col("before")
    after_col = F.col("after")

    if isinstance(before_dtype, StringType) and isinstance(after_dtype, StructType):
        before_col = F.from_json(F.col("before"), after_dtype)
    elif isinstance(before_dtype, StructType) and isinstance(after_dtype, StringType):
        after_col = F.from_json(F.col("after"), before_dtype)
    elif isinstance(before_dtype, StringType) and isinstance(after_dtype, StringType):
        raise ValueError("Both 'before' and 'after' were inferred as strings; payload schema inference needs an explicit schema.")

    return before_col, after_col


def normalize_events(raw_df: DataFrame) -> DataFrame:
    object_name = F.col("_object_name")
    topic_name = F.regexp_extract(object_name, r"^topics/([^/]+)/", 1)
    raw_cdc_topic_name = F.regexp_extract(object_name, r"^raw-cdc/topic=([^/]+)/", 1)
    object_topic = F.when(topic_name != "", topic_name).otherwise(raw_cdc_topic_name)

    topic_table = F.regexp_extract(topic_name, r"^[^.]+\.[^.]+\.([^.]+)$", 1)
    raw_cdc_table = F.regexp_extract(raw_cdc_topic_name, r".*_([^_]+)$", 1)

    object_schema = F.when(
        topic_name != "",
        F.regexp_extract(topic_name, r"^[^.]+\.([^.]+)\.[^.]+$", 1),
    ).otherwise(F.lit("public"))

    object_date = F.when(
        object_name.startswith("topics/"),
        F.concat_ws(
            "-",
            F.regexp_extract(object_name, r"/year=(\d{4})/", 1),
            F.regexp_extract(object_name, r"/month=(\d{2})/", 1),
            F.regexp_extract(object_name, r"/day=(\d{2})/", 1),
        ),
    ).otherwise(F.regexp_extract(object_name, r"/date=(\d{4}-\d{2}-\d{2})/", 1))

    object_hour = F.when(
        object_name.startswith("topics/"),
        F.regexp_extract(object_name, r"/hour=(\d{2})/", 1),
    ).otherwise(F.regexp_extract(object_name, r"/hour=(\d{2})/", 1))

    source_schema = F.col("source.schema") if "source" in raw_df.columns else F.lit(None)
    source_table = F.col("source.table") if "source" in raw_df.columns else F.lit(None)
    source_ts_ms = F.col("source.ts_ms").cast("long") if "source" in raw_df.columns else F.lit(None).cast("long")

    event_ts_ms = F.coalesce(F.col("ts_ms").cast("long"), source_ts_ms)
    is_delete = F.col("op") == F.lit("d")
    before_payload, after_payload = coerce_payload_columns(raw_df)
    payload = F.when(is_delete, before_payload).otherwise(after_payload)

    return (
        raw_df.withColumn("topic_name", object_topic)
        .withColumn("schema_name", F.coalesce(source_schema, object_schema))
        .withColumn("table_name", F.coalesce(source_table, F.when(topic_table != "", topic_table).otherwise(raw_cdc_table)))
        .withColumn("object_date", F.when(object_date != "", object_date))
        .withColumn("object_hour", F.when(object_hour != "", object_hour))
        .withColumn("source_ts_ms", source_ts_ms)
        .withColumn("event_ts_ms", event_ts_ms)
        .withColumn("event_ts", F.to_timestamp(F.from_unixtime(event_ts_ms / F.lit(1000.0))))
        .withColumn("is_delete", is_delete)
        .withColumn("payload", payload)
    )


def build_current_state_table(events_df: DataFrame, table_name: str) -> DataFrame:
    table_events = (
        events_df.filter(F.col("table_name") == F.lit(table_name))
        .filter(F.col("payload").isNotNull())
    )

    payload_fields = table_events.select("payload.*").schema.fieldNames()
    metadata_columns = [
        "op",
        "event_ts",
        "event_ts_ms",
        "source_ts_ms",
        "schema_name",
        "table_name",
        "topic_name",
        "object_date",
        "object_hour",
        "_object_name",
        "is_delete",
    ]

    selected_columns = [F.col(f"payload.`{field}`").alias(field) for field in payload_fields]
    selected_columns.extend(F.col(column) for column in metadata_columns if column in table_events.columns)
    clean_df = table_events.select(*selected_columns)

    # # Drop payload columns that are entirely null (cross-table schema artifacts)
    # null_summary = clean_df.select(
    #     *[F.sum(F.col(c).isNotNull().cast("long")).alias(c) for c in payload_fields]
    # ).first()
    # drop_cols = [c for c in payload_fields if (null_summary[c] or 0) == 0]
    # if drop_cols:
    #     clean_df = clean_df.drop(*drop_cols)

    pk_columns = infer_table_pk_columns(clean_df, table_name)
    if pk_columns:
        order_columns = [
            F.col("event_ts_ms").desc_nulls_last(),
            F.col("source_ts_ms").desc_nulls_last(),
            F.col("_object_name").desc_nulls_last(),
        ]
        window = Window.partitionBy(*pk_columns).orderBy(*order_columns)
        clean_df = clean_df.withColumn("_row_number", F.row_number().over(window))
        clean_df = clean_df.filter(F.col("_row_number") == 1).drop("_row_number")

    return clean_df.filter(~F.col("is_delete"))


def write_dataframe(df: DataFrame, output_path: str) -> None:
    df.write.mode(WRITE_MODE).parquet(output_path)


def main() -> None:
    spark = create_spark_session()
    events_frames: List[DataFrame] = []

    for table_name in TABLE_NAMES:
        raw_df = load_raw_events(spark, table_name)
        events_df = normalize_events(raw_df).cache()
        events_frames.append(events_df)

        clean_df = build_current_state_table(events_df, table_name)
        write_dataframe(clean_df, f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}/{table_name}")

    if events_frames:
        combined_events_df = events_frames[0]
        for next_df in events_frames[1:]:
            combined_events_df = combined_events_df.unionByName(next_df, allowMissingColumns=True)
        write_dataframe(combined_events_df, f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}/events")

    spark.stop()


if __name__ == "__main__":
    main()
