import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ==========================================
# CẤU HÌNH BIẾN MÔI TRƯỜNG
# ==========================================
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "socialrec-batch")
SILVER_PREFIX = os.getenv("SILVER_PREFIX", "silver")
GOLD_PREFIX = os.getenv("GOLD_PREFIX", "gold_bi")  # Thư mục đích cho Star Schema
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-batch:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")
MINIO_PATH_STYLE = os.getenv("MINIO_PATH_STYLE", "true").lower() == "true"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("socialrec-phase2-starschema")
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


def build_dim_users(silver_users: DataFrame) -> DataFrame:
    """Tạo bảng Chiều: dim_users"""
    return silver_users.select(
        F.col("user_id").cast("string"),
        F.col("username").cast("string"),
        F.col("account_type").cast("string"),
        F.col("interest_keywords").cast("string"),
        F.col("joined_on").cast("date")
    ).filter(F.col("user_id").isNotNull())


def build_dim_posts(silver_posts: DataFrame) -> DataFrame:
    """Tạo bảng Chiều: dim_posts"""
    return silver_posts.select(
        F.col("post_id").cast("string"),
        F.col("author_id").cast("string"),
        F.coalesce(F.col("title"), F.lit("(untitled)")).alias("title"),
        F.col("categories").cast("string"),
        F.col("tags").cast("string"),
        F.to_timestamp(F.col("published_at_raw")).alias("published_at")
    ).filter(F.col("post_id").isNotNull())


def build_fact_interactions(silver_interactions: DataFrame) -> DataFrame:
    """Tạo bảng Sự kiện: fact_interactions"""
    return silver_interactions.select(
        F.col("event_id").cast("string"),
        F.col("user_id").cast("string"),
        F.col("post_id").cast("string"),
        F.col("interaction_type").cast("string"),
        F.col("session_id").cast("string"),
        F.col("device_type").cast("string"),
        F.col("dwell_time_seconds").cast("integer"),
        # Xử lý thời gian từ chuỗi thô của Postgres sang Timestamp chuẩn của Spark
        F.to_timestamp(F.col("occurred_at_raw")).alias("occurred_at")
    ).filter(
        F.col("event_id").isNotNull() & 
        F.col("user_id").isNotNull() & 
        F.col("post_id").isNotNull()
    )


def main() -> None:
    spark = create_spark_session()
    
    # 1. Định nghĩa đường dẫn đọc từ Silver Layer (do file của bạn kia sinh ra)
    silver_users_path = f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}/users"
    silver_posts_path = f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}/posts"
    silver_interactions_path = f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}/interactions"

    # 2. Đọc dữ liệu Silver
    try:
        silver_users = spark.read.parquet(silver_users_path)
        silver_posts = spark.read.parquet(silver_posts_path)
        silver_interactions = spark.read.parquet(silver_interactions_path)
    except Exception as e:
        print(f"Không thể đọc dữ liệu Silver. Đảm bảo luồng preprocess đã chạy. Lỗi: {e}")
        spark.stop()
        return

    # 3. Biến đổi thành Star Schema
    dim_users = build_dim_users(silver_users)
    dim_posts = build_dim_posts(silver_posts)
    fact_interactions = build_fact_interactions(silver_interactions)

    # 4. Ghi ra vùng Gold Warehouse cho Trino
    gold_base_path = f"s3a://{MINIO_BUCKET}/{GOLD_PREFIX}"
    
    # Bảng Dimension: Nhỏ, ít cập nhật -> Ghi đè bình thường
    dim_users.write.mode("overwrite").parquet(f"{gold_base_path}/dim_users")
    dim_posts.write.mode("overwrite").parquet(f"{gold_base_path}/dim_posts")
    
    # Bảng Fact: Lớn, phình to liên tục -> Phân vùng theo loại tương tác (Partition)
    fact_interactions.write.mode("overwrite") \
        .partitionBy("interaction_type") \
        .parquet(f"{gold_base_path}/fact_interactions")
        
    print(f"Đã tạo Star Schema thành công tại: {gold_base_path}")
    spark.stop()


if __name__ == "__main__":
    main()