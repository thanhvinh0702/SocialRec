import json
import os
import io
import ast
from datetime import datetime
from typing import Iterator

from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, FloatType, 
    ArrayType, BooleanType, TimestampType
)

# Configuration from environment variables
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio-batch:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "socialrec-batch")
ASSETS_ENDPOINT = os.getenv("ASSETS_ENDPOINT", "http://minio-batch:9000")
MINIO_PATH_STYLE = os.getenv("MINIO_PATH_STYLE", "true").lower() == "true"
MINIO_REGION = os.getenv("MINIO_REGION", "us-east-1")

SILVER_PREFIX = "silver"
GOLD_PREFIX = "gold"

def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("socialrec-phase2-feature-engineering")
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


def strip_metadata(df: DataFrame) -> DataFrame:
    meta_cols = [
        "op", "event_ts", "event_ts_ms", "source_ts_ms", "schema_name",
        "table_name", "topic_name", "object_date", "object_hour",
        "_object_name", "is_delete"
    ]
    return df.drop(*[c for c in meta_cols if c in df.columns])

# ─── USER FEATURES ──────────────────────────────────────────────────────────

def build_user_features(spark: SparkSession, users_df: DataFrame, interactions_df: DataFrame) -> DataFrame:
    users_clean = strip_metadata(users_df)
    interactions_clean = strip_metadata(interactions_df)
    
    # Parse interest keywords using a standard UDF
    @F.udf(returnType=ArrayType(StringType()))
    def parse_keywords(val):
        if not val: return []
        try: return json.loads(val)
        except: return []
        
    users_clean = users_clean.withColumn("interest_kw_list", parse_keywords(F.col("interest_keywords")))
    users_clean = users_clean.withColumn("interest_kw_text", F.array_join(F.col("interest_kw_list"), ", "))
    
    # Tenure days
    reference_date = F.lit("2026-05-18").cast("date")
    users_clean = users_clean.withColumn("joined_date", F.to_date(F.from_unixtime(F.col("joined_on") / 1000)))
    users_clean = users_clean.withColumn("tenure_days", F.datediff(reference_date, F.col("joined_date")))
    
    # Aggregate interactions
    user_agg = interactions_clean.groupBy("user_id").agg(
        F.count("event_id").alias("total_interactions"),
        F.sum(F.when(F.col("interaction_type") == "view", 1).otherwise(0)).alias("num_views"),
        F.sum(F.when(F.col("interaction_type") == "like", 1).otherwise(0)).alias("num_likes"),
        F.sum(F.when(F.col("interaction_type") == "comment", 1).otherwise(0)).alias("num_comments_interaction"),
        F.sum(F.when(F.col("interaction_type") == "save", 1).otherwise(0)).alias("num_saves"),
        F.sum(F.when(F.col("interaction_type") == "share", 1).otherwise(0)).alias("num_shares"),
        F.sum(F.when(F.col("interaction_type") == "follow_author", 1).otherwise(0)).alias("num_follows"),
        F.avg("dwell_time_seconds").alias("avg_dwell_time"),
        F.countDistinct("post_id").alias("num_unique_posts"),
        F.countDistinct("session_id").alias("num_sessions")
    )
    
    # Calculate mode for device and surface using Window
    def get_mode_col(df: DataFrame, partition_col: str, target_col: str, alias: str) -> DataFrame:
        # Count occurrences of target_col per partition_col
        counts = df.groupBy(partition_col, target_col).count()
        # Rank by count descending
        w = Window.partitionBy(partition_col).orderBy(F.col("count").desc())
        return counts.withColumn("rn", F.row_number().over(w)) \
                     .filter(F.col("rn") == 1) \
                     .select(partition_col, F.col(target_col).alias(alias))

    preferred_device = get_mode_col(interactions_clean, "user_id", "device_type", "preferred_device")
    preferred_surface = get_mode_col(interactions_clean, "user_id", "source_surface", "preferred_surface")
    
    # Join everything
    user_features = users_clean \
        .join(user_agg, on="user_id", how="left") \
        .join(preferred_device, on="user_id", how="left") \
        .join(preferred_surface, on="user_id", how="left")
        
    user_features = user_features.fillna(0, subset=["avg_dwell_time", "total_interactions"])
    return user_features

# ─── POST FEATURES ──────────────────────────────────────────────────────────

def build_post_features(spark: SparkSession, posts_df: DataFrame, interactions_df: DataFrame, post_media_df: DataFrame) -> DataFrame:
    posts_clean = strip_metadata(posts_df)
    interactions_clean = strip_metadata(interactions_df)
    post_media_clean = strip_metadata(post_media_df)
    
    @F.udf(returnType=ArrayType(StringType()))
    def parse_str_list(val):
        if not val: return []
        try: return ast.literal_eval(val)
        except:
            val = val.strip("[]")
            return [s.strip("' \"") for s in val.split("' '") if s.strip("' \"")]

    posts_clean = posts_clean.withColumn("categories_list", parse_str_list(F.col("categories")))
    posts_clean = posts_clean.withColumn("tags_list", parse_str_list(F.col("tags")))
    
    # Body word count approximation (split by space)
    posts_clean = posts_clean.withColumn("body_word_count", F.size(F.split(F.col("body_text"), " ")))
    
    # Aggregate interactions
    post_agg = interactions_clean.groupBy("post_id").agg(
        F.count("event_id").alias("total_interactions"),
        F.sum(F.when(F.col("interaction_type") == "view", 1).otherwise(0)).alias("num_views"),
        F.sum(F.when(F.col("interaction_type") == "like", 1).otherwise(0)).alias("num_likes"),
        F.sum(F.when(F.col("interaction_type") == "comment", 1).otherwise(0)).alias("num_post_comments"),
        F.sum(F.when(F.col("interaction_type") == "save", 1).otherwise(0)).alias("num_saves"),
        F.sum(F.when(F.col("interaction_type") == "share", 1).otherwise(0)).alias("num_shares"),
        F.avg("dwell_time_seconds").alias("avg_dwell_time"),
        F.countDistinct("user_id").alias("num_unique_users")
    )
    
    post_agg = post_agg.withColumn("like_rate", F.col("num_likes") / F.greatest(F.col("num_views"), F.lit(1)))
    post_agg = post_agg.withColumn("comment_rate", F.col("num_post_comments") / F.greatest(F.col("num_views"), F.lit(1)))
    
    # Post media (first image)
    w_media = Window.partitionBy("post_id").orderBy(F.col("object_key").desc())
    post_image_map = post_media_clean.withColumn("rn", F.row_number().over(w_media)) \
                                     .filter(F.col("rn") == 1) \
                                     .select("post_id", "object_key")
                                     
    # Join everything
    post_features = posts_clean \
        .join(post_agg, on="post_id", how="left") \
        .join(post_image_map, on="post_id", how="left")
        
    post_features = post_features.withColumn("has_image", F.col("object_key").isNotNull())
    return post_features

# ─── INTERACTION LABELS ─────────────────────────────────────────────────────

def build_interaction_labels(spark: SparkSession, interactions_df: DataFrame) -> DataFrame:
    interactions_clean = strip_metadata(interactions_df)
    
    # Parse timestamp
    df = interactions_clean.withColumn("occurred_at", F.to_timestamp(F.col("occurred_at_raw")))
    df = df.withColumn("hour_of_day", F.hour(F.col("occurred_at")))
    df = df.withColumn("day_of_week", F.dayofweek(F.col("occurred_at")) - 1) # Spark dayofweek is 1-7 (Sun-Sat)
    
    # Engagement score label
    df = df.withColumn("label",
        F.when(F.col("interaction_type") == "like", 2.0)
         .when(F.col("interaction_type") == "comment", 3.0)
         .when(F.col("interaction_type") == "save", 4.0)
         .when(F.col("interaction_type") == "share", 5.0)
         .when(F.col("interaction_type") == "follow_author", 5.0)
         .when((F.col("interaction_type") == "view") & (F.col("dwell_time_seconds") >= 30), 1.0)
         .otherwise(0.0)
    )
    
    # Deduplicate: Aggregate to 1 row per (user_id, post_id)
    # Get max label, and get latest metadata using window functions
    w_latest = Window.partitionBy("user_id", "post_id").orderBy(F.col("occurred_at").desc_nulls_last())
    
    latest_events = df.withColumn("rn", F.row_number().over(w_latest)) \
                      .filter(F.col("rn") == 1) \
                      .select("user_id", "post_id", "event_id", "interaction_type", 
                              "hour_of_day", "day_of_week", "device_type", 
                              "source_surface", "occurred_at")
                              
    max_labels = df.groupBy("user_id", "post_id").agg(F.max("label").alias("label"))
    
    interaction_labels = max_labels.join(latest_events, on=["user_id", "post_id"], how="inner")
    return interaction_labels

# ─── MAIN ───────────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    
    # 1. Load Silver Data
    silver_path = f"s3a://{MINIO_BUCKET}/{SILVER_PREFIX}"
    users_df = spark.read.parquet(f"{silver_path}/users")
    posts_df = spark.read.parquet(f"{silver_path}/posts")
    interactions_df = spark.read.parquet(f"{silver_path}/interactions")
    comments_df = spark.read.parquet(f"{silver_path}/comments")
    post_media_df = spark.read.parquet(f"{silver_path}/post_media")
    
    # 2. Build Gold Feature Tables
    print("Building user features...")
    user_features = build_user_features(spark, users_df, interactions_df)
    
    print("Building post features...")
    post_features = build_post_features(spark, posts_df, interactions_df, post_media_df)
    
    print("Building interaction labels...")
    interaction_labels = build_interaction_labels(spark, interactions_df)
    
    # 3. Create Training Pairs (Scalar only)
    user_feat_cols = [
        'user_id', 'account_type', 'tenure_days', 'authored_posts_count',
        'comments_count', 'total_interactions', 'num_views', 'num_likes',
        'num_comments_interaction', 'num_saves', 'num_shares', 'num_follows',
        'avg_dwell_time', 'num_unique_posts', 'num_sessions',
        'preferred_device', 'preferred_surface'
    ]
    user_feat_compact = user_features.select(*[c for c in user_feat_cols if c in user_features.columns])
    
    post_feat_cols = [
        'post_id', 'author_id', 'body_word_count', 'comments_count',
        'total_interactions', 'num_views', 'num_likes', 'avg_dwell_time',
        'num_unique_users', 'like_rate', 'comment_rate', 'has_image'
    ]
    post_feat_compact = post_features.select(*[c for c in post_feat_cols if c in post_features.columns])
    
    # Rename overlaps
    overlap_cols = set(user_feat_compact.columns) & set(post_feat_compact.columns) - {'user_id', 'post_id'}
    for c in overlap_cols:
        user_feat_compact = user_feat_compact.withColumnRenamed(c, f"user_{c}")
        post_feat_compact = post_feat_compact.withColumnRenamed(c, f"post_{c}")
        
    training_pairs = interaction_labels \
        .join(user_feat_compact, on="user_id", how="left") \
        .join(post_feat_compact, on="post_id", how="left")
        
    # 4. Write Scalar Parquets to Gold
    gold_path = f"s3a://{MINIO_BUCKET}/{GOLD_PREFIX}"
    
    print("Writing gold tables...")
    user_feat_compact.write.mode("overwrite").parquet(f"{gold_path}/user_features")
    post_feat_compact.write.mode("overwrite").parquet(f"{gold_path}/post_features")
    interaction_labels.write.mode("overwrite").parquet(f"{gold_path}/interaction_labels")
    training_pairs.write.mode("overwrite").parquet(f"{gold_path}/training_pairs")
    print("✅ Gold scalar features successfully written!")
    
    # Note: Embedding generation (SentenceTransformer / CLIP) is compute intensive
    # and typically runs on GPU nodes. In a production Spark job without GPUs attached, 
    # we would export the raw text/image keys to object storage, and run a dedicated
    # PyTorch batch job to generate the .npy arrays.

if __name__ == "__main__":
    main()
