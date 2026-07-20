"""
Gold layer: user-level features for personalization/recommendation
models, aggregated daily from Silver. This is the feature store surface
that a model-training job or a real-time serving layer would read from.

Run: python -m src.gold.build_gold_feature_store
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

SILVER_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "silver" / "events")
GOLD_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "gold" / "user_features")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("gold_build_user_features")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def run(spark: SparkSession) -> int:
    silver = spark.read.format("delta").load(SILVER_PATH)
    silver = silver.withColumn("event_date", F.to_date("event_ts"))

    # Batch feature: daily watch time and completion behavior per user.
    daily_watch = silver.groupBy("user_id", "event_date").agg(
        F.sum("watch_seconds").alias("total_watch_seconds"),
        F.countDistinct("content_id").alias("distinct_titles_watched"),
        F.sum(F.when(F.col("event_type") == "play_complete", 1).otherwise(0)).alias("completions"),
        F.sum(F.when(F.col("event_type") == "abandon", 1).otherwise(0)).alias("abandons"),
    )

    # Genre affinity: share of a user's watch time per genre, all-time.
    genre_time = silver.groupBy("user_id", "genre").agg(
        F.sum("watch_seconds").alias("genre_watch_seconds")
    )
    total_time = silver.groupBy("user_id").agg(
        F.sum("watch_seconds").alias("total_watch_seconds_all_time")
    )
    genre_affinity = (
        genre_time.join(total_time, "user_id")
        .withColumn(
            "genre_affinity_score",
            F.when(F.col("total_watch_seconds_all_time") > 0,
                   F.col("genre_watch_seconds") / F.col("total_watch_seconds_all_time"))
            .otherwise(F.lit(0.0)),
        )
    )

    top_genre = (
        genre_affinity.withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(F.col("genre_affinity_score").desc())
            ),
        )
        .filter(F.col("rn") == 1)
        .select("user_id", F.col("genre").alias("top_genre"), "genre_affinity_score")
    )

    # Simple churn-risk heuristic feature: no completions and >=1 abandon
    # in the trailing window flags elevated churn risk. Illustrative only.
    churn_risk = daily_watch.withColumn(
        "churn_risk_flag",
        F.when((F.col("completions") == 0) & (F.col("abandons") >= 1), F.lit(True)).otherwise(F.lit(False)),
    ).select("user_id", "event_date", "churn_risk_flag")

    features = (
        daily_watch.join(top_genre, "user_id", "left")
        .join(churn_risk, ["user_id", "event_date"], "left")
    )

    row_count = features.count()

    (
        features.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("event_date")
        .save(GOLD_PATH)
    )

    print(f"[gold] user_feature_rows={row_count} -> {GOLD_PATH}")
    return row_count


if __name__ == "__main__":
    spark = get_spark()
    n = run(spark)
    spark.stop()
    sys.exit(0)
