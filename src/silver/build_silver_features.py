"""
Silver layer: dedupe, clean, and apply watermark-based incremental
processing on top of Bronze. This is where "raw event" becomes a
trustworthy, deduplicated record.

Run: python -m src.silver.build_silver_features
"""
import sys
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

BRONZE_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "bronze" / "events")
SILVER_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "silver" / "events")

# Only re-process events with a bronze_ingested_at newer than this watermark
# on incremental runs. On the very first run there's nothing written yet,
# so the job just processes everything in Bronze.
WATERMARK_TABLE_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "_watermarks" / "silver_events.txt")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("silver_build_events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def read_watermark() -> str | None:
    p = Path(WATERMARK_TABLE_PATH)
    if p.exists():
        return p.read_text().strip()
    return None


def write_watermark(value: str) -> None:
    p = Path(WATERMARK_TABLE_PATH)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(value)


def run(spark: SparkSession) -> int:
    bronze = spark.read.format("delta").load(BRONZE_PATH)

    watermark = read_watermark()
    if watermark:
        bronze = bronze.filter(F.col("bronze_ingested_at") > F.lit(watermark))

    if bronze.rdd.isEmpty():
        print("[silver] no new rows since last watermark, nothing to do")
        return 0

    # Fill missing device with an explicit sentinel instead of leaving null,
    # so "unknown device" is a queryable, testable value.
    cleaned = bronze.withColumn(
        "device", F.coalesce(F.col("device"), F.lit("unknown"))
    ).withColumn(
        "event_ts", F.to_timestamp("event_ts")
    )

    # Dedupe: at-least-once delivery means the same logical event can show
    # up twice. Keep one row per (user_id, content_id, event_type, event_ts).
    dedup_window = Window.partitionBy(
        "user_id", "content_id", "event_type", "event_ts"
    ).orderBy(F.col("bronze_ingested_at").desc())

    deduped = (
        cleaned.withColumn("_rn", F.row_number().over(dedup_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    row_count = deduped.count()
    max_ingested_at = bronze.agg(F.max("bronze_ingested_at")).collect()[0][0]

    (
        deduped.write.format("delta")
        .mode("append")
        .partitionBy("ingestion_batch_ts")
        .save(SILVER_PATH)
    )

    if max_ingested_at:
        write_watermark(max_ingested_at)

    print(f"[silver] rows_processed={row_count} new_watermark={max_ingested_at}")
    return row_count


if __name__ == "__main__":
    spark = get_spark()
    n = run(spark)
    spark.stop()
    sys.exit(0)
