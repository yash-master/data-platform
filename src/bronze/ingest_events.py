"""
Bronze layer: land raw streaming-event JSON batches into a Delta table,
untouched except for ingestion metadata. This is the "raw ingestion
foundation" layer — no business logic here, just capture + lineage.

Run: python -m src.bronze.ingest_events
"""
import sys
import uuid
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

RAW_PATH = str(Path(__file__).resolve().parent.parent.parent / "data" / "raw" / "*.json")
BRONZE_PATH = str(Path(__file__).resolve().parent.parent.parent / "lakehouse" / "bronze" / "events")


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("bronze_ingest_events")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def run(spark: SparkSession) -> int:
    ingestion_run_id = str(uuid.uuid4())
    ingested_at = datetime.utcnow().isoformat()

    df = (
        spark.read.json(RAW_PATH)
        .withColumn("ingestion_run_id", F.lit(ingestion_run_id))
        .withColumn("bronze_ingested_at", F.lit(ingested_at))
        .withColumn("source_file", F.input_file_name())
    )

    row_count = df.count()

    (
        df.write.format("delta")
        .mode("append")
        .partitionBy("ingestion_batch_ts")
        .save(BRONZE_PATH)
    )

    print(f"[bronze] run_id={ingestion_run_id} rows_ingested={row_count} -> {BRONZE_PATH}")
    return row_count


if __name__ == "__main__":
    spark = get_spark()
    n = run(spark)
    spark.stop()
    sys.exit(0 if n > 0 else 1)
