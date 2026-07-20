"""
Tests the dedupe/cleaning logic in isolation using a local SparkSession
(no Delta Lake I/O — just DataFrame transform correctness).

Requires: pip install pyspark pytest
Run: pytest -v
"""
import sys
from pathlib import Path

import pytest

sys.path.append(str(Path(__file__).resolve().parent.parent))

pyspark = pytest.importorskip("pyspark")
from pyspark.sql import SparkSession, Window  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    session = SparkSession.builder.master("local[2]").appName("test_silver").getOrCreate()
    yield session
    session.stop()


def _dedupe(df):
    """Mirrors the dedupe logic in src/silver/build_silver_features.py."""
    window = Window.partitionBy(
        "user_id", "content_id", "event_type", "event_ts"
    ).orderBy(F.col("bronze_ingested_at").desc())
    return (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )


def test_dedupe_removes_exact_duplicate_events(spark):
    rows = [
        ("U1", "C1", "play_start", "2026-01-01T00:00:00", "2026-06-01T00:00:01"),
        ("U1", "C1", "play_start", "2026-01-01T00:00:00", "2026-06-01T00:00:01"),  # duplicate
    ]
    df = spark.createDataFrame(
        rows, ["user_id", "content_id", "event_type", "event_ts", "bronze_ingested_at"]
    )
    result = _dedupe(df)
    assert result.count() == 1


def test_device_null_is_filled_with_sentinel(spark):
    df = spark.createDataFrame([("U1", None)], ["user_id", "device"])
    filled = df.withColumn("device", F.coalesce(F.col("device"), F.lit("unknown")))
    assert filled.collect()[0]["device"] == "unknown"
