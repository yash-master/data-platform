# Streaming Feature Store — Databricks/Delta Lake Lakehouse (Local)

A medallion-architecture (Bronze → Silver → Gold) feature-engineering
pipeline for streaming-media viewing events, built with **PySpark + Delta
Lake**, runnable entirely on your laptop — no Databricks workspace or
cloud account required. Includes an equivalent **Snowflake SQL** layer for
when a real warehouse is available.

This mirrors the shape of feature-store work I've done at Disney/Hulu
(batch + streaming personalization features at scale), rebuilt from
scratch here on fully synthetic data.

## Architecture

```
data/generate_raw_events.py     synthetic streaming events (JSON batches)
        │
        ▼
src/bronze/ingest_events.py     land raw JSON → Delta, tag with
                                 ingestion_run_id + bronze_ingested_at
        │
        ▼
src/silver/build_silver_features.py
                                 dedupe (at-least-once delivery),
                                 sentinel-fill nulls, watermark-based
                                 incremental processing
        │
        ▼
src/gold/build_gold_feature_store.py
                                 daily watch-time, genre affinity,
                                 churn-risk feature table — the
                                 feature-store surface for model
                                 training / BI
        │
        ▼
orchestration/airflow_dag.py     daily DAG: bronze >> silver >> gold
```

`governance/UNITY_CATALOG_PATTERNS.md` documents how the local folder
layout maps onto a real Unity Catalog `catalog.schema.table` namespace and
access-control model. `snowflake/ddl/` has the equivalent Bronze/Silver/Gold
structure in Snowflake SQL (streams + tasks in place of Airflow +
watermark file) for when you want to stand this up against a real warehouse.

## Design choices worth calling out

- **Watermark-based incremental processing**: Silver only reprocesses rows
  newer than the last watermark, tracked in a small local file (stand-in
  for a proper metadata table in a warehouse).
- **Dedup**: the synthetic data generator deliberately injects ~1%
  duplicate events to simulate at-least-once delivery; Silver dedupes on
  `(user_id, content_id, event_type, event_ts)`, keeping the
  most-recently-ingested copy.
- **Sentinel over NULL**: missing `device` is filled with `'unknown'`
  rather than left NULL, so "unknown" is a queryable, testable value
  instead of an invisible gap.
- **Idempotent Gold**: the feature table is rebuilt with `overwrite` each
  run rather than appended, since it's a point-in-time aggregate — re-running
  it never double-counts.

## Setup

```bash
pip install -r requirements.txt
python data/generate_raw_events.py   # writes ~65K synthetic events to data/raw/
```

## Run the pipeline

```bash
python -m src.bronze.ingest_events
python -m src.silver.build_silver_features
python -m src.gold.build_gold_feature_store
```

Output Delta tables land under `lakehouse/{bronze,silver,gold}/` (gitignored —
regenerate locally rather than committing lake data to the repo).

Inspect the result:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.read.format("delta").load("lakehouse/gold/user_features").show(20)
```

## Run tests

```bash
pytest -v
```

## Run via Airflow

Copy `orchestration/airflow_dag.py` into your Airflow `dags/` folder with
this project's root on `PYTHONPATH`; it runs the same three commands above
as a daily DAG (`bronze >> silver >> gold`).

## Migrating to a real warehouse

- **Databricks**: swap local Delta paths for managed Unity Catalog tables
  (`catalog.schema.table`); the PySpark transform code doesn't change.
- **Snowflake**: run `snowflake/ddl/01_bronze.sql` → `02_silver.sql` →
  `03_gold.sql` against a real account; Silver/Gold are implemented as a
  Stream+Task and a view respectively, so they stay current without an
  external orchestrator.

## Tech stack

`PySpark` · `Delta Lake` · `Apache Airflow` · `Snowflake SQL` · `pytest`
