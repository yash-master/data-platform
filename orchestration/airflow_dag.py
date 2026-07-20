"""
Orchestrates the medallion pipeline: Bronze -> Silver -> Gold, daily.

In production this would run on a Databricks Jobs cluster (each task a
notebook/JAR task); locally it runs the same PySpark entry points as
plain subprocess/python calls via Airflow's PythonVirtualenvOperator-free
BashOperator, so the DAG logic is identical either way.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "yash.master",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


@dag(
    dag_id="streaming_feature_pipeline",
    default_args=default_args,
    description="Bronze -> Silver -> Gold medallion pipeline for streaming viewing events",
    schedule_interval="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["databricks", "delta-lake", "feature-store", "portfolio"],
)
def streaming_feature_pipeline():
    bronze = BashOperator(
        task_id="bronze_ingest_events",
        bash_command="python -m src.bronze.ingest_events",
    )

    silver = BashOperator(
        task_id="silver_build_features",
        bash_command="python -m src.silver.build_silver_features",
    )

    gold = BashOperator(
        task_id="gold_build_user_features",
        bash_command="python -m src.gold.build_gold_feature_store",
    )

    bronze >> silver >> gold


streaming_feature_pipeline()
