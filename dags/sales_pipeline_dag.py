"""
dags/sales_pipeline_dag.py
──────────────────────────
Airflow DAG: MinIO → Validate → Transform → PostgreSQL → Archive

Schedule: every 15 minutes
Retry:    3 attempts with 2-minute back-off
"""

from __future__ import annotations

import io
import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.s3_key_sensor import S3KeySensor
from botocore.client import Config

# Airflow runs inside the container; include/ is on PYTHONPATH via volume mount
from include.config import settings
from include.db_loader import get_connection, log_pipeline_run, upsert_orders, upsert_purchased_products
from include.transformations import build_product_aggregations, clean_and_transform

logger = logging.getLogger(__name__)

# ── DAG default args ───────────────────────────────────────────
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# ── Helpers ────────────────────────────────────────────────────

def _s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def _list_pending_files() -> list[str]:
    """Return all object keys in the raw-data bucket."""
    client = _s3_client()
    resp = client.list_objects_v2(Bucket=settings.minio_raw_bucket)
    return [obj["Key"] for obj in resp.get("Contents", [])]


# ── Task callables ─────────────────────────────────────────────

def download_from_minio(**context) -> str:
    """Download the first pending CSV to a temp file; push XCom path."""
    files = _list_pending_files()
    if not files:
        raise ValueError("No files found in MinIO raw-data bucket.")

    object_key = files[0]
    logger.info("Downloading: %s", object_key)

    tmp = tempfile.NamedTemporaryFile(
        suffix=".csv", delete=False, prefix="sales_raw_"
    )
    _s3_client().download_fileobj(settings.minio_raw_bucket, object_key, tmp)
    tmp.flush()

    context["ti"].xcom_push(key="object_key", value=object_key)
    context["ti"].xcom_push(key="local_path", value=tmp.name)
    logger.info("Saved to: %s", tmp.name)
    return tmp.name


def validate_csv(**context) -> None:
    """Load the downloaded CSV and check basic schema/size."""
    local_path = context["ti"].xcom_pull(key="local_path")
    df = pd.read_csv(local_path)

    if df.empty:
        raise ValueError("Downloaded CSV is empty.")

    required = {
        "order_id", "customer_id", "product", "category",
        "region", "quantity", "unit_price", "discount",
        "order_date", "status",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"CSV missing columns: {missing}")

    logger.info("Validation passed: %d rows, %d columns.", len(df), len(df.columns))


def transform_data(**context) -> None:
    """Clean & transform raw CSV; push cleaned parquet path via XCom."""
    local_path = context["ti"].xcom_pull(key="local_path")
    df_raw = pd.read_csv(local_path)

    df_clean, rows_skipped = clean_and_transform(df_raw)

    # Persist cleaned data to a temp parquet for the next task
    out_path = local_path.replace(".csv", "_cleaned.parquet")
    df_clean.to_parquet(out_path, index=False)

    context["ti"].xcom_push(key="cleaned_path", value=out_path)
    context["ti"].xcom_push(key="rows_skipped", value=rows_skipped)
    logger.info("Transformed %d → %d rows.", len(df_raw), len(df_clean))


def load_to_postgres(**context) -> None:
    """Bulk-upsert cleaned data into PostgreSQL orders + purchased_products tables."""
    cleaned_path = context["ti"].xcom_pull(key="cleaned_path")
    rows_skipped  = context["ti"].xcom_pull(key="rows_skipped") or 0
    object_key    = context["ti"].xcom_pull(key="object_key")
    dag_run_id    = context["run_id"]

    df = pd.read_parquet(cleaned_path)
    agg_df = build_product_aggregations(df)

    with get_connection() as conn:
        rows_inserted, _ = upsert_orders(df, conn)
        upsert_purchased_products(agg_df, conn)
        log_pipeline_run(
            conn=conn,
            dag_run_id=dag_run_id,
            file_processed=object_key,
            rows_inserted=rows_inserted,
            rows_skipped=rows_skipped,
            status="success",
        )

    logger.info(
        "Loaded %d rows into PostgreSQL (skipped=%d).", rows_inserted, rows_skipped
    )


def archive_file(**context) -> None:
    """Move processed file from raw-data → processed-data bucket."""
    object_key = context["ti"].xcom_pull(key="object_key")
    client = _s3_client()

    # Copy to processed bucket
    client.copy_object(
        Bucket=settings.minio_processed_bucket,
        CopySource={"Bucket": settings.minio_raw_bucket, "Key": object_key},
        Key=object_key,
    )
    # Delete from raw bucket
    client.delete_object(Bucket=settings.minio_raw_bucket, Key=object_key)

    # Cleanup temp files
    local_path   = context["ti"].xcom_pull(key="local_path")
    cleaned_path = context["ti"].xcom_pull(key="cleaned_path")
    for path in [local_path, cleaned_path]:
        try:
            Path(path).unlink(missing_ok=True)
        except Exception:
            pass

    logger.info("Archived '%s' → processed-data bucket.", object_key)


# ── DAG Definition ─────────────────────────────────────────────
with DAG(
    dag_id="sales_pipeline_dag",
    description="E-Commerce sales ETL: MinIO → PostgreSQL",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl", "minio", "postgres"],
) as dag:

    t_download = PythonOperator(
        task_id="download_from_minio",
        python_callable=download_from_minio,
    )

    t_validate = PythonOperator(
        task_id="validate_csv",
        python_callable=validate_csv,
    )

    t_transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    t_load = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    t_archive = PythonOperator(
        task_id="archive_file",
        python_callable=archive_file,
    )

    # ── Task graph ──────────────────────────────────────────────
    t_download >> t_validate >> t_transform >> t_load >> t_archive
