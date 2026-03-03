"""
dags/sales_pipeline_dag.py
==========================
Airflow DAG: MinIO → Validate → Transform → PostgreSQL → Archive

Processes three CSV file types per run:
  1. customers_*.csv  — customer dimension
  2. products_*.csv   — product dimension (+ auto-extracted categories)
  3. sales_*.csv      — transaction fact table (+ returns extraction)

Schedule: every 15 minutes
Retry:    3 attempts with 2-minute back-off
"""

from __future__ import annotations

import logging
import os
import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import boto3
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from botocore.client import Config

# Airflow runs inside the container; include/ is on PYTHONPATH via volume mount
from include.config import settings
from include.db_loader import (
    get_connection,
    insert_returned_orders,
    log_pipeline_run,
    update_customer_lifetime_values,
    upsert_categories,
    upsert_customers,
    upsert_orders,
    upsert_products,
    upsert_purchased_products,
)
from include.transformations import (
    build_product_aggregations,
    clean_and_transform,
    clean_customers,
    clean_products,
    extract_categories,
    extract_returns,
)

logger = logging.getLogger(__name__)

# === DAG default args =============================================
DEFAULT_ARGS = {
    "owner": "data-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
}

# === Helpers =============================================================


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


def _classify_files(keys: list[str]) -> dict[str, str | None]:
    """Classify files by prefix: customers, products, sales."""
    result: dict[str, str | None] = {"customers": None, "products": None, "sales": None}
    for key in keys:
        basename = key.lower()
        if basename.startswith("customers"):
            result["customers"] = key
        elif basename.startswith("products"):
            result["products"] = key
        elif basename.startswith("sales"):
            result["sales"] = key
    return result


def _on_failure_callback(context: dict) -> None:
    """Log failed pipeline runs to the audit table."""
    try:
        dag_run_id = context.get("run_id", "unknown")
        with get_connection() as conn:
            log_pipeline_run(
                conn=conn,
                dag_run_id=dag_run_id,
                file_processed="N/A",
                rows_inserted=0,
                rows_skipped=0,
                status="failed",
            )
    except Exception as exc:
        logger.error("Failed to log pipeline failure: %s", exc)


# === Task callables =============================================


def run_data_generator(**context) -> None:
    """Generate and upload data using the existing script."""
    import sys

    gen_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data-generator")
    if gen_path not in sys.path:
        sys.path.insert(0, gen_path)

    try:
        from generate_data import (
            generate_customers,
            generate_products,
            generate_transactions,
            upload_csv_to_minio,
        )
        from config import settings as gen_settings

        logger.info("DAG generating data (seed=%d)", gen_settings.generator_seed)

        customers_df = generate_customers(gen_settings.generator_num_customers)
        products_df = generate_products()
        transactions_df = generate_transactions(
            customers_df["customer_id"].tolist(),
            products_df,
            gen_settings.generator_num_transactions,
        )

        keys = []
        keys.append(upload_csv_to_minio(customers_df, "customers"))
        keys.append(upload_csv_to_minio(products_df, "products"))
        keys.append(upload_csv_to_minio(transactions_df, "sales"))
        logger.info("DAG generated %d MinIO objects: %s", len(keys), keys)
    finally:
        if gen_path in sys.path:
            sys.path.remove(gen_path)


def download_from_minio(**context) -> None:
    """Download all pending CSVs (customers, products, sales) to temp files."""
    all_files = _list_pending_files()
    if not all_files:
        raise ValueError("No files found in MinIO raw-data bucket.")

    classified = _classify_files(all_files)
    logger.info("Classified files: %s", classified)

    client = _s3_client()
    local_paths: dict[str, str] = {}

    for file_type, object_key in classified.items():
        if object_key is None:
            logger.warning("No %s file found in raw-data bucket.", file_type)
            continue

        tmp = tempfile.NamedTemporaryFile(
            suffix=".csv", delete=False, prefix=f"{file_type}_raw_"
        )
        client.download_fileobj(settings.minio_raw_bucket, object_key, tmp)
        tmp.flush()
        tmp.close()
        local_paths[file_type] = tmp.name
        logger.info("Downloaded %s → %s", object_key, tmp.name)

    # Push all paths and keys via XCom
    ti = context["ti"]
    ti.xcom_push(key="classified_files", value=classified)
    ti.xcom_push(key="local_paths", value=local_paths)
    ti.xcom_push(key="all_object_keys", value=[k for k in classified.values() if k])


def validate_csv(**context) -> None:
    """Validate each downloaded CSV against its expected schema."""
    local_paths = context["ti"].xcom_pull(key="local_paths")

    # Validate customers
    if "customers" in local_paths:
        df = pd.read_csv(local_paths["customers"])
        if df.empty:
            raise ValueError("Customers CSV is empty.")
        required = {"customer_id", "name", "email", "region", "signup_date"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Customers CSV missing columns: {missing}")
        logger.info("Customers validation passed: %d rows.", len(df))

    # Validate products
    if "products" in local_paths:
        df = pd.read_csv(local_paths["products"])
        if df.empty:
            raise ValueError("Products CSV is empty.")
        required = {"product_id", "name", "category", "unit_price", "cost"}
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Products CSV missing columns: {missing}")
        logger.info("Products validation passed: %d rows.", len(df))

    # Validate sales/transactions
    if "sales" in local_paths:
        df = pd.read_csv(local_paths["sales"])
        if df.empty:
            raise ValueError("Sales CSV is empty.")
        required = {
            "order_id", "customer_id", "product_id",
            "quantity", "unit_price", "discount",
            "order_date", "status", "region",
        }
        missing = required - set(df.columns)
        if missing:
            raise ValueError(f"Sales CSV missing columns: {missing}")
        logger.info("Sales validation passed: %d rows.", len(df))


def transform_data(**context) -> None:
    """Clean & transform all CSVs; push cleaned parquet paths via XCom."""
    local_paths = context["ti"].xcom_pull(key="local_paths")
    cleaned_paths: dict[str, str] = {}
    total_skipped = 0

    # Transform customers
    if "customers" in local_paths:
        df_raw = pd.read_csv(local_paths["customers"])
        df_clean, skipped = clean_customers(df_raw)
        total_skipped += skipped
        out = local_paths["customers"].replace(".csv", "_cleaned.parquet")
        df_clean.to_parquet(out, index=False)
        cleaned_paths["customers"] = out

    # Transform products
    if "products" in local_paths:
        df_raw = pd.read_csv(local_paths["products"])
        df_clean, skipped = clean_products(df_raw)
        total_skipped += skipped
        out = local_paths["products"].replace(".csv", "_cleaned.parquet")
        df_clean.to_parquet(out, index=False)
        cleaned_paths["products"] = out

    # Transform sales/transactions
    if "sales" in local_paths:
        df_raw = pd.read_csv(local_paths["sales"])
        df_clean, skipped = clean_and_transform(df_raw)
        total_skipped += skipped
        out = local_paths["sales"].replace(".csv", "_cleaned.parquet")
        df_clean.to_parquet(out, index=False)
        cleaned_paths["sales"] = out

    context["ti"].xcom_push(key="cleaned_paths", value=cleaned_paths)
    context["ti"].xcom_push(key="rows_skipped", value=total_skipped)
    logger.info("Transformation complete. Cleaned paths: %s", cleaned_paths)


def load_to_postgres(**context) -> None:
    """
    Bulk-upsert all entities into PostgreSQL in FK-safe order:
    categories → products → customers → orders → returned_orders →
    purchased_products → update lifetime values
    """
    cleaned_paths = context["ti"].xcom_pull(key="cleaned_paths")
    rows_skipped = context["ti"].xcom_pull(key="rows_skipped") or 0
    all_keys = context["ti"].xcom_pull(key="all_object_keys") or []
    dag_run_id = context["run_id"]

    total_rows_inserted = 0

    with get_connection() as conn:
        # 1. Load products + extract categories
        products_df = None
        category_map = {}
        if "products" in cleaned_paths:
            products_df = pd.read_parquet(cleaned_paths["products"])
            cat_df = extract_categories(products_df)
            category_map = upsert_categories(cat_df, conn)
            upsert_products(products_df, conn, category_map)
            logger.info("Loaded %d products across %d categories.", len(products_df), len(category_map))

        # 2. Load customers
        if "customers" in cleaned_paths:
            customers_df = pd.read_parquet(cleaned_paths["customers"])
            upsert_customers(customers_df, conn)

        # 3. Load orders (transactions)
        if "sales" in cleaned_paths:
            orders_df = pd.read_parquet(cleaned_paths["sales"])
            rows_inserted, _ = upsert_orders(orders_df, conn)
            total_rows_inserted += rows_inserted

            # 4. Extract and load returned orders
            returns_df = extract_returns(orders_df)
            if not returns_df.empty:
                insert_returned_orders(returns_df, conn)

            # 5. Build and upsert purchased_products aggregation
            if products_df is not None:
                agg_df = build_product_aggregations(orders_df, products_df)
                upsert_purchased_products(agg_df, conn)

            # 6. Update customer lifetime values
            update_customer_lifetime_values(conn)

        # 7. Log pipeline run
        log_pipeline_run(
            conn=conn,
            dag_run_id=dag_run_id,
            file_processed=", ".join(all_keys),
            rows_inserted=total_rows_inserted,
            rows_skipped=rows_skipped,
            status="success",
        )

    logger.info(
        "Loaded %d order rows into PostgreSQL (skipped=%d).",
        total_rows_inserted, rows_skipped,
    )


def archive_file(**context) -> None:
    """Move all processed files from raw-data → processed-data bucket."""
    all_keys = context["ti"].xcom_pull(key="all_object_keys") or []
    client = _s3_client()

    for object_key in all_keys:
        # Copy to processed bucket
        client.copy_object(
            Bucket=settings.minio_processed_bucket,
            CopySource={"Bucket": settings.minio_raw_bucket, "Key": object_key},
            Key=object_key,
        )
        # Delete from raw bucket
        client.delete_object(Bucket=settings.minio_raw_bucket, Key=object_key)
        logger.info("Archived '%s' → processed-data bucket.", object_key)

    # Cleanup temp files
    local_paths = context["ti"].xcom_pull(key="local_paths") or {}
    cleaned_paths = context["ti"].xcom_pull(key="cleaned_paths") or {}
    for paths in [local_paths, cleaned_paths]:
        for path in paths.values():
            try:
                Path(path).unlink(missing_ok=True)
            except Exception:
                pass

    logger.info("Archived %d files to processed-data bucket.", len(all_keys))


# === DAG Definition =============================================
with DAG(
    dag_id="sales_pipeline_dag",
    description="E-Commerce sales ETL: MinIO → PostgreSQL (customers, products, orders, returns)",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 1, 1),
    schedule_interval="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["sales", "etl", "minio", "postgres"],
    on_failure_callback=_on_failure_callback,
) as dag:

    t_generate = PythonOperator(
        task_id="generate_data",
        python_callable=run_data_generator,
    )

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

    # === Task graph =============================================
    t_generate >> t_download >> t_validate >> t_transform >> t_load >> t_archive
