"""
tests/test_data_flow.py
========================
End-to-end data-flow validation tests.

Asserts that data successfully moved through:
  1. MinIO (raw-data → processed-data)
  2. Airflow (DAG completed successfully)
  3. PostgreSQL (orders + purchased_products populated)

Run locally after `docker compose up` and generating seed data:
    pytest tests/test_data_flow.py -v

Environment variables are loaded from .env via Pydantic Settings.
"""

from __future__ import annotations

import os
import sys

import boto3
import psycopg2
import pytest
from botocore.client import Config

# Allow importing include/ from project root
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from include.config import settings


# == Fixtures ===================================================

@pytest.fixture(scope="session")
def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


@pytest.fixture(scope="session")
def pg_conn():
    conn = psycopg2.connect(settings.postgres_dsn)
    conn.set_session(readonly=True, autocommit=True)
    yield conn
    conn.close()


# == Tests ======================================================

class TestMinIOFileIngestion:
    """Verify files were placed into the MinIO raw-data bucket."""

    def test_raw_bucket_exists(self, s3_client):
        buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
        assert settings.minio_raw_bucket in buckets, \
            f"Bucket '{settings.minio_raw_bucket}' not found. Buckets: {buckets}"

    def test_processed_bucket_exists(self, s3_client):
        buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
        assert settings.minio_processed_bucket in buckets, \
            f"Bucket '{settings.minio_processed_bucket}' not found."

    def test_file_archived_to_processed_bucket(self, s3_client):
        """After the DAG runs, the CSV should be in processed-data, not raw-data."""
        resp = s3_client.list_objects_v2(Bucket=settings.minio_processed_bucket)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        csv_files = [k for k in keys if k.endswith(".csv")]
        assert csv_files, (
            f"No CSV files found in '{settings.minio_processed_bucket}'. "
            "Has the Airflow DAG run to completion?"
        )


class TestPostgreSQLData:
    """Verify that records were successfully inserted into PostgreSQL."""

    def test_orders_table_has_rows(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders;")
            count = cur.fetchone()[0]
        assert count > 0, "orders table is empty — pipeline may not have run."

    def test_orders_revenue_is_positive(self, pg_conn):
        """total_revenue (generated column) should be > 0 for all completed orders."""
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE total_revenue <= 0 AND status = 'completed';"
            )
            bad_rows = cur.fetchone()[0]
        assert bad_rows == 0, f"{bad_rows} completed orders have non-positive revenue."

    def test_purchased_products_populated(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM purchased_products;")
            count = cur.fetchone()[0]
        assert count > 0, "purchased_products table is empty."

    def test_pipeline_runs_logged(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'success';")
            count = cur.fetchone()[0]
        assert count > 0, "No successful pipeline runs logged."

    def test_orders_have_valid_dates(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM orders WHERE order_date IS NULL OR order_date > CURRENT_DATE;"
            )
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} orders have null or future order_date."

    def test_no_null_order_ids(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM orders WHERE order_id IS NULL;")
            null_count = cur.fetchone()[0]
        assert null_count == 0, "Found orders with NULL order_id."


class TestTransformationUnit:
    """
    Fast, isolated unit tests for the transformation module.
    No Docker / DB required.
    """

    def test_clean_and_transform_basic(self):
        import pandas as pd
        from include.transformations import clean_and_transform

        data = {
            "order_id":    ["ord-001", "ord-002", "ord-001"],   # duplicate
            "customer_id": ["c1", "c2", "c1"],
            "product":     ["Laptop", "Shoes", "Laptop"],
            "category":    ["electronics", "apparel", "electronics"],
            "region":      ["north america", "europe", "north america"],
            "quantity":    [2, 1, 2],
            "unit_price":  [999.99, 49.95, 999.99],
            "discount":    [0.10, 0.0, 0.10],
            "order_date":  ["2024-06-01", "2024-07-15", "2024-06-01"],
            "status":      ["completed", "completed", "completed"],
        }
        df = pd.DataFrame(data)
        clean_df, skipped = clean_and_transform(df)

        assert len(clean_df) == 2, "Duplicate should be removed"
        assert "total_revenue" in clean_df.columns
        assert clean_df.loc[clean_df["order_id"] == "ord-001", "total_revenue"].iloc[0] == pytest.approx(
            2 * 999.99 * 0.90, rel=1e-3
        )
        assert clean_df["region"].iloc[0] == "North America"   # title-cased
        assert clean_df["category"].iloc[0] == "Electronics"

    def test_missing_columns_raises(self):
        import pandas as pd
        from include.transformations import clean_and_transform

        df = pd.DataFrame({"order_id": ["x"], "product": ["y"]})
        with pytest.raises(ValueError, match="missing required columns"):
            clean_and_transform(df)
