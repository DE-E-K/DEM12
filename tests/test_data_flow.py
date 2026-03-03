"""
tests/test_data_flow.py
========================
End-to-end data-flow validation tests.

Asserts that data successfully moved through:
  1. MinIO (raw-data → processed-data)
  2. Airflow (DAG completed successfully)
  3. PostgreSQL (all 7 tables populated correctly)

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


# == MinIO Tests ================================================


class TestMinIOFileIngestion:
    """Verify files were placed into the MinIO buckets."""

    def test_raw_bucket_exists(self, s3_client):
        buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
        assert settings.minio_raw_bucket in buckets, \
            f"Bucket '{settings.minio_raw_bucket}' not found. Buckets: {buckets}"

    def test_processed_bucket_exists(self, s3_client):
        buckets = [b["Name"] for b in s3_client.list_buckets().get("Buckets", [])]
        assert settings.minio_processed_bucket in buckets, \
            f"Bucket '{settings.minio_processed_bucket}' not found."

    def test_file_archived_to_processed_bucket(self, s3_client):
        """After the DAG runs, CSVs should be in processed-data, not raw-data."""
        resp = s3_client.list_objects_v2(Bucket=settings.minio_processed_bucket)
        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        csv_files = [k for k in keys if k.endswith(".csv")]
        assert csv_files, (
            f"No CSV files found in '{settings.minio_processed_bucket}'. "
            "Has the Airflow DAG run to completion?"
        )


# == Product Categories Tests ===================================


class TestProductCategories:
    """Verify that the product_categories dimension is populated."""

    def test_categories_table_has_rows(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM product_categories;")
            count = cur.fetchone()[0]
        assert count > 0, "product_categories table is empty."

    def test_categories_have_unique_names(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT name, COUNT(*) FROM product_categories
                GROUP BY name HAVING COUNT(*) > 1;
            """)
            duplicates = cur.fetchall()
        assert len(duplicates) == 0, f"Duplicate categories found: {duplicates}"


# == Products Tests =============================================


class TestProducts:
    """Verify that the products dimension is populated."""

    def test_products_table_has_rows(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products;")
            count = cur.fetchone()[0]
        assert count >= 100, f"Expected >= 100 products, got {count}."

    def test_products_have_positive_margin(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products WHERE margin < 0;")
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} products have negative margin (cost > price)."

    def test_products_have_valid_category_fk(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM products p
                WHERE NOT EXISTS (
                    SELECT 1 FROM product_categories c WHERE c.category_id = p.category_id
                );
            """)
            orphans = cur.fetchone()[0]
        assert orphans == 0, f"{orphans} products have invalid category_id FK."


# == Customers Tests ============================================


class TestCustomers:
    """Verify that the customers dimension is populated."""

    def test_customers_table_has_rows(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers;")
            count = cur.fetchone()[0]
        assert count > 0, "customers table is empty."

    def test_no_null_customer_ids(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers WHERE customer_id IS NULL;")
            null_count = cur.fetchone()[0]
        assert null_count == 0, "Found customers with NULL customer_id."

    def test_customers_have_valid_emails(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM customers WHERE email NOT LIKE '%%@%%';")
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} customers have invalid email addresses."


# == Orders Tests ===============================================


class TestPostgreSQLData:
    """Verify that orders were successfully inserted into PostgreSQL."""

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

    def test_orders_have_valid_customer_fk(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM orders o
                WHERE NOT EXISTS (
                    SELECT 1 FROM customers c WHERE c.customer_id = o.customer_id
                );
            """)
            orphans = cur.fetchone()[0]
        assert orphans == 0, f"{orphans} orders have invalid customer_id FK."

    def test_orders_have_valid_product_fk(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM orders o
                WHERE NOT EXISTS (
                    SELECT 1 FROM products p WHERE p.product_id = o.product_id
                );
            """)
            orphans = cur.fetchone()[0]
        assert orphans == 0, f"{orphans} orders have invalid product_id FK."

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


# == Returned Orders Tests ======================================


class TestReturnedOrders:
    """Verify that returned_orders are populated from returned transactions."""

    def test_returned_orders_has_rows(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM returned_orders;")
            count = cur.fetchone()[0]
        assert count > 0, "returned_orders table is empty — expected some returns."

    def test_returned_orders_have_valid_order_fk(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM returned_orders r
                WHERE NOT EXISTS (
                    SELECT 1 FROM orders o WHERE o.order_id = r.order_id
                );
            """)
            orphans = cur.fetchone()[0]
        assert orphans == 0, f"{orphans} returned_orders have invalid order_id FK."

    def test_returned_orders_refund_positive(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM returned_orders WHERE refund_amount < 0;")
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} returned_orders have negative refund_amount."


# == Purchased Products Tests ===================================


class TestPurchasedProducts:
    """Verify the purchased_products aggregation table."""

    def test_purchased_products_populated(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM purchased_products;")
            count = cur.fetchone()[0]
        assert count > 0, "purchased_products table is empty."

    def test_purchased_products_revenue_positive(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM purchased_products WHERE total_revenue <= 0;")
            bad = cur.fetchone()[0]
        assert bad == 0, f"{bad} purchased_products rows have non-positive revenue."


# == Pipeline Runs Tests ========================================


class TestPipelineRuns:
    """Verify pipeline audit logging."""

    def test_pipeline_runs_logged(self, pg_conn):
        with pg_conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'success';")
            count = cur.fetchone()[0]
        assert count > 0, "No successful pipeline runs logged."


# == Unit Tests (no Docker required) ============================


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
            "product_id":  ["p1", "p2", "p1"],
            "quantity":    [2, 1, 2],
            "unit_price":  [999.99, 49.95, 999.99],
            "discount":    [0.10, 0.0, 0.10],
            "order_date":  ["2024-06-01", "2024-07-15", "2024-06-01"],
            "status":      ["completed", "completed", "completed"],
            "region":      ["north america", "europe", "north america"],
        }
        df = pd.DataFrame(data)
        clean_df, skipped = clean_and_transform(df)

        assert len(clean_df) == 2, "Duplicate should be removed"
        assert "total_revenue" in clean_df.columns
        assert clean_df.loc[clean_df["order_id"] == "ord-001", "total_revenue"].iloc[0] == pytest.approx(
            2 * 999.99 * 0.90, rel=1e-3
        )
        assert clean_df["region"].iloc[0] == "North America"   # title-cased

    def test_missing_columns_raises(self):
        import pandas as pd
        from include.transformations import clean_and_transform

        df = pd.DataFrame({"order_id": ["x"], "product_id": ["y"]})
        with pytest.raises(ValueError, match="missing required columns"):
            clean_and_transform(df)

    def test_clean_customers_basic(self):
        import pandas as pd
        from include.transformations import clean_customers

        data = {
            "customer_id": ["c1", "c2", "c1"],  # duplicate
            "name":        ["Alice", "Bob", "Alice"],
            "email":       ["Alice@Test.com", "bob@test.com", "Alice@Test.com"],
            "region":      ["north america", "europe", "north america"],
            "signup_date": ["2023-01-15", "2023-06-20", "2023-01-15"],
        }
        df = pd.DataFrame(data)
        clean_df, skipped = clean_customers(df)

        assert len(clean_df) == 2, "Duplicate customer should be removed"
        assert clean_df.loc[clean_df["customer_id"] == "c1", "email"].iloc[0] == "alice@test.com"
        assert clean_df["region"].iloc[0] == "North America"

    def test_clean_products_basic(self):
        import pandas as pd
        from include.transformations import clean_products

        data = {
            "product_id": ["p1", "p2"],
            "name":       ["Laptop Pro", "Mouse"],
            "category":   ["electronics", "electronics"],
            "unit_price": [999.99, 29.99],
            "cost":       [500.00, 10.00],
        }
        df = pd.DataFrame(data)
        clean_df, skipped = clean_products(df)

        assert len(clean_df) == 2
        assert clean_df["category"].iloc[0] == "Electronics"

    def test_extract_returns(self):
        import pandas as pd
        from include.transformations import extract_returns

        data = {
            "order_id":      ["ord-001", "ord-002", "ord-003"],
            "customer_id":   ["c1", "c2", "c3"],
            "product_id":    ["p1", "p2", "p3"],
            "quantity":      [1, 2, 1],
            "unit_price":    [100.0, 50.0, 75.0],
            "discount":      [0.0, 0.1, 0.0],
            "order_date":    ["2024-01-01", "2024-02-01", "2024-03-01"],
            "status":        ["completed", "returned", "returned"],
            "region":        ["North America", "Europe", "Asia Pacific"],
            "total_revenue": [100.0, 90.0, 75.0],
        }
        df = pd.DataFrame(data)
        returns_df = extract_returns(df)

        assert len(returns_df) == 2, "Should extract 2 returned orders"
        assert set(returns_df["order_id"]) == {"ord-002", "ord-003"}
