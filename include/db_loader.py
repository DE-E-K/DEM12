"""
include/db_loader.py
────────────────────
Idempotent PostgreSQL bulk loader for the sales platform.
Inserts orders and upserts the purchased_products aggregation.
No Airflow imports — fully unit-testable in isolation.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Generator, Tuple

import psycopg2
import psycopg2.extras
import pandas as pd

from include.config import settings

logger = logging.getLogger(__name__)


@contextmanager
def get_connection() -> Generator[psycopg2.extensions.connection, None, None]:
    """Context-managed psycopg2 connection with auto-rollback on error."""
    conn = psycopg2.connect(settings.postgres_dsn)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def upsert_orders(df: pd.DataFrame, conn: psycopg2.extensions.connection) -> Tuple[int, int]:
    """
    Bulk-upsert rows into the `orders` table.

    Returns:
        (rows_inserted, rows_skipped)
    """
    if df.empty:
        logger.warning("upsert_orders called with empty DataFrame — nothing to do.")
        return 0, 0

    columns = [
        "order_id", "customer_id", "product", "category",
        "region", "quantity", "unit_price", "discount",
        "order_date", "status",
    ]

    records = [tuple(row[col] for col in columns) for _, row in df.iterrows()]

    sql = f"""
        INSERT INTO orders ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (order_id) DO UPDATE SET
            customer_id  = EXCLUDED.customer_id,
            product      = EXCLUDED.product,
            category     = EXCLUDED.category,
            region       = EXCLUDED.region,
            quantity     = EXCLUDED.quantity,
            unit_price   = EXCLUDED.unit_price,
            discount     = EXCLUDED.discount,
            order_date   = EXCLUDED.order_date,
            status       = EXCLUDED.status,
            ingested_at  = NOW()
        ;
    """

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, records, page_size=500)
        rows_affected = cur.rowcount

    rows_inserted = rows_affected
    rows_skipped = len(df) - rows_inserted if rows_inserted >= 0 else 0
    logger.info("Upserted %d order rows into PostgreSQL.", rows_inserted)
    return rows_inserted, rows_skipped


def upsert_purchased_products(
    agg_df: pd.DataFrame,
    conn: psycopg2.extensions.connection,
) -> None:
    """Upsert the purchased_products aggregation table."""
    if agg_df.empty:
        return

    columns = [
        "product", "category", "total_units_sold",
        "total_revenue", "avg_discount", "last_purchased_date",
    ]
    records = [tuple(row[col] for col in columns) for _, row in agg_df.iterrows()]

    sql = f"""
        INSERT INTO purchased_products ({', '.join(columns)}, updated_at)
        VALUES %s
        ON CONFLICT (product) DO UPDATE SET
            category            = EXCLUDED.category,
            total_units_sold    = purchased_products.total_units_sold + EXCLUDED.total_units_sold,
            total_revenue       = purchased_products.total_revenue    + EXCLUDED.total_revenue,
            avg_discount        = EXCLUDED.avg_discount,
            last_purchased_date = GREATEST(purchased_products.last_purchased_date, EXCLUDED.last_purchased_date),
            updated_at          = NOW()
        ;
    """

    records_with_ts = [r + ("NOW()",) for r in records]

    # Use plain executemany for the extra NOW() literal
    with conn.cursor() as cur:
        psycopg2.extras.execute_values(
            cur,
            f"""
                INSERT INTO purchased_products ({', '.join(columns)}, updated_at)
                VALUES %s
                ON CONFLICT (product) DO UPDATE SET
                    category            = EXCLUDED.category,
                    total_units_sold    = purchased_products.total_units_sold + EXCLUDED.total_units_sold,
                    total_revenue       = purchased_products.total_revenue    + EXCLUDED.total_revenue,
                    avg_discount        = EXCLUDED.avg_discount,
                    last_purchased_date = GREATEST(purchased_products.last_purchased_date, EXCLUDED.last_purchased_date),
                    updated_at          = NOW()
            """,
            records,
            template="(%s, %s, %s, %s, %s, %s, NOW())",
            page_size=500,
        )
    logger.info("Upserted %d product aggregation rows.", len(agg_df))


def log_pipeline_run(
    conn: psycopg2.extensions.connection,
    dag_run_id: str,
    file_processed: str,
    rows_inserted: int,
    rows_skipped: int,
    status: str,
) -> int:
    """Insert an audit row into pipeline_runs; return the new run_id."""
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (dag_run_id, file_processed, rows_inserted, rows_skipped, status, finished_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            RETURNING run_id
            """,
            (dag_run_id, file_processed, rows_inserted, rows_skipped, status),
        )
        run_id = cur.fetchone()[0]
    logger.info("Logged pipeline_run #%d — status=%s", run_id, status)
    return run_id
