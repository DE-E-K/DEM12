"""
include/transformations.py
──────────────────────────
Pure-Python / Pandas transformation logic for the sales pipeline.
No Airflow imports — fully unit-testable in isolation.
"""

from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "order_id",
    "customer_id",
    "product",
    "category",
    "region",
    "quantity",
    "unit_price",
    "discount",
    "order_date",
    "status",
}


def validate_schema(df: pd.DataFrame) -> None:
    """Raise ValueError if required columns are missing."""
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        raise ValueError(f"CSV is missing required columns: {missing}")


def clean_and_transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Apply all cleaning and transformation rules.

    Returns:
        (cleaned_df, num_rows_skipped)
    """
    original_len = len(df)

    # ── Schema check ──────────────────────────────────────────────
    validate_schema(df)

    # ── Drop exact duplicate rows ──────────────────────────────────
    df = df.drop_duplicates(subset=["order_id"])

    # ── Coerce types ──────────────────────────────────────────────
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").clip(0, 1)

    # ── Drop rows that cannot be salvaged ────────────────────────
    before_drop = len(df)
    df = df.dropna(subset=["order_id", "order_date", "quantity", "unit_price"])
    rows_dropped = before_drop - len(df)
    if rows_dropped:
        logger.warning("Dropped %d rows with null critical fields.", rows_dropped)

    # ── Normalise text fields ─────────────────────────────────────
    df["region"] = df["region"].str.strip().str.title()
    df["category"] = df["category"].str.strip().str.title()
    df["product"] = df["product"].str.strip()
    df["status"] = df["status"].str.strip().str.lower()

    # ── Compute revenue (also stored in PG as a generated column,
    #    but we persist it for validation ease) ────────────────────
    df["total_revenue"] = (
        df["quantity"] * df["unit_price"] * (1 - df["discount"])
    ).round(2)

    # ── Cast order_date to plain date string for PG DATE type ─────
    df["order_date"] = df["order_date"].dt.date

    rows_skipped = original_len - len(df)
    logger.info(
        "Transformation complete: %d rows kept, %d rows skipped.",
        len(df),
        rows_skipped,
    )
    return df, rows_skipped


def build_product_aggregations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute the purchased_products aggregation from a cleaned orders DataFrame.
    """
    agg = (
        df.groupby(["product", "category"])
        .agg(
            total_units_sold=("quantity", "sum"),
            total_revenue=("total_revenue", "sum"),
            avg_discount=("discount", "mean"),
            last_purchased_date=("order_date", "max"),
        )
        .reset_index()
    )
    agg["total_revenue"] = agg["total_revenue"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    return agg
