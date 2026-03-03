"""
include/transformations.py
==========================
Pure-Python / Pandas transformation logic for the sales pipeline.
Handles customers, products, transactions, returns, and product aggregations.
No Airflow imports — fully unit-testable in isolation.
"""

from __future__ import annotations

import logging
from typing import Tuple

import pandas as pd

logger = logging.getLogger(__name__)

# === Required column schemas per entity ==========================

REQUIRED_SALES_COLUMNS = {
    "order_id", "customer_id", "product_id",
    "quantity", "unit_price", "discount",
    "order_date", "status", "region",
}

REQUIRED_CUSTOMER_COLUMNS = {
    "customer_id", "name", "email", "region", "signup_date",
}

REQUIRED_PRODUCT_COLUMNS = {
    "product_id", "name", "category", "unit_price", "cost",
}


# === Schema validators ==========================================


def validate_schema(df: pd.DataFrame, required: set[str], entity: str = "CSV") -> None:
    """Raise ValueError if required columns are missing."""
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"{entity} is missing required columns: {missing}")


# === Customer transforms ==========================================


def clean_customers(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Clean and deduplicate customer records.

    Returns:
        (cleaned_df, num_rows_skipped)
    """
    original_len = len(df)
    validate_schema(df, REQUIRED_CUSTOMER_COLUMNS, "Customers CSV")

    # Deduplicate
    df = df.drop_duplicates(subset=["customer_id"])

    # Coerce types
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce")

    # Drop unsalvageable rows
    before_drop = len(df)
    df = df.dropna(subset=["customer_id", "name", "email", "signup_date"])
    rows_dropped = before_drop - len(df)
    if rows_dropped:
        logger.warning("Dropped %d customer rows with null critical fields.", rows_dropped)

    # Normalise text
    df["region"] = df["region"].str.strip().str.title()
    df["name"] = df["name"].str.strip()
    df["email"] = df["email"].str.strip().str.lower()

    # Cast date
    df["signup_date"] = df["signup_date"].dt.date

    # Add lifetime_value placeholder (updated after orders load)
    if "lifetime_value" not in df.columns:
        df["lifetime_value"] = 0.0

    rows_skipped = original_len - len(df)
    logger.info("Customers: %d kept, %d skipped.", len(df), rows_skipped)
    return df, rows_skipped


# === Product transforms ==========================================


def clean_products(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Clean and deduplicate product records.

    Returns:
        (cleaned_df, num_rows_skipped)
    """
    original_len = len(df)
    validate_schema(df, REQUIRED_PRODUCT_COLUMNS, "Products CSV")

    # Deduplicate
    df = df.drop_duplicates(subset=["product_id"])

    # Coerce types
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["cost"] = pd.to_numeric(df["cost"], errors="coerce")

    # Drop unsalvageable
    before_drop = len(df)
    df = df.dropna(subset=["product_id", "name", "category", "unit_price", "cost"])
    rows_dropped = before_drop - len(df)
    if rows_dropped:
        logger.warning("Dropped %d product rows with null critical fields.", rows_dropped)

    # Normalise text
    df["category"] = df["category"].str.strip().str.title()
    df["name"] = df["name"].str.strip()

    rows_skipped = original_len - len(df)
    logger.info("Products: %d kept, %d skipped.", len(df), rows_skipped)
    return df, rows_skipped


# === Sales / transaction transforms ==========================================


def clean_and_transform(df: pd.DataFrame) -> Tuple[pd.DataFrame, int]:
    """
    Apply all cleaning and transformation rules to sales/transaction data.

    Returns:
        (cleaned_df, num_rows_skipped)
    """
    original_len = len(df)

    # Schema check
    validate_schema(df, REQUIRED_SALES_COLUMNS, "Sales CSV")

    # Drop exact duplicate rows
    df = df.drop_duplicates(subset=["order_id"])

    # Coerce types
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")
    df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce")
    df["discount"] = pd.to_numeric(df["discount"], errors="coerce").clip(0, 1)

    # Drop rows that cannot be salvaged
    before_drop = len(df)
    df = df.dropna(subset=["order_id", "order_date", "quantity", "unit_price"])
    rows_dropped = before_drop - len(df)
    if rows_dropped:
        logger.warning("Dropped %d rows with null critical fields.", rows_dropped)

    # Normalise text fields
    df["region"] = df["region"].str.strip().str.title()
    df["status"] = df["status"].str.strip().str.lower()

    # Compute revenue (also stored in PG as a generated column,
    # but persisted here for validation ease)
    df["total_revenue"] = (
        df["quantity"] * df["unit_price"] * (1 - df["discount"])
    ).round(2)

    # Cast order_date to plain date for PG DATE type
    df["order_date"] = df["order_date"].dt.date

    rows_skipped = original_len - len(df)
    logger.info(
        "Transactions: %d rows kept, %d rows skipped.",
        len(df), rows_skipped,
    )
    return df, rows_skipped


# === Extract returns from orders ==========================================


def extract_returns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract returned orders from the clean transactions DataFrame.
    Creates records for the returned_orders table.
    """
    returned = df[df["status"] == "returned"].copy()
    if returned.empty:
        logger.info("No returned orders to extract.")
        return pd.DataFrame(columns=[
            "order_id", "return_reason", "return_date", "refund_amount",
        ])

    import random
    return_reasons = [
        "Defective product", "Wrong item received", "No longer needed",
        "Better price found", "Item not as described", "Arrived damaged",
        "Wrong size", "Changed mind", "Quality not as expected",
    ]

    returned["return_reason"] = [random.choice(return_reasons) for _ in range(len(returned))]
    returned["return_date"] = returned["order_date"]
    returned["refund_amount"] = returned["total_revenue"]

    result = returned[["order_id", "return_reason", "return_date", "refund_amount"]].copy()
    logger.info("Extracted %d returned order records.", len(result))
    return result


# === Product-level aggregation ==========================================


def build_product_aggregations(
    orders_df: pd.DataFrame,
    products_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compute the purchased_products aggregation from cleaned orders + products.
    """
    # Merge to get product name and category
    merged = orders_df.merge(
        products_df[["product_id", "name", "category"]],
        on="product_id",
        how="left",
    )

    agg = (
        merged.groupby(["product_id", "name", "category"])
        .agg(
            total_units_sold=("quantity", "sum"),
            total_revenue=("total_revenue", "sum"),
            avg_discount=("discount", "mean"),
            last_purchased_date=("order_date", "max"),
        )
        .reset_index()
    )
    agg.rename(columns={"name": "product_name", "category": "category_name"}, inplace=True)
    agg["total_revenue"] = agg["total_revenue"].round(2)
    agg["avg_discount"] = agg["avg_discount"].round(4)
    return agg


# === Extract distinct categories ==========================================


def extract_categories(products_df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract unique category names from cleaned products DataFrame.
    Returns DataFrame with 'name' and 'description' columns.
    """
    categories = products_df[["category"]].drop_duplicates().rename(columns={"category": "name"})
    categories["description"] = categories["name"].apply(
        lambda n: f"Products in the {n} category"
    )
    logger.info("Extracted %d unique product categories.", len(categories))
    return categories
