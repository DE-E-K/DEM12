"""
data-generator/generate_data.py
================================
Generates synthetic e-commerce CSV data and uploads to MinIO.

Produces three CSV files per run:
  1. customers_<ts>.csv  — 1,000+ customer records with demographics
  2. products_<ts>.csv   — 100+ products with categories and pricing
  3. sales_<ts>.csv      — 10,000+ transaction records

Run inside Docker:
    docker compose --profile tools run --rm data-generator

Or locally (with .env sourced):
    python data-generator/generate_data.py
"""

from __future__ import annotations

import io
import logging
import random
import sys
from datetime import datetime, timedelta, timezone

import boto3
import pandas as pd
from botocore.client import Config
from faker import Faker

from config import settings  # local config.py in data-generator/

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("data-generator")

# == Seed for reproducibility ===================================
fake = Faker()
Faker.seed(settings.generator_seed)
random.seed(settings.generator_seed)

# == Lookup tables ==============================================
CATEGORY_DESCRIPTIONS = {
    "Electronics":    "Consumer electronics and tech gadgets",
    "Apparel":        "Clothing and fashion accessories",
    "Home & Kitchen": "Household and kitchen products",
    "Books":          "Literature, textbooks and reference materials",
    "Sports":         "Sporting goods and fitness equipment",
}

PRODUCTS_BY_CATEGORY = {
    "Electronics":    [
        ("Laptop Pro 15", 999.99, 650.00),
        ("Wireless Earbuds", 79.99, 35.00),
        ("Smart Watch", 249.99, 120.00),
        ("USB-C Hub", 49.99, 22.00),
        ("Webcam HD", 89.99, 40.00),
        ("Bluetooth Speaker", 59.99, 25.00),
        ("Tablet 10 inch", 399.99, 220.00),
        ("Mechanical Keyboard", 129.99, 55.00),
        ("Wireless Mouse", 39.99, 15.00),
        ("Monitor 27 inch", 349.99, 200.00),
        ("External SSD 1TB", 109.99, 60.00),
        ("Noise Cancelling Headphones", 299.99, 140.00),
        ("Portable Charger", 29.99, 12.00),
        ("Smart Home Hub", 149.99, 75.00),
        ("Action Camera", 199.99, 95.00),
        ("Drone Mini", 449.99, 250.00),
        ("E-Reader", 129.99, 70.00),
        ("Gaming Controller", 59.99, 28.00),
        ("VR Headset", 399.99, 210.00),
        ("Smart Doorbell", 179.99, 85.00),
    ],
    "Apparel": [
        ("Running Shoes", 89.99, 35.00),
        ("Winter Jacket", 149.99, 65.00),
        ("Yoga Pants", 49.99, 18.00),
        ("Casual T-Shirt", 24.99, 8.00),
        ("Denim Jeans", 59.99, 22.00),
        ("Hiking Boots", 129.99, 55.00),
        ("Hoodie", 44.99, 16.00),
        ("Dress Shirt", 54.99, 20.00),
        ("Baseball Cap", 19.99, 6.00),
        ("Sunglasses", 79.99, 25.00),
        ("Leather Belt", 34.99, 12.00),
        ("Wool Scarf", 29.99, 10.00),
        ("Rain Coat", 69.99, 30.00),
        ("Sneakers Classic", 74.99, 32.00),
        ("Swim Shorts", 34.99, 14.00),
        ("Polo Shirt", 39.99, 14.00),
        ("Chino Pants", 49.99, 19.00),
        ("Thermal Underwear", 29.99, 10.00),
        ("Athletic Socks 6pk", 14.99, 4.00),
        ("Fleece Vest", 44.99, 18.00),
    ],
    "Home & Kitchen": [
        ("Coffee Maker", 79.99, 35.00),
        ("Air Fryer", 99.99, 45.00),
        ("Blender", 49.99, 20.00),
        ("Knife Set", 89.99, 38.00),
        ("Cast Iron Pan", 44.99, 18.00),
        ("Instant Pot", 89.99, 42.00),
        ("Toaster Oven", 59.99, 28.00),
        ("Dish Set 16pc", 49.99, 20.00),
        ("Food Processor", 69.99, 32.00),
        ("Electric Kettle", 34.99, 14.00),
        ("Vacuum Cleaner", 199.99, 95.00),
        ("Bed Sheet Set", 39.99, 15.00),
        ("Throw Pillows 2pk", 24.99, 8.00),
        ("Bath Towel Set", 29.99, 10.00),
        ("Candle Set", 19.99, 6.00),
        ("Storage Containers", 24.99, 9.00),
        ("Cutting Board Set", 29.99, 11.00),
        ("Wine Glass Set", 34.99, 12.00),
        ("Table Lamp", 44.99, 18.00),
        ("Wall Clock", 29.99, 12.00),
    ],
    "Books": [
        ("Data Engineering 101", 44.99, 15.00),
        ("Python Mastery", 39.99, 12.00),
        ("System Design", 49.99, 18.00),
        ("Clean Code", 34.99, 10.00),
        ("DevOps Handbook", 42.99, 14.00),
        ("Machine Learning Basics", 54.99, 20.00),
        ("SQL Cookbook", 39.99, 13.00),
        ("Cloud Architecture", 49.99, 17.00),
        ("Agile Development", 29.99, 9.00),
        ("Linux Administration", 44.99, 15.00),
        ("Web Development Guide", 34.99, 11.00),
        ("Algorithms Explained", 39.99, 13.00),
        ("Docker in Practice", 42.99, 14.00),
        ("Kubernetes Patterns", 49.99, 18.00),
        ("Data Science Handbook", 54.99, 20.00),
        ("JavaScript Complete", 39.99, 12.00),
        ("Database Internals", 49.99, 17.00),
        ("Network Programming", 44.99, 15.00),
        ("Security Fundamentals", 39.99, 13.00),
        ("AI for Everyone", 29.99, 9.00),
    ],
    "Sports": [
        ("Yoga Mat", 29.99, 10.00),
        ("Resistance Bands", 19.99, 6.00),
        ("Dumbbells 10kg", 39.99, 18.00),
        ("Jump Rope", 14.99, 4.00),
        ("Water Bottle", 12.99, 3.00),
        ("Protein Shaker", 14.99, 5.00),
        ("Exercise Ball", 24.99, 9.00),
        ("Pull-Up Bar", 34.99, 14.00),
        ("Foam Roller", 19.99, 7.00),
        ("Running Armband", 14.99, 4.00),
        ("Cycling Gloves", 19.99, 7.00),
        ("Tennis Racket", 79.99, 35.00),
        ("Basketball", 29.99, 12.00),
        ("Soccer Ball", 24.99, 10.00),
        ("Badminton Set", 34.99, 14.00),
        ("Gym Bag", 29.99, 11.00),
        ("Fitness Tracker Band", 49.99, 22.00),
        ("Knee Brace", 19.99, 7.00),
        ("Swim Goggles", 14.99, 5.00),
        ("Climbing Chalk Bag", 12.99, 4.00),
    ],
}

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East & Africa"]
STATUSES = ["completed", "pending", "returned", "cancelled"]
RETURN_REASONS = [
    "Defective product", "Wrong item received", "No longer needed",
    "Better price found", "Item not as described", "Arrived damaged",
    "Wrong size", "Changed mind", "Quality not as expected",
]


def _random_date(start_days_ago: int = 730) -> str:
    """Random date within the last N days."""
    start = datetime.now(timezone.utc) - timedelta(days=start_days_ago)
    delta = timedelta(days=random.randint(0, start_days_ago))
    return (start + delta).strftime("%Y-%m-%d")


# ── Generators ──────────────────────────────────────────────


def generate_customers(num_customers: int) -> pd.DataFrame:
    """Generate customer dimension records."""
    rows = []
    for _ in range(num_customers):
        rows.append({
            "customer_id":   fake.uuid4(),
            "name":          fake.name(),
            "email":         fake.email(),
            "region":        random.choice(REGIONS),
            "signup_date":   _random_date(start_days_ago=1095),  # up to 3 years ago
        })
    df = pd.DataFrame(rows)
    logger.info("Generated %d customer records.", len(df))
    return df


def generate_products() -> pd.DataFrame:
    """Generate product dimension records (all products from catalog)."""
    rows = []
    for category, products in PRODUCTS_BY_CATEGORY.items():
        for name, price, cost in products:
            rows.append({
                "product_id":  fake.uuid4(),
                "name":        name,
                "category":    category,
                "unit_price":  price,
                "cost":        cost,
            })
    df = pd.DataFrame(rows)
    logger.info("Generated %d product records across %d categories.",
                len(df), len(PRODUCTS_BY_CATEGORY))
    return df


def generate_transactions(
    customer_ids: list[str],
    product_df: pd.DataFrame,
    num_transactions: int,
) -> pd.DataFrame:
    """Generate transaction (order) records referencing existing customers and products."""
    product_records = product_df.to_dict("records")
    rows = []
    for _ in range(num_transactions):
        product = random.choice(product_records)
        qty = random.randint(1, 10)
        discount = round(random.choice([0, 0.05, 0.10, 0.15, 0.20, 0.25]), 2)
        status = random.choice(STATUSES)

        rows.append({
            "order_id":    fake.uuid4(),
            "customer_id": random.choice(customer_ids),
            "product_id":  product["product_id"],
            "quantity":    qty,
            "unit_price":  product["unit_price"],
            "discount":    discount,
            "order_date":  _random_date(start_days_ago=365),
            "status":      status,
            "region":      random.choice(REGIONS),
        })

    df = pd.DataFrame(rows)
    logger.info("Generated %d transaction records.", len(df))
    return df


# ── MinIO upload ────────────────────────────────────────────


def _get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def upload_csv_to_minio(df: pd.DataFrame, prefix: str) -> str:
    """Serialise DataFrame to CSV and upload to MinIO raw bucket."""
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    object_key = f"{prefix}_{timestamp}.csv"

    client = _get_s3_client()
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    csv_size = len(csv_buffer.getvalue())

    client.upload_fileobj(csv_buffer, settings.minio_raw_bucket, object_key)
    logger.info(
        "Uploaded '%s' to MinIO bucket '%s' (%d bytes).",
        object_key, settings.minio_raw_bucket, csv_size,
    )
    return object_key


# ── Legacy compatibility (DAG calls these) ──────────────────


def generate_dataframe(num_rows: int) -> pd.DataFrame:
    """Legacy single-file generator for backward compatibility with DAG."""
    customers_df = generate_customers(min(num_rows // 10, 100))
    products_df = generate_products()
    transactions_df = generate_transactions(
        customers_df["customer_id"].tolist(),
        products_df,
        num_rows,
    )
    return transactions_df


def upload_to_minio(df: pd.DataFrame) -> str:
    """Legacy single-file upload for backward compatibility."""
    return upload_csv_to_minio(df, "sales")


# ── Main entry point ────────────────────────────────────────


def main() -> None:
    logger.info(
        "Starting data generator (customers=%d, products=100, transactions=%d, seed=%d).",
        settings.generator_num_customers,
        settings.generator_num_transactions,
        settings.generator_seed,
    )

    # 1. Generate all data
    customers_df = generate_customers(settings.generator_num_customers)
    products_df = generate_products()
    transactions_df = generate_transactions(
        customers_df["customer_id"].tolist(),
        products_df,
        settings.generator_num_transactions,
    )

    # 2. Upload all three CSVs to MinIO
    keys = []
    keys.append(upload_csv_to_minio(customers_df, "customers"))
    keys.append(upload_csv_to_minio(products_df, "products"))
    keys.append(upload_csv_to_minio(transactions_df, "sales"))

    logger.info("Done — uploaded %d files: %s", len(keys), keys)


if __name__ == "__main__":
    main()
