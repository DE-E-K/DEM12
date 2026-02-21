"""
data-generator/generate_data.py
────────────────────────────────
Generates synthetic e-commerce sales CSV data and uploads it to MinIO.

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
from datetime import datetime, timedelta

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

# ── Seed for reproducibility ───────────────────────────────────
fake = Faker()
Faker.seed(settings.generator_seed)
random.seed(settings.generator_seed)

# ── Lookup tables ──────────────────────────────────────────────
PRODUCTS = {
    "Electronics":    ["Laptop Pro 15", "Wireless Earbuds", "Smart Watch", "USB-C Hub", "Webcam HD"],
    "Apparel":        ["Running Shoes", "Winter Jacket", "Yoga Pants", "Casual T-Shirt", "Denim Jeans"],
    "Home & Kitchen": ["Coffee Maker", "Air Fryer", "Blender", "Knife Set", "Cast Iron Pan"],
    "Books":          ["Data Engineering 101", "Python Mastery", "System Design", "Clean Code", "DevOps Handbook"],
    "Sports":         ["Yoga Mat", "Resistance Bands", "Dumbbells 10kg", "Jump Rope", "Water Bottle"],
}

REGIONS = ["North America", "Europe", "Asia Pacific", "Latin America", "Middle East & Africa"]
STATUSES = ["completed", "completed", "completed", "pending", "returned", "cancelled"]  # weighted


def random_order_date(start_days_ago: int = 365) -> str:
    start = datetime.now() - timedelta(days=start_days_ago)
    delta = timedelta(days=random.randint(0, start_days_ago))
    return (start + delta).strftime("%Y-%m-%d")


def generate_dataframe(num_rows: int) -> pd.DataFrame:
    """Create a DataFrame of synthetic sales orders."""
    rows = []
    for _ in range(num_rows):
        category = random.choice(list(PRODUCTS))
        product  = random.choice(PRODUCTS[category])
        qty      = random.randint(1, 10)
        price    = round(random.uniform(5.0, 1500.0), 2)
        discount = round(random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20, 0.25]), 2)

        rows.append({
            "order_id":    fake.uuid4(),
            "customer_id": fake.uuid4(),
            "product":     product,
            "category":    category,
            "region":      random.choice(REGIONS),
            "quantity":    qty,
            "unit_price":  price,
            "discount":    discount,
            "order_date":  random_order_date(),
            "status":      random.choice(STATUSES),
        })

    df = pd.DataFrame(rows)
    logger.info("Generated %d synthetic sales rows.", len(df))
    return df


def upload_to_minio(df: pd.DataFrame) -> str:
    """Serialise DataFrame to CSV and upload to MinIO raw bucket."""
    timestamp  = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    object_key = f"sales_{timestamp}.csv"

    client = boto3.client(
        "s3",
        endpoint_url=settings.minio_endpoint,
        aws_access_key_id=settings.minio_root_user,
        aws_secret_access_key=settings.minio_root_password,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    client.upload_fileobj(csv_buffer, settings.minio_raw_bucket, object_key)
    logger.info(
        "✅  Uploaded '%s' to MinIO bucket '%s' (%d bytes).",
        object_key,
        settings.minio_raw_bucket,
        csv_buffer.tell(),
    )
    return object_key


def main() -> None:
    logger.info("Starting data generator (rows=%d, seed=%d).",
                settings.generator_num_rows, settings.generator_seed)
    df  = generate_dataframe(settings.generator_num_rows)
    key = upload_to_minio(df)
    logger.info("Done — object key: %s", key)


if __name__ == "__main__":
    main()
