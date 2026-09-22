"""
generate_sales_orders_data.py

Purpose
-------
Generates synthetic "sales_orders" records that match schema/sales_orders_schema.json
and writes them as CSV files into a landing folder, one file per run.

This simulates a real source system dropping new files periodically (e.g. every
hour / every day). Each run creates ONE new file with a unique, timestamped name,
so that the Databricks Bronze notebook can pick up only the NEW file(s) on each
run -- that is exactly what "incremental load" means for a raw/landing folder.

Usage
-----
    python generate_sales_orders_data.py --rows 500 --output-dir ./landing/sales_orders

Run it multiple times (e.g. once per "day") to simulate multiple incremental
batches landing over time.
"""

import argparse
import csv
import json
import random
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Reference lists used to generate realistic-looking random values.
# In a real project this data would come from an actual source system --
# here we fabricate it so the pipeline has something to ingest.
# ---------------------------------------------------------------------------
PRODUCT_CATALOG = [
    ("P-1001", "Electronics", 799.99),
    ("P-1002", "Electronics", 129.50),
    ("P-1003", "Apparel", 39.99),
    ("P-1004", "Apparel", 19.99),
    ("P-1005", "Home & Kitchen", 59.00),
    ("P-1006", "Home & Kitchen", 24.75),
    ("P-1007", "Sports", 89.90),
    ("P-1008", "Books", 14.99),
]
ORDER_STATUSES = ["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"]
PAYMENT_METHODS = ["CREDIT_CARD", "PAYPAL", "COD", "DEBIT_CARD"]


def load_schema(schema_path: Path) -> dict:
    """Read the JSON schema so the column ORDER in the generated CSV always
    matches the schema definition. Keeping the generator schema-driven means
    if you add/rename a column in the JSON schema, this script automatically
    follows it (as long as you also add the generation logic for that column)."""
    with open(schema_path, "r") as f:
        return json.load(f)


def generate_row(order_date: datetime) -> dict:
    """Build a single fake sales order record."""
    product_id, category, unit_price = random.choice(PRODUCT_CATALOG)
    quantity = random.randint(1, 5)
    updated_at = order_date + timedelta(minutes=random.randint(0, 500))

    return {
        "order_id": f"ORD-{uuid.uuid4().hex[:10].upper()}",
        "customer_id": f"CUST-{random.randint(1000, 9999)}",
        "product_id": product_id,
        "product_category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": round(unit_price * quantity, 2),
        "order_status": random.choice(ORDER_STATUSES),
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_date": order_date.strftime("%Y-%m-%d"),
        "order_updated_at": updated_at.strftime("%Y-%m-%d %H:%M:%S"),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate a batch of sales_orders data")
    parser.add_argument("--rows", type=int, default=200, help="Number of rows to generate in this batch")
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./landing/sales_orders",
        help="Folder to write the batch CSV file into (this is your 'landing zone')",
    )
    parser.add_argument(
        "--schema-path",
        type=str,
        default="../schema/sales_orders_schema.json",
        help="Path to the JSON schema file, used to keep column order in sync",
    )
    args = parser.parse_args()

    schema = load_schema(Path(args.schema_path))
    columns = [c["name"] for c in schema["columns"]]

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Unique, timestamped filename => every run creates a brand-new file.
    # This is what lets the Bronze notebook detect "new" data incrementally:
    # it only has to look at files it has not processed yet.
    batch_ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"sales_orders_{batch_ts}.csv"

    today = datetime.now(timezone.utc)
    rows = [generate_row(today) for _ in range(args.rows)]

    with open(output_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} rows to {output_file}")


if __name__ == "__main__":
    main()
