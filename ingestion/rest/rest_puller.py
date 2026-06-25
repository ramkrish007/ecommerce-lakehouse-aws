"""
Batch ingestion (REST-style puller).
Simulates pulling reference data from a vendor REST API:
  - products   (catalog master)
  - customers  (customer master)
  - orders     (completed transactions)
Lands each as raw JSON into the S3 bronze layer, partitioned by pull date.

Product IDs and categories align with the clickstream producer so the
data joins cleanly in the silver/gold layers.
"""

import json
import random
import uuid
from datetime import datetime, timezone, timedelta
from io import BytesIO

import boto3
from faker import Faker

fake = Faker("en_IN")  # Indian names, cities, etc.

# --- Configuration ---
BRONZE_BUCKET = "ecom-lakehouse-dc3oqp-bronze"   # <-- your bronze bucket
AWS_REGION = "ap-south-1"

NUM_PRODUCTS = 1000
NUM_CUSTOMERS = 5000
NUM_ORDERS = 2000

# Must match the clickstream producer's categories + price ranges
CATEGORY_PRICES = {
    "Electronics": (5000, 50000),
    "Fashion":     (499, 8000),
    "Home":        (999, 20000),
    "Books":       (99, 1499),
    "Beauty":      (199, 5000),
    "Sports":      (499, 15000),
    "Grocery":     (49, 2000),
}
CATEGORIES = list(CATEGORY_PRICES.keys())
CATEGORY_WEIGHTS = [30, 28, 15, 8, 9, 6, 4]

BRANDS = ["Acme", "Zenith", "Nova", "Apex", "Orbit", "Vertex", "Lumen", "Pulse"]
ORDER_STATUSES = ["placed", "shipped", "delivered", "cancelled", "returned"]
STATUS_WEIGHTS = [15, 20, 50, 10, 5]

s3 = boto3.client("s3", region_name=AWS_REGION)


def corrupt_order(order):
    """Deliberately break ~5% of orders to exercise the validation layer."""
    if random.random() < 0.05:
        corruption = random.choice([
            "missing_field",
            "negative_amount",
            "bad_status",
            "zero_quantity",
        ])
        if corruption == "missing_field":
            order.pop("customer_id", None)
        elif corruption == "negative_amount":
            order["order_amount"] = -abs(order["order_amount"])
        elif corruption == "bad_status":
            order["status"] = "GHOST"
        elif corruption == "zero_quantity":
            order["quantity"] = 0
    return order


def generate_products():
    products = []
    for i in range(1, NUM_PRODUCTS + 1):
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        low, high = CATEGORY_PRICES[category]
        products.append({
            "product_id": f"prod_{i}",
            "product_name": f"{random.choice(BRANDS)} {fake.word().capitalize()} {category[:4]}",
            "category": category,
            "brand": random.choice(BRANDS),
            "base_price": round(random.uniform(low, high), 2),
            "in_stock": random.choice([True, True, True, False]),  # ~75% in stock
        })
    return products


def generate_customers():
    customers = []
    for i in range(1, NUM_CUSTOMERS + 1):
        signup = fake.date_between(start_date="-3y", end_date="today")
        customers.append({
            "customer_id": f"user_{i}",          # matches clickstream user_id space
            "name": fake.name(),
            "email": fake.email(),
            "city": fake.city(),
            "state": fake.state(),
            "signup_date": signup.isoformat(),
            "is_prime": random.choice([True, False]),
        })
    return customers


def generate_orders():
    orders = []
    for _ in range(NUM_ORDERS):
        category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
        low, high = CATEGORY_PRICES[category]
        qty = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 13, 7, 5], k=1)[0]
        unit_price = round(random.uniform(low, high), 2)
        order_ts = datetime.now(timezone.utc) - timedelta(days=random.randint(0, 30),
                                                           hours=random.randint(0, 23))
        orders.append(corrupt_order({
            "order_id": str(uuid.uuid4()),
            "customer_id": f"user_{random.randint(1, NUM_CUSTOMERS)}",
            "product_id": f"prod_{random.randint(1, NUM_PRODUCTS)}",
            "category": category,
            "quantity": qty,
            "unit_price": unit_price,
            "order_amount": round(unit_price * qty, 2),
            "status": random.choices(ORDER_STATUSES, weights=STATUS_WEIGHTS, k=1)[0],
            "order_timestamp": order_ts.isoformat(),
        }))
    return orders


def land_to_bronze(dataset_name, records):
    """Write a dataset as newline-delimited JSON into bronze, partitioned by pull date."""
    now = datetime.now(timezone.utc)
    key = (
        f"{dataset_name}/pull_date={now:%Y-%m-%d}/"
        f"{dataset_name}_{now:%Y%m%d_%H%M%S}.json"
    )
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=BytesIO(body))
    print(f"  Landed {len(records):>5} {dataset_name:<10} -> s3://{BRONZE_BUCKET}/{key}")


def main():
    print(f"REST-style batch pull -> s3://{BRONZE_BUCKET}/\n")

    print("Pulling reference data...")
    land_to_bronze("products", generate_products())
    land_to_bronze("customers", generate_customers())
    land_to_bronze("orders", generate_orders())

    print("\nBatch pull complete.")


if __name__ == "__main__":
    main()