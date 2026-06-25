"""
Bronze validation Lambda.
Triggered automatically when a new file lands in the S3 bronze bucket.
Reads the file, validates each record against its schema, and routes
invalid records to an SQS Dead Letter Queue (quarantine).

Datasets handled:
  - clickstream  (under clickstream/ prefix)
  - orders       (under orders/ prefix)
Other prefixes (products, customers) are passed through without record-level checks.
"""

import json
import os
import urllib.parse

import boto3

s3 = boto3.client("s3")
sqs = boto3.client("sqs")

DLQ_URL = os.environ["DLQ_URL"]  # injected by Terraform as an env var

# Allowed categorical values
VALID_EVENT_TYPES = {
    "page_view", "product_view", "search",
    "add_to_cart", "remove_from_cart", "checkout", "purchase",
}
VALID_ORDER_STATUSES = {"placed", "shipped", "delivered", "cancelled", "returned"}


def validate_clickstream(record):
    """Return a list of validation errors for a clickstream record (empty = valid)."""
    errors = []
    required = ["event_id", "event_type", "event_timestamp", "user_id", "session_id", "product_id"]
    for field in required:
        if field not in record or record[field] in (None, ""):
            errors.append(f"missing_or_null:{field}")

    if record.get("event_type") not in VALID_EVENT_TYPES:
        errors.append(f"invalid_event_type:{record.get('event_type')}")

    price = record.get("price")
    if price is None or not isinstance(price, (int, float)):
        errors.append("price_missing_or_not_numeric")
    elif price < 0:
        errors.append("price_negative")

    return errors


def validate_order(record):
    """Return a list of validation errors for an order record (empty = valid)."""
    errors = []
    required = ["order_id", "customer_id", "product_id", "status"]
    for field in required:
        if field not in record or record[field] in (None, ""):
            errors.append(f"missing_or_null:{field}")

    qty = record.get("quantity")
    if qty is None or not isinstance(qty, int):
        errors.append("quantity_missing_or_not_int")
    elif qty < 1:
        errors.append("quantity_less_than_one")

    amount = record.get("order_amount")
    if amount is None or not isinstance(amount, (int, float)):
        errors.append("order_amount_missing_or_not_numeric")
    elif amount < 0:
        errors.append("order_amount_negative")

    if record.get("status") not in VALID_ORDER_STATUSES:
        errors.append(f"invalid_status:{record.get('status')}")

    return errors


def pick_validator(s3_key):
    """Choose which validator to apply based on the file's prefix."""
    if s3_key.startswith("clickstream/"):
        return validate_clickstream, "clickstream"
    if s3_key.startswith("orders/"):
        return validate_order, "orders"
    return None, None  # products/customers -> no record-level validation here


def send_to_dlq(record, errors, source_key, dataset):
    """Send one bad record to the SQS quarantine queue with its error reasons."""
    sqs.send_message(
        QueueUrl=DLQ_URL,
        MessageBody=json.dumps({
            "dataset": dataset,
            "source_file": source_key,
            "errors": errors,
            "record": record,
        }),
    )


def lambda_handler(event, context):
    # An S3 trigger can batch multiple file events; loop over them
    for s3_event in event.get("Records", []):
        bucket = s3_event["s3"]["bucket"]["name"]
        key = urllib.parse.unquote_plus(s3_event["s3"]["object"]["key"])

        validator, dataset = pick_validator(key)
        if validator is None:
            print(f"Skipping {key} (no record-level validation for this prefix)")
            continue

        # Read the file from S3
        obj = s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")

        total = valid = invalid = 0
        for line in body.splitlines():
            line = line.strip()
            if not line:
                continue
            total += 1
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                invalid += 1
                send_to_dlq({"raw": line}, ["unparseable_json"], key, dataset)
                continue

            errors = validator(record)
            if errors:
                invalid += 1
                send_to_dlq(record, errors, key, dataset)
            else:
                valid += 1

        print(f"Validated {key} [{dataset}]: total={total} valid={valid} invalid={invalid}")

    return {"statusCode": 200, "body": "validation complete"}