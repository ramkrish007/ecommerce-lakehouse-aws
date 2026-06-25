"""
E-commerce clickstream consumer.
Reads events from Kafka, batches them, and writes them as JSON files
into the S3 bronze bucket, partitioned by date and hour.
"""

import json
import time
from datetime import datetime, timezone
from io import BytesIO

import boto3
from kafka import KafkaConsumer

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
TOPIC = "clickstream-events"
BRONZE_BUCKET = "ecom-lakehouse-dc3oqp-bronze"   # <-- your bronze bucket name
AWS_REGION = "ap-south-1"

BATCH_SIZE = 100          # write a file every 100 events
FLUSH_SECONDS = 30        # ...or every 30 seconds, whichever comes first

s3 = boto3.client("s3", region_name=AWS_REGION)


def write_batch_to_s3(records):
    """Write a list of event dicts to S3 bronze as a single newline-delimited JSON file."""
    if not records:
        return

    now = datetime.now(timezone.utc)
    # Partition path: date and hour -> enables efficient querying later
    key = (
        f"clickstream/ingest_date={now:%Y-%m-%d}/hour={now:%H}/"
        f"events_{now:%Y%m%d_%H%M%S}_{int(time.time()*1000) % 1000:03d}.json"
    )

    # Newline-delimited JSON (one event per line) — the standard for raw event landing
    body = "\n".join(json.dumps(r) for r in records).encode("utf-8")

    s3.put_object(Bucket=BRONZE_BUCKET, Key=key, Body=BytesIO(body))
    print(f"  Wrote {len(records)} events -> s3://{BRONZE_BUCKET}/{key}")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset="earliest",        # start from the beginning if no saved position
        enable_auto_commit=True,
        group_id="bronze-s3-writer",         # consumer group name
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    )

    print(f"Consuming from '{TOPIC}', writing to s3://{BRONZE_BUCKET}/clickstream/")
    print("Press Ctrl+C to stop.\n")

    batch = []
    last_flush = time.time()

    try:
        for message in consumer:
            batch.append(message.value)

            # Flush when batch is full OR enough time has passed
            if len(batch) >= BATCH_SIZE or (time.time() - last_flush) >= FLUSH_SECONDS:
                write_batch_to_s3(batch)
                batch = []
                last_flush = time.time()

    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Write whatever is left before exiting
        write_batch_to_s3(batch)
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    main()