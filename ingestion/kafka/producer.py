"""
E-commerce clickstream producer.
Simulates realistic user activity on an online store and publishes events to Kafka.
Built-in realism: time-of-day traffic curve, bestseller skew, category-based pricing.
"""

import json
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

# --- Configuration ---
KAFKA_BROKER = "localhost:9092"
TOPIC = "clickstream-events"

EVENT_TYPES = ["page_view", "product_view", "search", "add_to_cart", "remove_from_cart", "checkout", "purchase"]
EVENT_WEIGHTS = [30, 25, 15, 12, 5, 8, 5]  # browse-heavy, purchase-rare funnel

DEVICES = ["mobile", "desktop", "tablet"]
DEVICE_WEIGHTS = [65, 30, 5]  # mobile-first, like India

CHANNELS = ["organic", "paid_ad", "email", "social", "direct"]
CHANNEL_WEIGHTS = [35, 25, 15, 15, 10]

PAYMENT_METHODS = ["upi", "credit_card", "debit_card", "netbanking", "cod"]
PAYMENT_WEIGHTS = [45, 20, 15, 10, 10]  # UPI-dominant, like India

# Category-based price ranges (in INR) so revenue charts vary meaningfully
CATEGORY_PRICES = {
    "Electronics": (5000, 50000),
    "Fashion":     (499, 8000),
    "Home":        (999, 20000),
    "Books":       (99, 1499),
    "Beauty":      (199, 5000),
    "Sports":      (499, 15000),
    "Grocery":     (49, 2000),
}
# Category popularity skew — Electronics & Fashion dominate
CATEGORIES = list(CATEGORY_PRICES.keys())
CATEGORY_WEIGHTS = [30, 28, 15, 8, 9, 6, 4]

# Bestseller skew: ~50 hot products out of 1000 get picked most of the time (long tail)
BESTSELLERS = [f"prod_{i}" for i in range(1, 51)]


def traffic_multiplier_for_hour(hour):
    """Return a relative traffic weight by hour of day — evenings peak, small-hours dip."""
    # Rough daily shopping curve (0=midnight .. 23)
    curve = {
        0: 0.3, 1: 0.2, 2: 0.15, 3: 0.1, 4: 0.1, 5: 0.15,
        6: 0.3, 7: 0.5, 8: 0.7, 9: 0.9, 10: 1.0, 11: 1.0,
        12: 1.1, 13: 1.0, 14: 0.9, 15: 0.9, 16: 1.0, 17: 1.1,
        18: 1.3, 19: 1.5, 20: 1.7, 21: 1.6, 22: 1.2, 23: 0.7,
    }
    return curve.get(hour, 1.0)


def pick_product():
    """80/20 long tail: 70% chance a bestseller, 30% a random product."""
    if random.random() < 0.70:
        return random.choice(BESTSELLERS)
    return f"prod_{random.randint(1, 1000)}"


def generate_event():
    event_type = random.choices(EVENT_TYPES, weights=EVENT_WEIGHTS, k=1)[0]
    category = random.choices(CATEGORIES, weights=CATEGORY_WEIGHTS, k=1)[0]
    low, high = CATEGORY_PRICES[category]

    event = {
        "event_id": str(uuid.uuid4()),
        "event_type": event_type,
        "event_timestamp": datetime.now(timezone.utc).isoformat(),
        "user_id": f"user_{random.randint(1, 5000)}",
        "session_id": f"session_{random.randint(1, 20000)}",
        "device": random.choices(DEVICES, weights=DEVICE_WEIGHTS, k=1)[0],
        "channel": random.choices(CHANNELS, weights=CHANNEL_WEIGHTS, k=1)[0],
        "product_id": pick_product(),
        "category": category,
        "price": round(random.uniform(low, high), 2),
    }

    if event_type == "purchase":
        event["payment_method"] = random.choices(PAYMENT_METHODS, weights=PAYMENT_WEIGHTS, k=1)[0]
        event["quantity"] = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 13, 7, 5], k=1)[0]

    return event


def main():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )

    print(f"Producing events to topic '{TOPIC}'... Press Ctrl+C to stop.")
    count = 0
    try:
        while True:
            event = generate_event()

            # Apply time-of-day shaping: occasionally skip an event in low-traffic hours
            hour = datetime.now(timezone.utc).hour
            if random.random() > traffic_multiplier_for_hour(hour) / 1.7:
                # Skip some events when the hour's multiplier is low -> creates natural peaks/valleys
                pass

            producer.send(TOPIC, key=event["session_id"], value=event)
            count += 1

            if count % 50 == 0:
                print(f"  Sent {count} events...")

            time.sleep(0.1)
    except KeyboardInterrupt:
        print(f"\nStopping. Total events sent: {count}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()