"""
Real-Time Data Pipeline — Event Simulator
==========================================
Publishes simulated IoT sensor, application log, and transaction events
to a Google Cloud Pub/Sub topic. Run this locally or from Cloud Shell.

Usage:
    python simulator.py --project YOUR_PROJECT_ID --topic raw-events \
        --mode mixed --rate 5 --duration 300
"""

import argparse
import json
import random
import time
import math
from datetime import datetime, timezone
from google.cloud import pubsub_v1

# ──────────────────────────────────────────────
# Event generators
# ──────────────────────────────────────────────

DEVICE_IDS  = [f"sensor-{i:03d}" for i in range(1, 21)]
SERVICES    = ["auth-service", "api-gateway", "order-service", "payment-service"]
LOG_LEVELS  = ["INFO", "INFO", "INFO", "WARN", "ERROR"]   # weighted
REGIONS     = ["us-east1", "us-west1", "europe-west1", "asia-east1"]


def iot_event() -> dict:
    """Simulates a sensor reading. Injects anomalies ~5% of the time."""
    device = random.choice(DEVICE_IDS)
    ts     = datetime.now(timezone.utc).isoformat()

    # Baseline values with mild drift over time
    t = time.time()
    temp     = 22 + 5 * math.sin(t / 300) + random.gauss(0, 0.5)
    humidity = 55 + 10 * math.cos(t / 600) + random.gauss(0, 1)
    pressure = 1013 + random.gauss(0, 2)

    # Inject anomaly ~5% of the time
    if random.random() < 0.05:
        temp += random.choice([-20, 30])   # spike or drop

    return {
        "event_type": "iot_reading",
        "device_id":  device,
        "timestamp":  ts,
        "region":     random.choice(REGIONS),
        "payload": {
            "temperature_c": round(temp, 2),
            "humidity_pct":  round(max(0, min(100, humidity)), 2),
            "pressure_hpa":  round(pressure, 2),
            "battery_pct":   random.randint(10, 100),
        },
    }


def log_event() -> dict:
    """Simulates an application log entry."""
    level   = random.choice(LOG_LEVELS)
    service = random.choice(SERVICES)
    ts      = datetime.now(timezone.utc).isoformat()

    messages = {
        "INFO":  [f"Request processed in {random.randint(10,300)}ms",
                  "Cache hit", "User authenticated"],
        "WARN":  [f"Slow query: {random.randint(500,2000)}ms",
                  "Rate limit approaching", "Retry attempt"],
        "ERROR": ["Database connection timeout", "NullPointerException",
                  "Upstream service unavailable"],
    }

    return {
        "event_type": "app_log",
        "service":    service,
        "timestamp":  ts,
        "payload": {
            "level":      level,
            "message":    random.choice(messages[level]),
            "request_id": f"req-{random.randint(100000,999999)}",
            "user_id":    f"user-{random.randint(1,5000)}",
            "latency_ms": random.randint(5, 3000) if level != "ERROR" else None,
        },
    }


def transaction_event() -> dict:
    """Simulates an e-commerce transaction."""
    ts     = datetime.now(timezone.utc).isoformat()
    amount = round(random.expovariate(1/80), 2)  # skewed toward small amounts

    # Inject fraud-like anomaly ~3% of the time
    if random.random() < 0.03:
        amount = round(random.uniform(5000, 20000), 2)

    return {
        "event_type": "transaction",
        "transaction_id": f"txn-{random.randint(1000000,9999999)}",
        "timestamp": ts,
        "payload": {
            "user_id":        f"user-{random.randint(1,5000)}",
            "amount_usd":     amount,
            "currency":       random.choice(["USD", "EUR", "GBP", "INR"]),
            "merchant":       random.choice(["Amazon", "Uber", "Netflix",
                                             "Shopify", "Unknown"]),
            "payment_method": random.choice(["card", "upi", "wallet"]),
            "status":         random.choice(["approved", "approved", "declined"]),
            "region":         random.choice(REGIONS),
        },
    }


GENERATORS = {
    "iot":         iot_event,
    "log":         log_event,
    "transaction": transaction_event,
}


# ──────────────────────────────────────────────
# Publisher
# ──────────────────────────────────────────────

def publish_events(project_id: str, topic_id: str,
                   mode: str, rate: float, duration: int):
    """
    Publishes events to Pub/Sub at `rate` events/second for `duration` seconds.

    Args:
        project_id:  GCP project ID
        topic_id:    Pub/Sub topic name (e.g. "raw-events")
        mode:        "iot" | "log" | "transaction" | "mixed"
        rate:        events per second
        duration:    total seconds to run (0 = run forever)
    """
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    # Validate topic exists
    try:
        publisher.get_topic(request={"topic": topic_path})
    except Exception:
        print(f"[WARN] Topic '{topic_path}' not found — creating it.")
        publisher.create_topic(request={"name": topic_path})

    generators = (
        list(GENERATORS.values()) if mode == "mixed"
        else [GENERATORS[mode]]
    )

    interval   = 1.0 / rate
    start      = time.time()
    sent       = 0
    errors     = 0

    print(f"[START] Publishing {mode!r} events → {topic_path}")
    print(f"        rate={rate}/s  duration={duration or '∞'}s")

    try:
        while True:
            if duration and (time.time() - start) >= duration:
                break

            event      = random.choice(generators)()
            payload    = json.dumps(event).encode("utf-8")
            attributes = {
                "event_type": event["event_type"],
                "source":     "simulator-v1",
            }

            future = publisher.publish(topic_path, payload, **attributes)

            try:
                msg_id = future.result(timeout=5)
                sent += 1
                if sent % 50 == 0:
                    elapsed = time.time() - start
                    print(f"  [{elapsed:6.1f}s]  sent={sent}  errors={errors}  "
                          f"last={event['event_type']}")
            except Exception as e:
                errors += 1
                print(f"  [ERROR] {e}")

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[STOP] Interrupted by user.")

    elapsed = time.time() - start
    print(f"\n[DONE] sent={sent}  errors={errors}  elapsed={elapsed:.1f}s")


# ──────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pub/Sub Event Simulator")
    parser.add_argument("--project",  required=True, help="GCP Project ID")
    parser.add_argument("--topic",    default="raw-events", help="Pub/Sub topic name")
    parser.add_argument("--mode",     default="mixed",
                        choices=["iot", "log", "transaction", "mixed"])
    parser.add_argument("--rate",     type=float, default=5.0,
                        help="Events per second")
    parser.add_argument("--duration", type=int,   default=0,
                        help="Run for N seconds (0 = forever)")
    args = parser.parse_args()

    publish_events(
        project_id=args.project,
        topic_id=args.topic,
        mode=args.mode,
        rate=args.rate,
        duration=args.duration,
    )
