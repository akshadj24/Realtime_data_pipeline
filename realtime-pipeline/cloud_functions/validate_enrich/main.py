"""
Cloud Function: validate_and_enrich
====================================
Triggered by Pub/Sub. Validates incoming events, enriches with metadata,
and inserts clean rows into BigQuery `raw_events` table.

Deploy:
    gcloud functions deploy validate_and_enrich \
        --runtime python311 \
        --trigger-topic raw-events \
        --entry-point main \
        --set-env-vars PROJECT_ID=YOUR_PROJECT,DATASET=pipeline_data \
        --memory 256MB \
        --region us-central1
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ── Config from environment ──────────────────
PROJECT_ID   = os.environ["PROJECT_ID"]
DATASET      = os.environ.get("DATASET", "pipeline_data")
TABLE_RAW    = f"{PROJECT_ID}.{DATASET}.raw_events"
TABLE_PROC   = f"{PROJECT_ID}.{DATASET}.processed_events"

bq_client    = bigquery.Client(project=PROJECT_ID)

# ── Allowed event types ──────────────────────
VALID_TYPES  = {"iot_reading", "app_log", "transaction"}


# ──────────────────────────────────────────────
# Schema validators
# ──────────────────────────────────────────────

def validate_iot(payload: dict) -> list[str]:
    errors = []
    p = payload.get("payload", {})
    if not (-50 <= p.get("temperature_c", 0) <= 80):
        errors.append("temperature_c out of range")
    if not (0 <= p.get("humidity_pct", -1) <= 100):
        errors.append("humidity_pct out of range")
    return errors


def validate_log(payload: dict) -> list[str]:
    errors = []
    p = payload.get("payload", {})
    if p.get("level") not in {"INFO", "WARN", "ERROR", "DEBUG"}:
        errors.append("invalid log level")
    if not p.get("message"):
        errors.append("missing message")
    return errors


def validate_transaction(payload: dict) -> list[str]:
    errors = []
    p = payload.get("payload", {})
    if p.get("amount_usd", -1) < 0:
        errors.append("negative amount")
    if not p.get("user_id"):
        errors.append("missing user_id")
    return errors


VALIDATORS = {
    "iot_reading": validate_iot,
    "app_log":     validate_log,
    "transaction": validate_transaction,
}


# ──────────────────────────────────────────────
# Enrichment
# ──────────────────────────────────────────────

def enrich(event: dict, raw_message: dict) -> dict:
    """Add pipeline metadata to every event."""
    return {
        **event,
        "_ingested_at":  datetime.now(timezone.utc).isoformat(),
        "_pipeline_v":   "1.0",
        "_message_id":   raw_message.get("messageId", ""),
        "_publish_time": raw_message.get("publishTime", ""),
        "_source":       raw_message.get("attributes", {}).get("source", "unknown"),
    }


# ──────────────────────────────────────────────
# BigQuery insert
# ──────────────────────────────────────────────

def insert_rows(table: str, rows: list[dict]) -> None:
    errors = bq_client.insert_rows_json(table, rows)
    if errors:
        logger.error("BigQuery insert errors: %s", errors)
        raise RuntimeError(f"BQ insert failed: {errors}")
    logger.info("Inserted %d row(s) → %s", len(rows), table)


# ──────────────────────────────────────────────
# Cloud Function entry point
# ──────────────────────────────────────────────

def main(event: dict, context) -> None:
    """
    Pub/Sub push subscription entry point.
    `event["data"]` is base64-encoded JSON.
    """
    # 1. Decode message
    try:
        raw_bytes = base64.b64decode(event["data"])
        payload   = json.loads(raw_bytes)
    except Exception as exc:
        logger.error("Failed to decode message: %s", exc)
        return   # ack bad message — don't retry

    # 2. Basic structure check
    event_type = payload.get("event_type")
    if event_type not in VALID_TYPES:
        logger.warning("Unknown event_type=%s — dropping", event_type)
        return

    if not payload.get("timestamp"):
        logger.warning("Missing timestamp — dropping")
        return

    # 3. Type-specific validation
    validation_errors = VALIDATORS[event_type](payload)
    is_valid = len(validation_errors) == 0

    # 4. Enrich
    enriched = enrich(payload, event)
    enriched["_valid"]             = is_valid
    enriched["_validation_errors"] = validation_errors

    # 5. Insert into raw table (every message — for audit)
    raw_row = {
        "event_id":    enriched.get("transaction_id")
                       or enriched.get("device_id")
                       or enriched.get("service", ""),
        "event_type":  event_type,
        "raw_json":    json.dumps(payload),
        "ingested_at": enriched["_ingested_at"],
        "is_valid":    is_valid,
        "errors":      json.dumps(validation_errors),
        "source":      enriched["_source"],
        "message_id":  enriched["_message_id"],
    }
    insert_rows(TABLE_RAW, [raw_row])

    # 6. Insert into processed table only if valid
    if is_valid:
        processed_row = build_processed_row(event_type, enriched)
        insert_rows(TABLE_PROC, [processed_row])
    else:
        logger.warning("Invalid event dropped from processed table: %s",
                       validation_errors)


def build_processed_row(event_type: str, enriched: dict) -> dict:
    """Flatten nested payload into a single wide row for analytics."""
    p  = enriched.get("payload", {})
    ts = enriched.get("timestamp", enriched["_ingested_at"])

    base = {
        "event_type":    event_type,
        "event_time":    ts,
        "ingested_at":   enriched["_ingested_at"],
        "pipeline_ver":  enriched["_pipeline_v"],
        "region":        enriched.get("region", p.get("region", "")),
    }

    if event_type == "iot_reading":
        base.update({
            "device_id":      enriched.get("device_id"),
            "temperature_c":  p.get("temperature_c"),
            "humidity_pct":   p.get("humidity_pct"),
            "pressure_hpa":   p.get("pressure_hpa"),
            "battery_pct":    p.get("battery_pct"),
        })
    elif event_type == "app_log":
        base.update({
            "service":      enriched.get("service"),
            "log_level":    p.get("level"),
            "message":      p.get("message"),
            "request_id":   p.get("request_id"),
            "user_id":      p.get("user_id"),
            "latency_ms":   p.get("latency_ms"),
        })
    elif event_type == "transaction":
        base.update({
            "transaction_id": enriched.get("transaction_id"),
            "user_id":        p.get("user_id"),
            "amount_usd":     p.get("amount_usd"),
            "currency":       p.get("currency"),
            "merchant":       p.get("merchant"),
            "payment_method": p.get("payment_method"),
            "status":         p.get("status"),
        })

    return base
