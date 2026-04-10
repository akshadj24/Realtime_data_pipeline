"""
Cloud Function: anomaly_detect
================================
Triggered by Pub/Sub (same topic, separate subscription).
Detects anomalies using statistical rules and writes flagged events
to BigQuery `anomalies` table. Also publishes alerts back to Pub/Sub.

Deploy:
    gcloud functions deploy anomaly_detect \
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
from dataclasses import dataclass, asdict

from google.cloud import bigquery, pubsub_v1

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID     = os.environ["PROJECT_ID"]
DATASET        = os.environ.get("DATASET", "pipeline_data")
TABLE_ANOMALY  = f"{PROJECT_ID}.{DATASET}.anomalies"
ALERT_TOPIC    = os.environ.get("ALERT_TOPIC", "pipeline-alerts")

bq_client      = bigquery.Client(project=PROJECT_ID)
pub_client     = pubsub_v1.PublisherClient()
alert_topic    = pub_client.topic_path(PROJECT_ID, ALERT_TOPIC)


# ──────────────────────────────────────────────
# Thresholds (tune these per domain)
# ──────────────────────────────────────────────

IOT_THRESHOLDS = {
    "temperature_c": (-10, 50),   # (min, max) — outside = anomaly
    "humidity_pct":  (10, 95),
    "pressure_hpa":  (980, 1050),
}

LOG_ANOMALY_LEVELS  = {"ERROR"}
ERROR_RATE_WINDOW   = 60   # seconds — used for rate-based logic

TXN_AMOUNT_MAX      = 2000.0   # single-transaction threshold (USD)
TXN_FRAUD_MERCHANTS = {"Unknown"}


# ──────────────────────────────────────────────
# Anomaly model
# ──────────────────────────────────────────────

@dataclass
class Anomaly:
    event_type:   str
    entity_id:    str          # device_id / service / transaction_id
    timestamp:    str
    anomaly_type: str          # threshold | error_log | high_value_txn | etc.
    severity:     str          # LOW | MEDIUM | HIGH | CRITICAL
    description:  str
    raw_value:    float | None
    threshold:    float | None
    region:       str


def severity_from_delta(actual: float, low: float, high: float) -> str:
    mid      = (low + high) / 2
    span     = (high - low) / 2
    distance = abs(actual - mid) - span
    ratio    = distance / span if span else 0
    if ratio > 1.5:   return "CRITICAL"
    if ratio > 0.75:  return "HIGH"
    if ratio > 0.25:  return "MEDIUM"
    return "LOW"


# ──────────────────────────────────────────────
# Detectors
# ──────────────────────────────────────────────

def detect_iot_anomalies(payload: dict) -> list[Anomaly]:
    anomalies = []
    p         = payload.get("payload", {})
    ts        = payload.get("timestamp", datetime.now(timezone.utc).isoformat())
    device    = payload.get("device_id", "unknown")
    region    = payload.get("region", "")

    for field, (lo, hi) in IOT_THRESHOLDS.items():
        val = p.get(field)
        if val is None:
            continue
        if not (lo <= val <= hi):
            severity = severity_from_delta(val, lo, hi)
            anomalies.append(Anomaly(
                event_type   = "iot_reading",
                entity_id    = device,
                timestamp    = ts,
                anomaly_type = f"threshold:{field}",
                severity     = severity,
                description  = (f"{field}={val} outside [{lo},{hi}] "
                                f"on {device}"),
                raw_value    = val,
                threshold    = hi if val > hi else lo,
                region       = region,
            ))
    return anomalies


def detect_log_anomalies(payload: dict) -> list[Anomaly]:
    p       = payload.get("payload", {})
    ts      = payload.get("timestamp", datetime.now(timezone.utc).isoformat())
    service = payload.get("service", "unknown")
    region  = payload.get("region", "")

    if p.get("level") not in LOG_ANOMALY_LEVELS:
        return []

    return [Anomaly(
        event_type   = "app_log",
        entity_id    = service,
        timestamp    = ts,
        anomaly_type = "error_log",
        severity     = "HIGH",
        description  = f"ERROR in {service}: {p.get('message','')[:120]}",
        raw_value    = None,
        threshold    = None,
        region       = region,
    )]


def detect_transaction_anomalies(payload: dict) -> list[Anomaly]:
    anomalies = []
    p         = payload.get("payload", {})
    ts        = payload.get("timestamp", datetime.now(timezone.utc).isoformat())
    txn_id    = payload.get("transaction_id", "unknown")
    region    = payload.get("region", "")
    amount    = p.get("amount_usd", 0)

    if amount > TXN_AMOUNT_MAX:
        severity = "CRITICAL" if amount > 10000 else "HIGH"
        anomalies.append(Anomaly(
            event_type   = "transaction",
            entity_id    = txn_id,
            timestamp    = ts,
            anomaly_type = "high_value_transaction",
            severity     = severity,
            description  = (f"Transaction ${amount:.2f} exceeds "
                            f"threshold ${TXN_AMOUNT_MAX}"),
            raw_value    = amount,
            threshold    = TXN_AMOUNT_MAX,
            region       = region,
        ))

    if p.get("merchant") in TXN_FRAUD_MERCHANTS:
        anomalies.append(Anomaly(
            event_type   = "transaction",
            entity_id    = txn_id,
            timestamp    = ts,
            anomaly_type = "suspicious_merchant",
            severity     = "MEDIUM",
            description  = f"Transaction with flagged merchant: {p.get('merchant')}",
            raw_value    = amount,
            threshold    = None,
            region       = region,
        ))

    return anomalies


DETECTORS = {
    "iot_reading": detect_iot_anomalies,
    "app_log":     detect_log_anomalies,
    "transaction": detect_transaction_anomalies,
}


# ──────────────────────────────────────────────
# Storage & alerting
# ──────────────────────────────────────────────

def store_anomalies(anomalies: list[Anomaly]) -> None:
    if not anomalies:
        return
    rows   = [asdict(a) for a in anomalies]
    errors = bq_client.insert_rows_json(TABLE_ANOMALY, rows)
    if errors:
        logger.error("BigQuery anomaly insert errors: %s", errors)
    else:
        logger.info("Stored %d anomalies", len(anomalies))


def publish_alert(anomaly: Anomaly) -> None:
    """Publish HIGH/CRITICAL anomalies back to Pub/Sub for downstream consumers."""
    if anomaly.severity not in {"HIGH", "CRITICAL"}:
        return
    try:
        msg   = json.dumps(asdict(anomaly)).encode()
        attrs = {
            "severity":     anomaly.severity,
            "anomaly_type": anomaly.anomaly_type,
            "event_type":   anomaly.event_type,
        }
        pub_client.publish(alert_topic, msg, **attrs).result(timeout=5)
        logger.info("Alert published: %s %s", anomaly.severity, anomaly.anomaly_type)
    except Exception as exc:
        logger.error("Failed to publish alert: %s", exc)


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main(event: dict, context) -> None:
    try:
        payload = json.loads(base64.b64decode(event["data"]))
    except Exception as exc:
        logger.error("Decode error: %s", exc)
        return

    event_type = payload.get("event_type")
    detector   = DETECTORS.get(event_type)
    if not detector:
        return   # unknown type — ack silently

    anomalies = detector(payload)

    if anomalies:
        logger.info("Detected %d anomalies for %s", len(anomalies), event_type)
        store_anomalies(anomalies)
        for a in anomalies:
            publish_alert(a)
    else:
        logger.debug("No anomalies in %s event", event_type)
