"""
Cloud Function: aggregate_and_route
=====================================
Triggered by Cloud Scheduler (every 1 minute via Pub/Sub).
Reads the last N minutes from `processed_events`, computes
windowed aggregates, and upserts into `hourly_aggregates`.

This simulates the windowed aggregation you'd get from Dataflow,
implemented via scheduled Cloud Functions for simplicity.

Deploy:
    gcloud functions deploy aggregate_and_route \
        --runtime python311 \
        --trigger-topic pipeline-scheduler \
        --entry-point main \
        --set-env-vars PROJECT_ID=YOUR_PROJECT,DATASET=pipeline_data \
        --memory 512MB \
        --timeout 300s \
        --region us-central1

    # Cloud Scheduler job (every minute):
    gcloud scheduler jobs create pubsub pipeline-aggregator \
        --schedule="* * * * *" \
        --topic=pipeline-scheduler \
        --message-body='{"trigger":"aggregate"}' \
        --location=us-central1
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone, timedelta

from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID  = os.environ["PROJECT_ID"]
DATASET     = os.environ.get("DATASET", "pipeline_data")
TABLE_PROC  = f"`{PROJECT_ID}.{DATASET}.processed_events`"
TABLE_AGG   = f"{PROJECT_ID}.{DATASET}.hourly_aggregates"
WINDOW_MINS = int(os.environ.get("WINDOW_MINS", "2"))   # look-back window

bq = bigquery.Client(project=PROJECT_ID)


# ──────────────────────────────────────────────
# Aggregation queries
# ──────────────────────────────────────────────

def iot_aggregate_query(since: str) -> str:
    return f"""
    SELECT
        'iot_reading'                        AS event_type,
        DATE_TRUNC(TIMESTAMP(event_time), MINUTE) AS window_start,
        region,
        device_id                            AS entity_id,
        COUNT(*)                             AS event_count,
        ROUND(AVG(temperature_c), 2)         AS avg_temperature_c,
        ROUND(MIN(temperature_c), 2)         AS min_temperature_c,
        ROUND(MAX(temperature_c), 2)         AS max_temperature_c,
        ROUND(AVG(humidity_pct), 2)          AS avg_humidity_pct,
        ROUND(AVG(pressure_hpa), 2)          AS avg_pressure_hpa,
        NULL                                 AS error_count,
        NULL                                 AS avg_latency_ms,
        NULL                                 AS total_amount_usd,
        NULL                                 AS txn_count,
        CURRENT_TIMESTAMP()                  AS computed_at
    FROM {TABLE_PROC}
    WHERE event_type = 'iot_reading'
      AND TIMESTAMP(event_time) >= TIMESTAMP('{since}')
    GROUP BY 1,2,3,4
    """


def log_aggregate_query(since: str) -> str:
    return f"""
    SELECT
        'app_log'                            AS event_type,
        DATE_TRUNC(TIMESTAMP(event_time), MINUTE) AS window_start,
        region,
        service                              AS entity_id,
        COUNT(*)                             AS event_count,
        NULL                                 AS avg_temperature_c,
        NULL                                 AS min_temperature_c,
        NULL                                 AS max_temperature_c,
        NULL                                 AS avg_humidity_pct,
        NULL                                 AS avg_pressure_hpa,
        COUNTIF(log_level = 'ERROR')         AS error_count,
        ROUND(AVG(IF(latency_ms IS NOT NULL,
                     CAST(latency_ms AS FLOAT64), NULL)), 2) AS avg_latency_ms,
        NULL                                 AS total_amount_usd,
        NULL                                 AS txn_count,
        CURRENT_TIMESTAMP()                  AS computed_at
    FROM {TABLE_PROC}
    WHERE event_type = 'app_log'
      AND TIMESTAMP(event_time) >= TIMESTAMP('{since}')
    GROUP BY 1,2,3,4
    """


def transaction_aggregate_query(since: str) -> str:
    return f"""
    SELECT
        'transaction'                        AS event_type,
        DATE_TRUNC(TIMESTAMP(event_time), MINUTE) AS window_start,
        region,
        payment_method                       AS entity_id,
        COUNT(*)                             AS event_count,
        NULL                                 AS avg_temperature_c,
        NULL                                 AS min_temperature_c,
        NULL                                 AS max_temperature_c,
        NULL                                 AS avg_humidity_pct,
        NULL                                 AS avg_pressure_hpa,
        COUNTIF(status = 'declined')         AS error_count,
        NULL                                 AS avg_latency_ms,
        ROUND(SUM(amount_usd), 2)            AS total_amount_usd,
        COUNT(*)                             AS txn_count,
        CURRENT_TIMESTAMP()                  AS computed_at
    FROM {TABLE_PROC}
    WHERE event_type = 'transaction'
      AND TIMESTAMP(event_time) >= TIMESTAMP('{since}')
    GROUP BY 1,2,3,4
    """


# ──────────────────────────────────────────────
# Runner
# ──────────────────────────────────────────────

def run_aggregate(query: str, label: str) -> int:
    """Execute query and stream results into the aggregates table."""
    job_config = bigquery.QueryJobConfig(
        destination                = TABLE_AGG,
        write_disposition          = bigquery.WriteDisposition.WRITE_APPEND,
        create_disposition         = bigquery.CreateDisposition.CREATE_IF_NEEDED,
        allow_large_results        = True,
        use_legacy_sql             = False,
    )
    job    = bq.query(query, job_config=job_config)
    result = job.result()   # blocks until done
    rows   = result.total_rows or 0
    logger.info("[%s] aggregated %d rows", label, rows)
    return rows


def dedupe_aggregates(since: str) -> None:
    """
    Remove duplicate windows that may result from function retries.
    Keeps the most-recently computed row per (event_type, window_start, entity_id).
    """
    dedup_sql = f"""
    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.hourly_aggregates` AS
    SELECT * EXCEPT(row_num)
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY event_type, window_start, entity_id
                ORDER BY computed_at DESC
            ) AS row_num
        FROM `{PROJECT_ID}.{DATASET}.hourly_aggregates`
        WHERE window_start >= TIMESTAMP('{since}')
    )
    WHERE row_num = 1
    """
    bq.query(dedup_sql).result()
    logger.info("Deduplication complete")


# ──────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────

def main(event: dict, context) -> None:
    try:
        msg = json.loads(base64.b64decode(event["data"]))
        logger.info("Scheduler triggered: %s", msg)
    except Exception:
        logger.info("Scheduler triggered (no body)")

    since = (
        datetime.now(timezone.utc) - timedelta(minutes=WINDOW_MINS)
    ).strftime("%Y-%m-%dT%H:%M:%S")

    logger.info("Aggregating events since %s", since)

    total = 0
    total += run_aggregate(iot_aggregate_query(since),         "iot")
    total += run_aggregate(log_aggregate_query(since),         "log")
    total += run_aggregate(transaction_aggregate_query(since), "transactions")

    if total > 0:
        dedupe_aggregates(since)

    logger.info("Aggregation run complete — %d rows written", total)
