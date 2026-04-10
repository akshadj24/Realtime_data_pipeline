-- ══════════════════════════════════════════════════════════
-- BigQuery Exploration Queries — Real-Time Pipeline Project
-- Replace `YOUR_PROJECT.pipeline_data` with your values.
-- ══════════════════════════════════════════════════════════

-- ── 1. Pipeline health: events per minute (last hour) ──────
SELECT
    TIMESTAMP_TRUNC(ingested_at, MINUTE) AS minute,
    event_type,
    COUNT(*)                             AS events,
    COUNTIF(NOT is_valid)                AS invalid
FROM `YOUR_PROJECT.pipeline_data.raw_events`
WHERE ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY 1, 2
ORDER BY 1 DESC, 2;


-- ── 2. IoT: devices with anomalous temperature (last 30 min) ─
SELECT
    device_id,
    MIN(temperature_c)  AS min_temp,
    MAX(temperature_c)  AS max_temp,
    AVG(temperature_c)  AS avg_temp,
    COUNT(*)            AS readings
FROM `YOUR_PROJECT.pipeline_data.processed_events`
WHERE event_type = 'iot_reading'
  AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 MINUTE)
GROUP BY 1
HAVING MAX(temperature_c) > 50 OR MIN(temperature_c) < -10
ORDER BY max_temp DESC;


-- ── 3. Logs: error rate by service (rolling 1 hour) ─────────
SELECT
    service,
    COUNT(*)                              AS total_logs,
    COUNTIF(log_level = 'ERROR')          AS errors,
    ROUND(
        COUNTIF(log_level = 'ERROR') / COUNT(*) * 100, 2
    )                                     AS error_rate_pct,
    ROUND(AVG(latency_ms), 1)             AS avg_latency_ms,
    ROUND(APPROX_QUANTILES(latency_ms, 100)[OFFSET(95)], 1) AS p95_latency_ms
FROM `YOUR_PROJECT.pipeline_data.processed_events`
WHERE event_type = 'app_log'
  AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY 1
ORDER BY error_rate_pct DESC;


-- ── 4. Transactions: revenue and decline rate by region ──────
SELECT
    region,
    COUNT(*)                              AS total_txns,
    ROUND(SUM(amount_usd), 2)             AS revenue_usd,
    ROUND(AVG(amount_usd), 2)             AS avg_txn_usd,
    COUNTIF(status = 'declined')          AS declines,
    ROUND(
        COUNTIF(status = 'declined') / COUNT(*) * 100, 2
    )                                     AS decline_rate_pct
FROM `YOUR_PROJECT.pipeline_data.processed_events`
WHERE event_type = 'transaction'
  AND event_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
GROUP BY 1
ORDER BY revenue_usd DESC;


-- ── 5. Anomalies: all CRITICAL events today ──────────────────
SELECT
    timestamp,
    event_type,
    entity_id,
    anomaly_type,
    severity,
    description,
    raw_value,
    threshold
FROM `YOUR_PROJECT.pipeline_data.anomalies`
WHERE DATE(timestamp) = CURRENT_DATE()
  AND severity = 'CRITICAL'
ORDER BY timestamp DESC;


-- ── 6. Anomaly heatmap: count by type and hour ───────────────
SELECT
    EXTRACT(HOUR FROM timestamp)          AS hour_of_day,
    event_type,
    anomaly_type,
    COUNT(*)                              AS count
FROM `YOUR_PROJECT.pipeline_data.anomalies`
WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
GROUP BY 1, 2, 3
ORDER BY 1, 4 DESC;


-- ── 7. Aggregate trend: temperature across all devices ───────
SELECT
    window_start,
    ROUND(AVG(avg_temperature_c), 2) AS fleet_avg_temp,
    ROUND(MIN(min_temperature_c), 2) AS fleet_min_temp,
    ROUND(MAX(max_temperature_c), 2) AS fleet_max_temp,
    SUM(event_count)                 AS total_readings
FROM `YOUR_PROJECT.pipeline_data.hourly_aggregates`
WHERE event_type = 'iot_reading'
  AND window_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 HOUR)
GROUP BY 1
ORDER BY 1;


-- ── 8. Scalability check: ingestion lag ──────────────────────
-- Measures how long between Pub/Sub publish and BQ ingestion
SELECT
    event_type,
    ROUND(AVG(
        TIMESTAMP_DIFF(ingested_at, TIMESTAMP(
            JSON_EXTRACT_SCALAR(raw_json, '$.timestamp')
        ), SECOND)
    ), 2) AS avg_lag_seconds,
    MAX(TIMESTAMP_DIFF(ingested_at, TIMESTAMP(
            JSON_EXTRACT_SCALAR(raw_json, '$.timestamp')
        ), SECOND)
    )     AS max_lag_seconds
FROM `YOUR_PROJECT.pipeline_data.raw_events`
WHERE ingested_at > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
  AND is_valid = TRUE
GROUP BY 1;
