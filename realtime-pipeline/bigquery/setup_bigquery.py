"""
BigQuery Schema Setup
======================
Creates the dataset and all four tables required by the pipeline.
Run once before deploying Cloud Functions.

Usage:
    python setup_bigquery.py --project YOUR_PROJECT_ID
"""

import argparse
from google.cloud import bigquery

# ──────────────────────────────────────────────
# Table schemas
# ──────────────────────────────────────────────

SCHEMAS = {

    "raw_events": [
        bigquery.SchemaField("event_id",     "STRING",    "NULLABLE"),
        bigquery.SchemaField("event_type",   "STRING",    "REQUIRED"),
        bigquery.SchemaField("raw_json",     "STRING",    "REQUIRED"),
        bigquery.SchemaField("ingested_at",  "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("is_valid",     "BOOL",      "NULLABLE"),
        bigquery.SchemaField("errors",       "STRING",    "NULLABLE"),
        bigquery.SchemaField("source",       "STRING",    "NULLABLE"),
        bigquery.SchemaField("message_id",   "STRING",    "NULLABLE"),
    ],

    "processed_events": [
        bigquery.SchemaField("event_type",      "STRING",    "REQUIRED"),
        bigquery.SchemaField("event_time",      "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("ingested_at",     "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("pipeline_ver",    "STRING",    "NULLABLE"),
        bigquery.SchemaField("region",          "STRING",    "NULLABLE"),
        # IoT fields
        bigquery.SchemaField("device_id",       "STRING",    "NULLABLE"),
        bigquery.SchemaField("temperature_c",   "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("humidity_pct",    "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("pressure_hpa",    "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("battery_pct",     "INT64",     "NULLABLE"),
        # Log fields
        bigquery.SchemaField("service",         "STRING",    "NULLABLE"),
        bigquery.SchemaField("log_level",       "STRING",    "NULLABLE"),
        bigquery.SchemaField("message",         "STRING",    "NULLABLE"),
        bigquery.SchemaField("request_id",      "STRING",    "NULLABLE"),
        bigquery.SchemaField("user_id",         "STRING",    "NULLABLE"),
        bigquery.SchemaField("latency_ms",      "INT64",     "NULLABLE"),
        # Transaction fields
        bigquery.SchemaField("transaction_id",  "STRING",    "NULLABLE"),
        bigquery.SchemaField("amount_usd",      "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("currency",        "STRING",    "NULLABLE"),
        bigquery.SchemaField("merchant",        "STRING",    "NULLABLE"),
        bigquery.SchemaField("payment_method",  "STRING",    "NULLABLE"),
        bigquery.SchemaField("status",          "STRING",    "NULLABLE"),
    ],

    "anomalies": [
        bigquery.SchemaField("event_type",    "STRING",    "REQUIRED"),
        bigquery.SchemaField("entity_id",     "STRING",    "REQUIRED"),
        bigquery.SchemaField("timestamp",     "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("anomaly_type",  "STRING",    "REQUIRED"),
        bigquery.SchemaField("severity",      "STRING",    "REQUIRED"),
        bigquery.SchemaField("description",   "STRING",    "NULLABLE"),
        bigquery.SchemaField("raw_value",     "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("threshold",     "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("region",        "STRING",    "NULLABLE"),
    ],

    "hourly_aggregates": [
        bigquery.SchemaField("event_type",       "STRING",    "REQUIRED"),
        bigquery.SchemaField("window_start",     "TIMESTAMP", "REQUIRED"),
        bigquery.SchemaField("region",           "STRING",    "NULLABLE"),
        bigquery.SchemaField("entity_id",        "STRING",    "NULLABLE"),
        bigquery.SchemaField("event_count",      "INT64",     "NULLABLE"),
        bigquery.SchemaField("avg_temperature_c","FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("min_temperature_c","FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("max_temperature_c","FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("avg_humidity_pct", "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("avg_pressure_hpa", "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("error_count",      "INT64",     "NULLABLE"),
        bigquery.SchemaField("avg_latency_ms",   "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("total_amount_usd", "FLOAT64",   "NULLABLE"),
        bigquery.SchemaField("txn_count",        "INT64",     "NULLABLE"),
        bigquery.SchemaField("computed_at",      "TIMESTAMP", "REQUIRED"),
    ],
}

# Partition + cluster config per table
PARTITIONING = {
    "raw_events":        ("ingested_at",  []),
    "processed_events":  ("event_time",   ["event_type", "region"]),
    "anomalies":         ("timestamp",    ["severity", "event_type"]),
    "hourly_aggregates": ("window_start", ["event_type"]),
}


def create_dataset(client: bigquery.Client, project: str, dataset_id: str,
                   location: str) -> bigquery.Dataset:
    full_id = f"{project}.{dataset_id}"
    dataset = bigquery.Dataset(full_id)
    dataset.location = location
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(f"  Dataset ready: {full_id}")
    return dataset


def create_table(client: bigquery.Client, project: str, dataset_id: str,
                 table_id: str) -> None:
    full_id   = f"{project}.{dataset_id}.{table_id}"
    schema    = SCHEMAS[table_id]
    part_col, cluster_cols = PARTITIONING[table_id]

    table = bigquery.Table(full_id, schema=schema)

    table.time_partitioning = bigquery.TimePartitioning(
        type_   = bigquery.TimePartitioningType.DAY,
        field   = part_col,
        expiration_ms = 90 * 24 * 60 * 60 * 1000,   # 90-day partition expiry
    )

    if cluster_cols:
        table.clustering_fields = cluster_cols

    client.create_table(table, exists_ok=True)
    print(f"  Table ready:   {full_id}")
    print(f"                 partitioned by {part_col}"
          + (f", clustered by {cluster_cols}" if cluster_cols else ""))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",  required=True)
    parser.add_argument("--dataset",  default="pipeline_data")
    parser.add_argument("--location", default="US")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project)

    print(f"\nSetting up BigQuery in project: {args.project}")
    print("─" * 55)

    create_dataset(client, args.project, args.dataset, args.location)

    for table_id in SCHEMAS:
        create_table(client, args.project, args.dataset, table_id)

    print("\nAll tables created successfully.")
    print("\nUseful queries to try:\n")
    print(f"  -- Recent anomalies")
    print(f"  SELECT * FROM `{args.project}.{args.dataset}.anomalies`")
    print(f"  WHERE timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)")
    print(f"  ORDER BY timestamp DESC LIMIT 50;\n")
    print(f"  -- IoT temperature trend (last hour)")
    print(f"  SELECT window_start, entity_id, avg_temperature_c")
    print(f"  FROM `{args.project}.{args.dataset}.hourly_aggregates`")
    print(f"  WHERE event_type='iot_reading'")
    print(f"  AND window_start > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)")
    print(f"  ORDER BY window_start;\n")


if __name__ == "__main__":
    main()
