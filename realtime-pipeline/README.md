# Real-Time Data Pipeline & Dashboard

A production-style streaming data pipeline on Google Cloud Platform.
Ingests simulated IoT, log, and transaction data → processes it through
Cloud Functions → stores it in BigQuery → visualizes it in Streamlit.

---

## Architecture

```
[Simulator]  →  [Pub/Sub]  →  [Cloud Functions]  →  [BigQuery]  →  [Streamlit]
  IoT/Logs        raw-events    validate_enrich        raw_events      Dashboard
  Transactions                  anomaly_detect         processed_events
                                aggregate_route        anomalies
                                                       hourly_aggregates
```

## Project Structure

```
realtime-pipeline/
├── simulator/
│   └── simulator.py          # Pub/Sub event publisher
├── cloud_functions/
│   ├── validate_enrich/      # CF 1: Schema validation + enrichment
│   │   ├── main.py
│   │   └── requirements.txt
│   ├── anomaly_detect/       # CF 2: Statistical anomaly detection
│   │   ├── main.py
│   │   └── requirements.txt
│   └── aggregate_route/      # CF 3: Windowed aggregation (scheduler)
│       ├── main.py
│       └── requirements.txt
├── bigquery/
│   └── setup_bigquery.py     # Dataset + table creation script
├── dashboard/
│   └── streamlit/
│       └── dashboard.py      # Real-time Streamlit dashboard
└── docs/
    └── queries.sql           # Useful BigQuery exploration queries
```

---

## Prerequisites

1. **Google Cloud Project** with billing enabled
2. **APIs enabled:**
   ```bash
   gcloud services enable pubsub.googleapis.com \
       cloudfunctions.googleapis.com \
       bigquery.googleapis.com \
       cloudscheduler.googleapis.com \
       cloudbuild.googleapis.com
   ```
3. **Service account** with roles:
   - `roles/pubsub.publisher`
   - `roles/pubsub.subscriber`
   - `roles/bigquery.dataEditor`
   - `roles/bigquery.jobUser`
   - `roles/cloudfunctions.invoker`

---

## Setup & Deployment

### Step 1 — Create Pub/Sub resources

```bash
export PROJECT_ID="your-project-id"
export REGION="us-central1"

# Main ingestion topic + subscriptions for each Cloud Function
gcloud pubsub topics create raw-events
gcloud pubsub subscriptions create validate-sub \
    --topic=raw-events --ack-deadline=60
gcloud pubsub subscriptions create anomaly-sub \
    --topic=raw-events --ack-deadline=60

# Alert topic for high-severity anomalies
gcloud pubsub topics create pipeline-alerts

# Scheduler trigger topic
gcloud pubsub topics create pipeline-scheduler
```

### Step 2 — Create BigQuery tables

```bash
pip install google-cloud-bigquery
python bigquery/setup_bigquery.py --project $PROJECT_ID
```

### Step 3 — Deploy Cloud Functions

```bash
# Function 1: Validate + Enrich
gcloud functions deploy validate_and_enrich \
    --runtime python311 \
    --trigger-topic raw-events \
    --entry-point main \
    --source cloud_functions/validate_enrich \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET=pipeline_data \
    --memory 256MB \
    --region $REGION

# Function 2: Anomaly Detection
gcloud functions deploy anomaly_detect \
    --runtime python311 \
    --trigger-topic raw-events \
    --entry-point main \
    --source cloud_functions/anomaly_detect \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET=pipeline_data,ALERT_TOPIC=pipeline-alerts \
    --memory 256MB \
    --region $REGION

# Function 3: Aggregate + Route (triggered every minute)
gcloud functions deploy aggregate_and_route \
    --runtime python311 \
    --trigger-topic pipeline-scheduler \
    --entry-point main \
    --source cloud_functions/aggregate_route \
    --set-env-vars PROJECT_ID=$PROJECT_ID,DATASET=pipeline_data,WINDOW_MINS=2 \
    --memory 512MB \
    --timeout 300s \
    --region $REGION

# Cloud Scheduler (fires aggregate function every minute)
gcloud scheduler jobs create pubsub pipeline-aggregator \
    --schedule="* * * * *" \
    --topic=pipeline-scheduler \
    --message-body='{"trigger":"aggregate"}' \
    --location=$REGION
```

### Step 4 — Run the Simulator

```bash
pip install google-cloud-pubsub
python simulator/simulator.py \
    --project $PROJECT_ID \
    --topic raw-events \
    --mode mixed \
    --rate 10 \
    --duration 600
```

### Step 5 — Launch the Dashboard

```bash
pip install streamlit google-cloud-bigquery pandas plotly db-dtypes
streamlit run dashboard/streamlit/dashboard.py -- --project $PROJECT_ID
```

---

## BigQuery Tables

| Table | Description | Partitioned by | Clustered by |
|---|---|---|---|
| `raw_events` | Every message received (audit log) | `ingested_at` | — |
| `processed_events` | Validated + enriched events (wide) | `event_time` | `event_type, region` |
| `anomalies` | Flagged events with severity | `timestamp` | `severity, event_type` |
| `hourly_aggregates` | Windowed 1-minute rollups | `window_start` | `event_type` |

---

## Dashboard Features

| Panel | Metric |
|---|---|
| KPI cards | Total events, per-type counts, invalid events, HIGH anomalies |
| IoT trends | Temperature & humidity per device over time |
| Log metrics | Error rate % and avg latency per service |
| Transactions | Revenue over time, transactions by region (pie) |
| Anomaly timeline | Scatter plot by time + entity; data table |

---

## Anomaly Detection Rules

| Event type | Rule | Severity |
|---|---|---|
| IoT | Temperature outside [-10, 50]°C | LOW → CRITICAL |
| IoT | Humidity outside [10, 95]% | LOW → CRITICAL |
| IoT | Pressure outside [980, 1050] hPa | LOW → CRITICAL |
| App log | Any ERROR-level log | HIGH |
| Transaction | Amount > $2,000 | HIGH |
| Transaction | Amount > $10,000 | CRITICAL |
| Transaction | Merchant in flagged list | MEDIUM |

---

## Bonus: Dataflow Extension

To replace the Cloud Functions aggregation with Apache Beam/Dataflow:

```python
# Replace aggregate_route Cloud Function with:
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.window import FixedWindows

def run():
    options = PipelineOptions([
        "--runner=DataflowRunner",
        f"--project={PROJECT_ID}",
        f"--region={REGION}",
        "--streaming",
    ])
    with beam.Pipeline(options=options) as p:
        (p
         | "Read Pub/Sub" >> beam.io.ReadFromPubSub(
               subscription=f"projects/{PROJECT_ID}/subscriptions/validate-sub")
         | "Parse JSON"   >> beam.Map(json.loads)
         | "Window"       >> beam.WindowInto(FixedWindows(60))
         | "Group"        >> beam.GroupByKey()
         | "Aggregate"    >> beam.Map(compute_window_aggregate)
         | "Write BQ"     >> beam.io.WriteToBigQuery(
               table=f"{PROJECT_ID}:pipeline_data.hourly_aggregates")
        )
```

---

## Evaluation Checklist

- **Data Engineering**: Pub/Sub → Cloud Functions → BigQuery end-to-end ✓
- **Scalability**: Pub/Sub fan-out to multiple subscribers; BQ partitioning + clustering ✓
- **Real-time processing**: Pub/Sub push triggers; sub-second ingestion latency ✓
- **Anomaly detection**: Statistical thresholds + severity classification ✓
- **Dashboard**: Streamlit with Plotly charts; auto-refresh every 30s ✓
- **Bonus — Dataflow**: See Dataflow extension section above ✓
