"""
Real-Time Pipeline Dashboard — Streamlit
==========================================
Connects to BigQuery and renders live charts for:
  • IoT sensor trends (temperature, humidity)
  • Application log error rates and latency
  • Transaction volume and anomalies
  • Anomaly timeline with severity badges

Run:
    streamlit run dashboard.py -- --project YOUR_PROJECT_ID

Install:
    pip install streamlit google-cloud-bigquery pandas plotly db-dtypes
"""

import argparse
import sys
import time
from datetime import datetime, timezone, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

# ──────────────────────────────────────────────
# Page config
# ──────────────────────────────────────────────

st.set_page_config(
    page_title  = "Pipeline Monitor",
    page_icon   = "📡",
    layout      = "wide",
    initial_sidebar_state = "expanded",
)

# ──────────────────────────────────────────────
# CLI args (passed after "--" when using streamlit run)
# ──────────────────────────────────────────────

@st.cache_resource
def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", default="your-project-id")
    parser.add_argument("--dataset", default="pipeline_data")
    # streamlit passes its own args first; split on "--"
    try:
        idx  = sys.argv.index("--")
        argv = sys.argv[idx + 1:]
    except ValueError:
        argv = []
    return parser.parse_args(argv)

args    = get_args()
PROJECT = args.project
DATASET = args.dataset


# ──────────────────────────────────────────────
# BigQuery client (cached across sessions)
# ──────────────────────────────────────────────

@st.cache_resource
def bq_client():
    return bigquery.Client(project=PROJECT)

client = bq_client()


# ──────────────────────────────────────────────
# Query helpers
# ──────────────────────────────────────────────

def q(sql: str) -> pd.DataFrame:
    """Run a BigQuery query and return a DataFrame. Cached for 30s."""
    return client.query(sql).to_dataframe()


def table(name: str) -> str:
    return f"`{PROJECT}.{DATASET}.{name}`"


# ──────────────────────────────────────────────
# Sidebar controls
# ──────────────────────────────────────────────

with st.sidebar:
    st.title("📡 Pipeline Monitor")
    st.markdown(f"**Project:** `{PROJECT}`")
    st.markdown(f"**Dataset:** `{DATASET}`")
    st.divider()

    lookback_h  = st.slider("Lookback (hours)", 1, 24, 2)
    auto_refresh = st.toggle("Auto-refresh (30s)", value=True)
    st.divider()

    event_filter = st.multiselect(
        "Event types",
        ["iot_reading", "app_log", "transaction"],
        default=["iot_reading", "app_log", "transaction"],
    )
    severity_filter = st.multiselect(
        "Anomaly severity",
        ["LOW", "MEDIUM", "HIGH", "CRITICAL"],
        default=["HIGH", "CRITICAL"],
    )

since = (
    datetime.now(timezone.utc) - timedelta(hours=lookback_h)
).strftime("%Y-%m-%dT%H:%M:%S")


# ──────────────────────────────────────────────
# Header
# ──────────────────────────────────────────────

st.title("Real-Time Data Pipeline Dashboard")
st.caption(f"Showing data from the last **{lookback_h}h** — "
           f"last refreshed {datetime.now().strftime('%H:%M:%S')}")

# ──────────────────────────────────────────────
# Section 1: KPI cards
# ──────────────────────────────────────────────

st.subheader("Overview")

kpi_sql = f"""
SELECT
    COUNTIF(event_type = 'iot_reading')  AS iot_events,
    COUNTIF(event_type = 'app_log')      AS log_events,
    COUNTIF(event_type = 'transaction')  AS txn_events,
    COUNTIF(NOT is_valid)                AS invalid_events,
    COUNT(*)                             AS total_events
FROM {table('raw_events')}
WHERE ingested_at >= TIMESTAMP('{since}')
"""

anomaly_sql = f"""
SELECT
    COUNT(*)                                   AS total_anomalies,
    COUNTIF(severity IN ('HIGH','CRITICAL'))   AS high_anomalies
FROM {table('anomalies')}
WHERE timestamp >= TIMESTAMP('{since}')
"""

try:
    kpi_df  = q(kpi_sql)
    anom_df = q(anomaly_sql)

    c1, c2, c3, c4, c5, c6 = st.columns(6)
    c1.metric("Total Events",     f"{kpi_df['total_events'][0]:,}")
    c2.metric("IoT Readings",     f"{kpi_df['iot_events'][0]:,}")
    c3.metric("App Logs",         f"{kpi_df['log_events'][0]:,}")
    c4.metric("Transactions",     f"{kpi_df['txn_events'][0]:,}")
    c5.metric("Invalid Events",   f"{kpi_df['invalid_events'][0]:,}",
              delta_color="inverse")
    c6.metric("⚠ HIGH Anomalies", f"{anom_df['high_anomalies'][0]:,}",
              delta_color="inverse")

except Exception as e:
    st.warning(f"KPI query failed: {e}")

st.divider()

# ──────────────────────────────────────────────
# Section 2: IoT trends
# ──────────────────────────────────────────────

if "iot_reading" in event_filter:
    st.subheader("🌡 IoT Sensor Trends")

    iot_sql = f"""
    SELECT
        window_start,
        entity_id            AS device_id,
        avg_temperature_c,
        avg_humidity_pct,
        event_count
    FROM {table('hourly_aggregates')}
    WHERE event_type = 'iot_reading'
      AND window_start >= TIMESTAMP('{since}')
    ORDER BY window_start
    """

    try:
        iot_df = q(iot_sql)
        if iot_df.empty:
            st.info("No IoT data in this window — is the simulator running?")
        else:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.line(iot_df, x="window_start", y="avg_temperature_c",
                              color="device_id", title="Temperature °C over time",
                              labels={"window_start": "Time",
                                      "avg_temperature_c": "Temp (°C)"})
                fig.update_layout(showlegend=len(iot_df["device_id"].unique()) < 8)
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig2 = px.line(iot_df, x="window_start", y="avg_humidity_pct",
                               color="device_id", title="Humidity % over time",
                               labels={"window_start": "Time",
                                       "avg_humidity_pct": "Humidity (%)"})
                fig2.update_layout(showlegend=len(iot_df["device_id"].unique()) < 8)
                st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"IoT chart error: {e}")

# ──────────────────────────────────────────────
# Section 3: Application logs
# ──────────────────────────────────────────────

if "app_log" in event_filter:
    st.subheader("📋 Application Log Metrics")

    log_sql = f"""
    SELECT
        window_start,
        entity_id            AS service,
        event_count,
        error_count,
        avg_latency_ms,
        SAFE_DIVIDE(error_count, event_count) * 100 AS error_rate_pct
    FROM {table('hourly_aggregates')}
    WHERE event_type = 'app_log'
      AND window_start >= TIMESTAMP('{since}')
    ORDER BY window_start
    """

    try:
        log_df = q(log_sql)
        if log_df.empty:
            st.info("No log data in this window.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(log_df, x="window_start", y="error_rate_pct",
                             color="service",
                             title="Error rate % by service",
                             labels={"window_start": "Time",
                                     "error_rate_pct": "Error rate (%)"})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                fig2 = px.line(log_df, x="window_start", y="avg_latency_ms",
                               color="service", title="Avg latency (ms)",
                               labels={"window_start": "Time",
                                       "avg_latency_ms": "Latency (ms)"})
                st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Log chart error: {e}")

# ──────────────────────────────────────────────
# Section 4: Transactions
# ──────────────────────────────────────────────

if "transaction" in event_filter:
    st.subheader("💳 Transaction Volume & Revenue")

    txn_sql = f"""
    SELECT
        window_start,
        entity_id            AS payment_method,
        txn_count,
        total_amount_usd,
        error_count          AS declined_count,
        region
    FROM {table('hourly_aggregates')}
    WHERE event_type = 'transaction'
      AND window_start >= TIMESTAMP('{since}')
    ORDER BY window_start
    """

    try:
        txn_df = q(txn_sql)
        if txn_df.empty:
            st.info("No transaction data in this window.")
        else:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.area(txn_df, x="window_start", y="total_amount_usd",
                              color="payment_method",
                              title="Revenue over time (USD)",
                              labels={"window_start": "Time",
                                      "total_amount_usd": "Revenue ($)"})
                st.plotly_chart(fig, use_container_width=True)

            with col2:
                region_df = (
                    txn_df.groupby("region")["txn_count"].sum().reset_index()
                )
                fig2 = px.pie(region_df, names="region", values="txn_count",
                              title="Transactions by region")
                st.plotly_chart(fig2, use_container_width=True)

    except Exception as e:
        st.error(f"Transaction chart error: {e}")

# ──────────────────────────────────────────────
# Section 5: Anomaly timeline
# ──────────────────────────────────────────────

st.subheader("⚠ Anomaly Timeline")

sev_str = ", ".join(f"'{s}'" for s in severity_filter)
anom_timeline_sql = f"""
SELECT
    timestamp,
    event_type,
    entity_id,
    anomaly_type,
    severity,
    description,
    raw_value,
    threshold
FROM {table('anomalies')}
WHERE timestamp >= TIMESTAMP('{since}')
  AND severity IN ({sev_str})
ORDER BY timestamp DESC
LIMIT 500
"""

SEVERITY_COLORS = {
    "LOW":      "#3498db",
    "MEDIUM":   "#f39c12",
    "HIGH":     "#e67e22",
    "CRITICAL": "#e74c3c",
}

try:
    anom_df = q(anom_timeline_sql)
    if anom_df.empty:
        st.success("No anomalies matching your filters. System looks healthy!")
    else:
        # Timeline scatter
        anom_df["color"] = anom_df["severity"].map(SEVERITY_COLORS)
        fig = px.scatter(
            anom_df, x="timestamp", y="entity_id",
            color="severity",
            color_discrete_map=SEVERITY_COLORS,
            symbol="event_type",
            hover_data=["anomaly_type", "description", "raw_value", "threshold"],
            title=f"Anomaly Timeline ({len(anom_df)} events)",
            labels={"timestamp": "Time", "entity_id": "Entity"},
            size_max=12,
        )
        fig.update_traces(marker=dict(size=10, opacity=0.8))
        st.plotly_chart(fig, use_container_width=True)

        # Severity breakdown
        col1, col2 = st.columns([1, 2])
        with col1:
            sev_counts = anom_df["severity"].value_counts()
            fig_bar = go.Figure(go.Bar(
                x=sev_counts.index,
                y=sev_counts.values,
                marker_color=[SEVERITY_COLORS.get(s, "#888") for s in sev_counts.index],
            ))
            fig_bar.update_layout(title="Anomaly counts by severity",
                                  showlegend=False)
            st.plotly_chart(fig_bar, use_container_width=True)

        with col2:
            st.dataframe(
                anom_df[["timestamp", "severity", "event_type",
                          "entity_id", "anomaly_type", "description"]]
                .head(50),
                use_container_width=True,
                hide_index=True,
            )

except Exception as e:
    st.error(f"Anomaly query error: {e}")


# ──────────────────────────────────────────────
# Auto-refresh
# ──────────────────────────────────────────────

if auto_refresh:
    time.sleep(30)
    st.rerun()
