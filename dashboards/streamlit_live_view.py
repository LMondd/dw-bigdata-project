"""
streamlit_live_view.py — Live hot path dashboard.
Polls DynamoDB every 5 seconds and shows sensor aggregates.
Run: streamlit run dashboards/streamlit_live_view.py
"""

import time
import boto3
import pandas as pd
import streamlit as st
from datetime import datetime, timezone, timedelta
from decimal import Decimal

# Page config
st.set_page_config(
    page_title="Live Sensor Dashboard",
    page_icon="🌡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.title("🌡️ Live IoT Sensor Dashboard")
st.caption("Bangkok Facility — Real-time anomaly detection via Kafka → DynamoDB")

# Sidebar
st.sidebar.header("Filters")
refresh_rate = st.sidebar.slider("Refresh rate (seconds)", 5, 30, 5)
show_anomalies_only = st.sidebar.checkbox("Show anomalies only", False)
selected_zone = st.sidebar.selectbox(
    "Filter by zone",
    ["All", "Z01", "Z02", "Z03", "Z04", "Z05"]
)

# Connect to DynamoDB
@st.cache_resource
def get_table():
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
    return dynamodb.Table("sensor-aggregates")

def fetch_recent_data() -> pd.DataFrame:
    """Fetch last 10 minutes of data from DynamoDB."""
    table = get_table()
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=10)).isoformat()

    response = table.scan()
    items = response.get("Items", [])

    if not items:
        return pd.DataFrame()

    df = pd.DataFrame(items)

    # Convert Decimal to float
    for col in ["avg_value", "max_value", "min_value", "target_value"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, Decimal) else x)

    # Filter recent
    if "window_start" in df.columns:
        df = df[df["window_start"] >= cutoff]

    return df


# Main loop
placeholder = st.empty()

while True:
    df = fetch_recent_data()

    with placeholder.container():
        if df.empty:
            st.warning("No data yet — waiting for sensor readings...")
        else:
            # KPI metrics
            total = len(df)
            anomalies = df[df["is_anomaly"] == True] if "is_anomaly" in df.columns else pd.DataFrame()
            anomaly_count = len(anomalies)

            col1, col2, col3, col4 = st.columns(4)
            col1.metric("Total Readings", total)
            col2.metric("Anomalies Detected", anomaly_count, delta=f"{round(anomaly_count/total*100, 1)}%" if total > 0 else "0%")
            col3.metric("Active Sensors", df["sensor_id"].nunique() if "sensor_id" in df.columns else 0)
            col4.metric("Last Updated", datetime.now(timezone.utc).strftime("%H:%M:%S UTC"))

            st.divider()

            # Filter
            if show_anomalies_only and "is_anomaly" in df.columns:
                df = df[df["is_anomaly"] == True]

            if selected_zone != "All" and "zone_id" in df.columns:
                df = df[df["zone_id"] == selected_zone]

            # Anomaly table
            if anomaly_count > 0:
                st.subheader("🚨 Anomalies Detected")
                anomaly_df = anomalies[["sensor_id", "sensor_type", "zone_id", "avg_value", "target_value", "window_start"]].copy()
                anomaly_df.columns = ["Sensor ID", "Type", "Zone", "Avg Value", "Target", "Window"]
                st.dataframe(anomaly_df, use_container_width=True)

            st.divider()

            # All readings table
            st.subheader("📊 Recent Sensor Aggregates")
            if not df.empty:
                display_cols = ["sensor_id", "sensor_type", "zone_id", "avg_value", "target_value", "is_anomaly", "reading_count", "window_start"]
                available = [c for c in display_cols if c in df.columns]
                display_df = df[available].sort_values("window_start", ascending=False).head(50)
                display_df.columns = [c.replace("_", " ").title() for c in display_df.columns]

                # Color anomalies red
                def highlight_anomaly(row):
                    if "Is Anomaly" in row.index and row["Is Anomaly"]:
                        return ["background-color: #ffcccc"] * len(row)
                    return [""] * len(row)

                st.dataframe(
                    display_df.style.apply(highlight_anomaly, axis=1),
                    use_container_width=True
                )

        st.caption(f"Auto-refreshing every {refresh_rate} seconds...")

    time.sleep(refresh_rate)
