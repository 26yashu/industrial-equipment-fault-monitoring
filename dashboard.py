"""Live Streamlit dashboard for industrial equipment sensor monitoring."""
import time
import pandas as pd
import streamlit as st
import mysql.connector
from streamlit_autorefresh import st_autorefresh
from config import MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE

st.set_page_config(
    page_title="Industrial Fault Monitor",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for modern industrial styling
st.markdown("""
<style>
    .metric-card {
        background: #1e222d;
        border-radius: 8px;
        padding: 15px;
        border-left: 4px solid #3b82f6;
        margin-bottom: 10px;
    }
    .status-normal {
        color: #10b981;
        font-weight: bold;
    }
    .status-anomaly {
        color: #ef4444;
        font-weight: bold;
    }
    .stMetric {
        background-color: rgba(255, 255, 255, 0.03);
        border-radius: 8px;
        padding: 10px;
    }
</style>
""", unsafe_allow_html=True)

# Sidebar
st.sidebar.title("⚙️ Control Panel")
refresh_rate = st.sidebar.slider("Auto-Refresh Rate (seconds)", min_value=1, max_value=10, value=3)
st_autorefresh(interval=refresh_rate * 1000, key="industrial_refresh")

st.sidebar.markdown("---")
st.sidebar.subheader("🔌 System Status")
st.sidebar.markdown("✅ **Kafka Broker:** `localhost:9092`")
st.sidebar.markdown("✅ **Kafka Topic:** `industrial-sensor-events`")
st.sidebar.markdown(f"✅ **Database:** `{MYSQL_DATABASE}`")
st.sidebar.markdown("✅ **ML Model:** `Isolation Forest (150 estimators)`")

st.sidebar.markdown("---")
machine_filter = st.sidebar.selectbox(
    "Filter by Machine",
    options=["All Machines", "MOTOR-01", "PUMP-02", "COMPRESSOR-03"],
    index=0
)

# Title Header
st.title("🏭 Real-Time Industrial Equipment Fault Monitoring")
st.caption("End-to-End Stream Analytics: Simulated Sensor Stream ➜ Kafka ➜ ML Anomaly Detector ➜ MySQL ➜ Live Dashboard")

def read_data():
    conn = mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DATABASE
    )
    try:
        query = """
            SELECT id, event_time, machine_id, temperature_c, vibration_mm_s,
                   pressure_bar, rpm, rolling_vibration, is_anomaly
            FROM sensor_readings
            ORDER BY id DESC
            LIMIT 300
        """
        return pd.read_sql(query, conn)
    finally:
        conn.close()

try:
    df_raw = read_data()
except Exception as exc:
    st.error("⚠️ Cannot connect to MySQL. Ensure MySQL is running and credentials in config.py are correct.")
    st.code(str(exc))
    st.stop()

if df_raw.empty:
    st.info("ℹ️ No sensor events stored yet. Waiting for incoming stream...")
    st.stop()

# Data processing
df_raw["event_time"] = pd.to_datetime(df_raw["event_time"])
df = df_raw.copy()
if machine_filter != "All Machines":
    df = df[df["machine_id"] == machine_filter]

if df.empty:
    st.warning(f"No events found for {machine_filter} in recent window.")
    st.stop()

df = df.sort_values("event_time")
latest = df.iloc[-1]
total_events = len(df)
anomaly_count = int(df["is_anomaly"].sum())
normal_count = total_events - anomaly_count
anomaly_rate = (anomaly_count / total_events) * 100 if total_events > 0 else 0

# Machine Fleet Status Cards
st.subheader("🖥️ Equipment Fleet Status")
machines = ["MOTOR-01", "PUMP-02", "COMPRESSOR-03"]
f_cols = st.columns(len(machines))
for idx, m_id in enumerate(machines):
    m_data = df_raw[df_raw["machine_id"] == m_id]
    with f_cols[idx]:
        if not m_data.empty:
            m_latest = m_data.sort_values("event_time").iloc[-1]
            is_anom = m_latest["is_anomaly"] == 1
            status_text = "CRITICAL / ANOMALY" if is_anom else "NORMAL OPERATING"
            status_color = "🔴" if is_anom else "🟢"
            st.metric(
                label=f"{status_color} {m_id}",
                value=status_text,
                delta=f"{m_latest['temperature_c']:.1f}°C | {m_latest['vibration_mm_s']:.2f} mm/s",
                delta_color="inverse" if is_anom else "normal"
            )
        else:
            st.metric(label=f"⚪ {m_id}", value="NO DATA", delta="Waiting for events")

st.markdown("---")

# KPI Summary Metrics
k1, k2, k3, k4, k5 = st.columns(5)
k1.metric("Latest Temperature", f"{latest['temperature_c']:.1f} °C", delta=f"{latest['temperature_c'] - 65.0:+.1f} vs baseline")
k2.metric("Latest Vibration", f"{latest['vibration_mm_s']:.2f} mm/s", delta=f"{latest['vibration_mm_s'] - latest['rolling_vibration']:+.2f} vs rolling avg")
k3.metric("Latest Pressure", f"{latest['pressure_bar']:.2f} bar", delta=f"{latest['pressure_bar'] - 6.0:+.2f} vs baseline")
k4.metric("Motor Speed (RPM)", f"{int(latest['rpm'])} RPM", delta=f"{int(latest['rpm']) - 1500:+d} vs baseline")
k5.metric("Anomalies in Window", f"{anomaly_count} / {total_events}", delta=f"{anomaly_rate:.1f}% rate", delta_color="inverse")

# Main Charts
st.subheader("📈 Real-Time Sensor Telemetry")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.markdown("**Temperature (°C), Vibration (mm/s), Pressure (bar)**")
    plot_df = df.set_index("event_time")[["temperature_c", "vibration_mm_s", "pressure_bar"]]
    st.line_chart(plot_df, height=300)

with chart_col2:
    st.markdown("**Motor RPM & Operating Speed**")
    st.line_chart(df.set_index("event_time")[["rpm"]], height=300)

st.markdown("**Vibration: Instantaneous Reading vs 10-Point Rolling Average**")
st.line_chart(df.set_index("event_time")[["vibration_mm_s", "rolling_vibration"]], height=240)

# Alert Section
st.subheader("🚨 Detected Anomaly Alerts")
alerts = df[df["is_anomaly"] == 1].sort_values("event_time", ascending=False)
if alerts.empty:
    st.success("✅ All systems operating within normal nominal parameters. No anomalies detected in current window.")
else:
    st.error(f"⚠️ **{len(alerts)} Anomalous Events Flagged** by ML Isolation Forest & Domain Thresholds.")
    alert_display = alerts[["event_time", "machine_id", "temperature_c", "vibration_mm_s", "pressure_bar", "rpm"]].copy()
    alert_display.rename(columns={
        "event_time": "Timestamp",
        "machine_id": "Machine ID",
        "temperature_c": "Temp (°C)",
        "vibration_mm_s": "Vibration (mm/s)",
        "pressure_bar": "Pressure (bar)",
        "rpm": "RPM"
    }, inplace=True)
    st.dataframe(alert_display, use_container_width=True, hide_index=True)

# Recent Event Log
with st.expander("📋 View Recent Event Log (Latest 25 Events)", expanded=False):
    log_df = df.sort_values("event_time", ascending=False).head(25)[
        ["event_time", "machine_id", "temperature_c", "vibration_mm_s", "pressure_bar", "rpm", "rolling_vibration", "is_anomaly"]
    ].copy()
    log_df["Status"] = log_df["is_anomaly"].apply(lambda x: "🚨 ANOMALY" if x == 1 else "✓ Normal")
    log_df.drop(columns=["is_anomaly"], inplace=True)
    st.dataframe(log_df, use_container_width=True, hide_index=True)

st.caption("Live Dashboard auto-refreshes every 3s • Industrial Equipment Fault Monitoring System")
