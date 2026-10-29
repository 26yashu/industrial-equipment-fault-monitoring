# Real-Time Industrial Equipment Fault Monitoring Using Data Stream Analytics

**Student name(s):** [Enter names]  
**Course:** Data Stream Analytics  
**Date:** [Enter date]

## Abstract
This project implements a real-time industrial equipment monitoring pipeline. Simulated temperature, vibration, pressure and motor-speed events are published through Apache Kafka, processed in Python, scored by an Isolation Forest model, stored in MySQL and presented in an automatically refreshing Streamlit dashboard. The system is designed to demonstrate streaming ingestion, live processing and anomaly flagging using reproducible synthetic data.

## 1. Problem statement
Industrial equipment can produce sensor patterns that differ from normal operation. This project demonstrates how a streaming pipeline can surface unusual sensor readings as they arrive. Because the dataset is simulated, the results are a demonstration of the architecture rather than proof of performance on real factory equipment.

## 2. Objectives
- Generate timestamped sensor readings.
- Transport events with Apache Kafka.
- Compute a rolling vibration average.
- Train and serve an Isolation Forest model.
- Store sensor readings and anomaly flags in MySQL.
- Display live metrics and alerts in Streamlit.

## 3. Architecture
Sensor simulator (Python) → Kafka → Python stream processor + Isolation Forest → MySQL → Streamlit live dashboard.

## 4. Dataset and event fields
Each event includes event ID, timestamp, machine ID, temperature in °C, vibration in mm/s, pressure in bar, RPM and a simulator-only fault label. The fault label is used to make the demonstration interpretable; the model does not use it when scoring events.

## 5. Implementation
### Layer 1: Data source
Describe how `producer.py` creates normal and occasional abnormal sensor events.

### Layer 2: Streaming tool
Describe the Kafka broker and `industrial-sensor-events` topic.

### Layer 3: Stream processor
Describe the Kafka consumer and rolling average over the latest 10 vibration readings.

### Layer 4: Machine learning
Describe the Isolation Forest trained on simulated normal readings and loaded by the processor to score each event.

### Layer 5: Database
Describe the MySQL `sensor_readings` table.

### Layer 6: Visualization
Describe the Streamlit KPI metrics, sensor trends, alert table and 3-second refresh.

## 6. Results
Fill in only after running the project:
- Number of events stored: [actual result]
- Number of anomalies flagged: [actual result]
- Example normal reading: [actual result]
- Example anomaly reading: [actual result]
- Dashboard refresh behavior observed: [describe actual observation]

## 7. Limitations
The data is synthetic and the model is not validated on labelled real-world equipment failures. Anomaly flags indicate unusual patterns and should not be interpreted as confirmed diagnoses.

## 8. Conclusion
Summarize what worked in the end-to-end pipeline, what you learned about streaming analytics, and one realistic future improvement such as collecting real sensor data or evaluating the model against labelled fault records.

## Appendix
Add selected code snippets, the architecture diagram, dashboard screenshots and evidence from the live demonstration.
