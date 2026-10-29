"""Consume Kafka events, score them with ML, compute a rolling metric, and store in MySQL."""
import json
from collections import deque
from datetime import datetime
import joblib
import mysql.connector
from kafka import KafkaConsumer
from config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC, KAFKA_GROUP_ID,
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE,
    MODEL_PATH
)

FEATURES = ["temperature_c", "vibration_mm_s", "pressure_bar", "rpm"]

def get_db_connection():
    return mysql.connector.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DATABASE
    )

def main():
    print(f"Loading ML model from {MODEL_PATH} ...")
    model = joblib.load(MODEL_PATH)
    print(f"Connecting to MySQL database '{MYSQL_DATABASE}' ...")
    db = get_db_connection()
    cursor = db.cursor()
    consumer = KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    vibration_window = deque(maxlen=10)
    print(f"Listening to Kafka topic '{KAFKA_TOPIC}'. Press Ctrl+C to stop.")
    try:
        for message in consumer:
            event = message.value
            try:
                values = [[
                    float(event["temperature_c"]),
                    float(event["vibration_mm_s"]),
                    float(event["pressure_bar"]),
                    int(event["rpm"]),
                ]]
                import pandas as pd
                features_df = pd.DataFrame(values, columns=FEATURES)
                prediction = int(model.predict(features_df)[0])  # -1 anomaly, 1 normal
                
                # Hybrid detection: ML Isolation Forest + Industrial Safety Thresholds
                temp, vib, press, rpm_val = values[0]
                rule_fault = (
                    temp > 85.0 or 
                    vib > 4.5 or 
                    press < 4.0 or press > 8.0 or 
                    rpm_val > 1850
                )
                is_anomaly = 1 if (prediction == -1 or rule_fault) else 0

                vibration_window.append(vib)
                rolling_vibration = sum(vibration_window) / len(vibration_window)
                event_time = datetime.strptime(event["event_time"], "%Y-%m-%d %H:%M:%S")

                cursor.execute(
                    """INSERT INTO sensor_readings
                    (event_id, event_time, machine_id, temperature_c, vibration_mm_s,
                     pressure_bar, rpm, rolling_vibration, is_anomaly)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON DUPLICATE KEY UPDATE event_id=event_id""",
                    (
                        event["event_id"], event_time, event["machine_id"],
                        temp, vib, press, rpm_val,
                        rolling_vibration, is_anomaly
                    )
                )
                db.commit()
                status = "[ANOMALY]" if is_anomaly else "[NORMAL]"
                print(
                    f"{event_time} | {event['machine_id']:<14} | "
                    f"T={temp:5.1f}C | V={vib:4.2f} mm/s (avg:{rolling_vibration:4.2f}) | "
                    f"P={press:4.2f} bar | RPM={rpm_val:4d} | {status}",
                    flush=True
                )
            except (KeyError, TypeError, ValueError) as error:
                print(f"Skipping invalid event: {error} | event={event}", flush=True)
    except KeyboardInterrupt:
        print("\nProcessor stopped.")
    finally:
        consumer.close()
        cursor.close()
        db.close()

if __name__ == "__main__":
    main()
