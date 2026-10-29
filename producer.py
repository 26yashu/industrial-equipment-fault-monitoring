"""Generate simulated industrial sensor readings and publish them to Kafka."""
import json
import random
import time
import uuid
from datetime import datetime
from kafka import KafkaProducer
from config import KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC

def make_event():
    # Normal operating ranges are centered around the model's training data.
    event = {
        "event_id": str(uuid.uuid4()),
        "event_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "machine_id": random.choice(["MOTOR-01", "PUMP-02", "COMPRESSOR-03"]),
        "temperature_c": round(random.gauss(65, 4), 2),
        "vibration_mm_s": round(max(0.5, random.gauss(2.0, 0.45)), 2),
        "pressure_bar": round(random.gauss(6.0, 0.35), 2),
        "rpm": int(random.gauss(1500, 55)),
    }

    # Inject an abnormal event occasionally so the demo visibly produces alerts.
    if random.random() < 0.08:
        fault_type = random.choice(["overheating", "high_vibration", "pressure_drop", "overspeed"])
        if fault_type == "overheating":
            event["temperature_c"] = round(random.uniform(90, 115), 2)
        elif fault_type == "high_vibration":
            event["vibration_mm_s"] = round(random.uniform(6, 12), 2)
        elif fault_type == "pressure_drop":
            event["pressure_bar"] = round(random.uniform(1.5, 3.0), 2)
        else:
            event["rpm"] = random.randint(1950, 2400)
        event["simulated_fault_type"] = fault_type
    else:
        event["simulated_fault_type"] = "none"

    return event

def main():
    print(f"Connecting to Kafka at {KAFKA_BOOTSTRAP_SERVERS} ...")
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5,
    )
    print(f"Publishing sensor events to topic '{KAFKA_TOPIC}'. Press Ctrl+C to stop.")
    try:
        while True:
            event = make_event()
            producer.send(KAFKA_TOPIC, value=event)
            producer.flush()
            print(
                f"{event['event_time']} | {event['machine_id']} | "
                f"T={event['temperature_c']} C | V={event['vibration_mm_s']} mm/s | "
                f"P={event['pressure_bar']} bar | RPM={event['rpm']} | "
                f"simulated={event['simulated_fault_type']}",
                flush=True
            )
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nProducer stopped.")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
