"""Shared settings for the Industrial Equipment Fault Monitoring project."""
import os
from pathlib import Path

# Load local .env file if present (without requiring external dependencies)
_env_file = Path(__file__).resolve().parent / ".env"
if _env_file.is_file():
    try:
        with open(_env_file, "r", encoding="utf-8") as _f:
            for _line in _f:
                _line = _line.strip()
                if _line and not _line.startswith("#") and "=" in _line:
                    _k, _v = _line.split("=", 1)
                    os.environ.setdefault(_k.strip(), _v.strip().strip("'\""))
    except Exception:
        pass

# Kafka Settings
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "industrial-sensor-events")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "industrial-fault-monitor")

# MySQL Database Settings
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE", "industrial_monitor")

# Machine Learning Model Path
MODEL_PATH = os.getenv("MODEL_PATH", "fault_model.joblib")
