CREATE DATABASE IF NOT EXISTS industrial_monitor;
USE industrial_monitor;

CREATE TABLE IF NOT EXISTS sensor_readings (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    event_id VARCHAR(64) NOT NULL UNIQUE,
    event_time DATETIME NOT NULL,
    machine_id VARCHAR(50) NOT NULL,
    temperature_c DOUBLE NOT NULL,
    vibration_mm_s DOUBLE NOT NULL,
    pressure_bar DOUBLE NOT NULL,
    rpm INT NOT NULL,
    rolling_vibration DOUBLE NOT NULL,
    is_anomaly TINYINT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_event_time (event_time),
    INDEX idx_anomaly (is_anomaly)
);
