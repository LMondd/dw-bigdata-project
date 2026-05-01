# Synthetic IoT Sensor Stream — Design

> See `CLAUDE.md` Section 3.3 for the high-level spec.

## Sensors

| Type | Count | Baseline | Noise | Anomaly behavior | Anomaly rate |
|---|---|---|---|---|---|
| Temperature | 20 | 22°C | ±0.5°C | sustained spike +5–15°C | 1% |
| Pressure | 15 | 100 kPa | ±1 kPa | brief spike ±10 kPa | 0.5% |
| Vibration | 10 | 0.5 g | ±0.1 g | drift +0.5 g/hour | 2% windows |
| Flow | 5 | correlated with pressure | — | follows pressure anomalies | inherited |

## Zones

5 zones, each containing a mix of sensors. Sensors in the same zone share some
correlation (e.g., zone-wide temperature drift).

## Modes

- **Live mode:** one reading per sensor per 5 seconds → Kafka topic `sensor-stream`
- **Backfill mode:** bypass Kafka, write Parquet directly to `s3://<bucket>/bronze/sensor_readings/date=YYYY-MM-DD/hour=HH/`. Used to populate ~30 days of history quickly.

## Ground truth

Each row carries an `is_anomaly` boolean (ground truth) so the hot-path detector's accuracy can be measured.
