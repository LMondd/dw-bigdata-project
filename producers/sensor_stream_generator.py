"""
sensor_stream_generator.py — Synthetic IoT sensor data generator.
Two modes:
  - live:    produces to Kafka topic sensor-stream at ~1 reading/5s per sensor
  - backfill: writes Parquet directly to S3 Bronze for historical data

Run live:     python -m producers.sensor_stream_generator --mode live
Run backfill: python -m producers.sensor_stream_generator --mode backfill
"""

import argparse
import io
import logging
import math
import random
import time
from datetime import datetime, timedelta, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

from producers.common.config import KAFKA_BOOTSTRAP_SERVERS, S3_BUCKET
from producers.common.kafka_utils import (
    flush_producer,
    get_producer,
    produce_message,
)

logger = logging.getLogger(__name__)

# ── Sensor definitions ──────────────────────────────────────────────────────

ZONES = ["Z01", "Z02", "Z03", "Z04", "Z05"]

SENSOR_TYPES = {
    "temperature": {"count": 20, "target": 22.0, "unit": "celsius",  "sigma": 1.5, "anomaly_rate": 0.01},
    "pressure":    {"count": 15, "target": 100.0, "unit": "kPa",     "sigma": 2.0, "anomaly_rate": 0.02},
    "vibration":   {"count": 10, "target": 0.5,   "unit": "mm/s",    "sigma": 0.1, "anomaly_rate": 0.015},
    "flow":        {"count":  5, "target": 50.0,  "unit": "L/min",   "sigma": 3.0, "anomaly_rate": 0.01},
}

TOPIC = "sensor-stream"


def build_sensor_catalog() -> list[dict]:
    """
    Build list of 50 sensors with metadata.
    Returns a list of sensor dicts with id, type, zone, target, sigma.
    """
    sensors = []
    sensor_num = 1

    for sensor_type, config in SENSOR_TYPES.items():
        count = config["count"]
        zones_cycle = ZONES * (count // len(ZONES) + 1)

        for i in range(count):
            sensors.append({
                "sensor_id": f"SEN{sensor_num:03d}",
                "sensor_type": sensor_type,
                "zone_id": zones_cycle[i],
                "target": config["target"],
                "sigma": config["sigma"],
                "unit": config["unit"],
                "anomaly_rate": config["anomaly_rate"],
            })
            sensor_num += 1

    return sensors


def generate_reading(sensor: dict, ts: datetime) -> dict:
    """
    Generate a realistic sensor reading for a given timestamp.
    Applies daily cycle, drift, and random anomalies.

    Args:
        sensor: Sensor metadata dict
        ts: Timestamp for this reading

    Returns:
        Reading dict matching Bronze sensor schema
    """
    hour = ts.hour
    target = sensor["target"]
    sigma = sensor["sigma"]

    # Daily cycle — peaks at 14:00, lowest at 04:00
    daily_offset = math.sin((hour - 4) * math.pi / 12) * sigma * 0.5

    # Random noise
    noise = random.gauss(0, sigma * 0.3)

    # Drift for vibration sensors
    drift = 0.0
    if sensor["sensor_type"] == "vibration":
        drift = (hour / 24) * sigma * 0.2

    # Base value
    value = target + daily_offset + noise + drift

    # Anomaly injection
    is_anomaly = random.random() < sensor["anomaly_rate"]
    if is_anomaly:
        value = target + random.choice([-1, 1]) * sigma * random.uniform(4, 7)

    return {
        "sensor_id": sensor["sensor_id"],
        "sensor_type": sensor["sensor_type"],
        "zone_id": sensor["zone_id"],
        "unit": sensor["unit"],
        "value": round(value, 4),
        "target": sensor["target"],
        "is_anomaly": is_anomaly,
        "reading_ts": ts.isoformat(),
    }


# ── Live mode ────────────────────────────────────────────────────────────────

def run_live(sensors: list[dict]) -> None:
    """
    Produce live sensor readings to Kafka every 5 seconds per sensor.
    Runs indefinitely until Ctrl+C.
    """
    logger.info(f"Starting live mode — {len(sensors)} sensors, 5s interval")
    producer = get_producer(KAFKA_BOOTSTRAP_SERVERS)

    try:
        while True:
            ts = datetime.now(timezone.utc)
            for sensor in sensors:
                reading = generate_reading(sensor, ts)
                produce_message(
                    producer=producer,
                    topic=TOPIC,
                    key=sensor["sensor_id"],
                    payload=reading,
                )
            producer.poll(0)
            logger.info(f"Produced {len(sensors)} readings at {ts.isoformat()}")
            time.sleep(5)

    except KeyboardInterrupt:
        logger.info("Shutting down live mode...")
    finally:
        flush_producer(producer)


# ── Backfill mode ─────────────────────────────────────────────────────────────

def run_backfill(sensors: list[dict], days: int = 30) -> None:
    """
    Generate historical sensor data and write directly to S3 Bronze.
    Simulates `days` days of history in minutes.

    Args:
        sensors: List of sensor metadata dicts
        days: Number of days of history to generate
    """
    logger.info(f"Starting backfill mode — {days} days, {len(sensors)} sensors")
    s3_client = boto3.client("s3", region_name="ap-southeast-1")

    end_ts = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    start_ts = end_ts - timedelta(days=days)
    current_ts = start_ts

    total_rows = 0
    batch: list[dict] = []
    BATCH_SIZE = 10000

    while current_ts < end_ts:
        for sensor in sensors:
            reading = generate_reading(sensor, current_ts)
            batch.append(reading)
            total_rows += 1

            if len(batch) >= BATCH_SIZE:
                _write_batch_to_s3(s3_client, batch, current_ts)
                batch = []
                logger.info(f"Progress: {total_rows:,} rows written, current: {current_ts.date()}")

        current_ts += timedelta(seconds=5)

    # Write remaining batch
    if batch:
        _write_batch_to_s3(s3_client, batch, current_ts)

    logger.info(f"Backfill complete — {total_rows:,} total rows written to S3")


def _write_batch_to_s3(
    s3_client,
    batch: list[dict],
    ts: datetime,
) -> None:
    """Write a batch of readings as Parquet to S3 Bronze."""
    table = pa.Table.from_pylist(batch)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")

    date_str = ts.strftime("%Y-%m-%d")
    hour_str = ts.strftime("%H")
    batch_id = ts.strftime("%Y%m%d_%H%M%S_%f")

    s3_key = (
        f"bronze/sensor_readings/"
        f"date={date_str}/hour={hour_str}/"
        f"{batch_id}.parquet"
    )

    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=buf.getvalue().to_pybytes(),
    )


# ── Entry point ──────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Synthetic IoT sensor generator")
    parser.add_argument(
        "--mode",
        choices=["live", "backfill"],
        required=True,
        help="live: produce to Kafka | backfill: write to S3",
    )
    parser.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of days to backfill (default: 30)",
    )
    args = parser.parse_args()

    sensors = build_sensor_catalog()
    logger.info(f"Built sensor catalog: {len(sensors)} sensors")

    if args.mode == "live":
        run_live(sensors)
    else:
        run_backfill(sensors, days=args.days)


if __name__ == "__main__":
    main()
