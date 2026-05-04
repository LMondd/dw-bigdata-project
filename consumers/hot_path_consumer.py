"""
hot_path_consumer.py — Hot path Kafka consumer for sensor-stream.
Reads sensor-stream topic, computes 1-minute tumbling window aggregates,
flags anomalies, writes to DynamoDB.
Run: python -m consumers.hot_path_consumer
"""

import json
import logging
import time
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from decimal import Decimal

import boto3
from confluent_kafka import Consumer, KafkaException

from producers.common.config import KAFKA_BOOTSTRAP_SERVERS

logger = logging.getLogger(__name__)

TOPIC = "sensor-stream"
DYNAMODB_TABLE = "sensor-aggregates"
WINDOW_SECONDS = 60
ANOMALY_SIGMA = 3.0
TTL_HOURS = 24


def get_consumer() -> Consumer:
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": "hot-path-consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC])
    logger.info(f"Subscribed to {TOPIC}")
    return consumer


def get_window_start(ts: datetime) -> str:
    """Truncate timestamp to 1-minute window."""
    truncated = ts.replace(second=0, microsecond=0)
    return truncated.isoformat()


def compute_anomaly(value: float, target: float, sigma: float) -> bool:
    """Flag as anomaly if value is more than 3-sigma from target."""
    return abs(value - target) > (ANOMALY_SIGMA * sigma)


SENSOR_SIGMA = {
    "temperature": 1.5,
    "pressure": 2.0,
    "vibration": 0.1,
    "flow": 3.0,
}


def write_to_dynamodb(
    table,
    sensor_id: str,
    sensor_type: str,
    zone_id: str,
    window_start: str,
    readings: list[dict],
) -> None:
    """Write 1-minute aggregate to DynamoDB."""
    values = [r["value"] for r in readings]
    targets = [r["target"] for r in readings]
    anomalies = [r for r in readings if r.get("is_anomaly")]

    avg_val = sum(values) / len(values)
    target = targets[0] if targets else 0
    sigma = SENSOR_SIGMA.get(sensor_type, 1.0)
    is_anomaly = compute_anomaly(avg_val, target, sigma)

    ttl = int((datetime.now(timezone.utc) + timedelta(hours=TTL_HOURS)).timestamp())

    table.put_item(Item={
        "sensor_id": sensor_id,
        "window_start": window_start,
        "sensor_type": sensor_type,
        "zone_id": zone_id,
        "avg_value": Decimal(str(round(avg_val, 4))),
        "max_value": Decimal(str(round(max(values), 4))),
        "min_value": Decimal(str(round(min(values), 4))),
        "target_value": Decimal(str(target)),
        "reading_count": len(readings),
        "anomaly_count": len(anomalies),
        "is_anomaly": is_anomaly,
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "ttl": ttl,
    })


def run() -> None:
    logger.info("Starting hot path consumer")
    consumer = get_consumer()
    dynamodb = boto3.resource("dynamodb", region_name="ap-southeast-1")
    table = dynamodb.Table(DYNAMODB_TABLE)

    # Buffer: {sensor_id: {window_start: [readings]}}
    buffers: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    last_flush = datetime.now(timezone.utc)

    try:
        while True:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                pass
            elif msg.error():
                logger.error(f"Consumer error: {msg.error()}")
            else:
                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    sensor_id = payload.get("sensor_id")
                    reading_ts = datetime.fromisoformat(
                        payload.get("reading_ts", "").replace("Z", "+00:00")
                    )
                    window_start = get_window_start(reading_ts)
                    buffers[sensor_id][window_start].append(payload)

                except Exception as e:
                    logger.error(f"Failed to parse message: {e}")

            # Flush completed windows every 30 seconds
            now = datetime.now(timezone.utc)
            if (now - last_flush).seconds >= 30:
                current_window = get_window_start(now)

                for sensor_id, windows in list(buffers.items()):
                    for window_start, readings in list(windows.items()):
                        if window_start < current_window and readings:
                            try:
                                write_to_dynamodb(
                                    table=table,
                                    sensor_id=sensor_id,
                                    sensor_type=readings[0].get("sensor_type", "unknown"),
                                    zone_id=readings[0].get("zone_id", "unknown"),
                                    window_start=window_start,
                                    readings=readings,
                                )
                                logger.info(
                                    f"Wrote aggregate: {sensor_id} "
                                    f"window={window_start} "
                                    f"readings={len(readings)} "
                                    f"anomaly={readings[0].get('is_anomaly', False)}"
                                )
                                del buffers[sensor_id][window_start]
                            except Exception as e:
                                logger.error(f"Failed to write to DynamoDB: {e}")

                last_flush = now

    except KeyboardInterrupt:
        logger.info("Shutting down hot path consumer...")
    finally:
        consumer.close()
        logger.info("Consumer closed")


if __name__ == "__main__":
    run()
