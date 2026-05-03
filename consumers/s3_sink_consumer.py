"""
s3_sink_consumer.py — Reads from Kafka topics and writes Parquet to S3 Bronze.
Input:  Kafka topics air-quality-raw, weather-raw
Output: s3://<bucket>/bronze/<source>/date=YYYY-MM-DD/hour=HH/
Commits Kafka offsets only after successful S3 write (at-least-once semantics).
Run: python -m consumers.s3_sink_consumer
"""

import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from confluent_kafka import Consumer, KafkaException

from producers.common.config import KAFKA_BOOTSTRAP_SERVERS, S3_BUCKET

logger = logging.getLogger(__name__)

# How many messages to batch before writing to S3
BATCH_SIZE = 50

# Topics to consume
TOPICS = ["air-quality-raw", "weather-raw"]

# Map topic to S3 prefix
TOPIC_TO_PREFIX = {
    "air-quality-raw": "bronze/air_quality",
    "weather-raw": "bronze/weather",
}


def get_consumer(bootstrap_servers: str, group_id: str) -> Consumer:
    """Create and return a Kafka consumer."""
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # manual commit after S3 write
    }
    consumer = Consumer(conf)
    consumer.subscribe(TOPICS)
    logger.info(f"Subscribed to topics: {TOPICS}")
    return consumer


def get_s3_path(prefix: str, dt: datetime) -> str:
    """
    Build S3 path partitioned by date and hour.
    e.g. bronze/air_quality/date=2026-01-01/hour=08/
    """
    return (
        f"{prefix}/"
        f"date={dt.strftime('%Y-%m-%d')}/"
        f"hour={dt.strftime('%H')}/"
    )


def write_batch_to_s3(
    s3_client,
    bucket: str,
    prefix: str,
    messages: list[dict],
    batch_id: str,
) -> None:
    """
    Write a batch of messages as Parquet to S3.

    Args:
        s3_client: boto3 S3 client
        bucket: S3 bucket name
        prefix: S3 path prefix (includes date/hour partitioning)
        messages: List of message dicts to write
        batch_id: Unique ID for this batch (used in filename)
    """
    table = pa.Table.from_pylist(messages)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")

    s3_key = f"{prefix}{batch_id}.parquet"
    s3_client.put_object(
        Bucket=bucket,
        Key=s3_key,
        Body=buf.getvalue().to_pybytes(),
    )
    logger.info(f"Wrote {len(messages)} records to s3://{bucket}/{s3_key}")


def run() -> None:
    """
    Main consumer loop — reads from Kafka and writes batches to S3.
    Runs indefinitely until interrupted with Ctrl+C.
    """
    logger.info("Starting S3 sink consumer")

    consumer = get_consumer(KAFKA_BOOTSTRAP_SERVERS, "s3-sink-consumer")
    s3_client = boto3.client("s3", region_name="ap-southeast-1")

    # Buffer messages per topic
    buffers: dict[str, list[dict]] = defaultdict(list)

    try:
        while True:
            msg = consumer.poll(timeout=5.0)

            if msg is None:
                # No message — flush any non-empty buffers
                for topic, messages in buffers.items():
                    if messages:
                        _flush_buffer(
                            s3_client, topic, messages, consumer, buffers
                        )
                continue

            if msg.error():
                logger.error(f"Consumer error: {msg.error()}")
                continue

            # Parse message
            try:
                payload = json.loads(msg.value().decode("utf-8"))
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse message: {e}")
                continue

            topic = msg.topic()
            buffers[topic].append(payload)

            # Flush when batch is full
            if len(buffers[topic]) >= BATCH_SIZE:
                _flush_buffer(
                    s3_client, topic, buffers[topic], consumer, buffers
                )

    except KeyboardInterrupt:
        logger.info("Shutting down consumer...")
    finally:
        # Flush remaining messages
        for topic, messages in buffers.items():
            if messages:
                _flush_buffer(
                    s3_client, topic, messages, consumer, buffers
                )
        consumer.close()
        logger.info("Consumer closed")


def _flush_buffer(
    s3_client,
    topic: str,
    messages: list[dict],
    consumer: Consumer,
    buffers: dict,
) -> None:
    """Write buffer to S3 then commit Kafka offsets."""
    now = datetime.now(timezone.utc)
    prefix = get_s3_path(TOPIC_TO_PREFIX[topic], now)
    batch_id = now.strftime("%Y%m%d_%H%M%S_%f")

    try:
        write_batch_to_s3(s3_client, S3_BUCKET, prefix, messages, batch_id)
        consumer.commit(asynchronous=False)  # commit AFTER successful S3 write
        buffers[topic] = []  # clear buffer
    except Exception as e:
        logger.error(f"Failed to write batch to S3: {e}")


if __name__ == "__main__":
    run()
