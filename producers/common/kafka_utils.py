"""
kafka_utils.py — Shared Kafka producer wrapper.
Used by all producers in this project.
Every producer imports this instead of managing connections directly.
"""

import json
import logging
import time
from datetime import datetime, timezone
from typing import Any

from confluent_kafka import Producer, KafkaException

logger = logging.getLogger(__name__)


def get_producer(bootstrap_servers: str, retries: int = 5) -> Producer:
    """
    Create and return a Kafka producer with retry logic.

    Args:
        bootstrap_servers: Kafka broker address e.g. '46.137.207.35:9092'
        retries: Number of connection attempts before giving up

    Returns:
        confluent_kafka.Producer instance
    """
    conf = {
        "bootstrap.servers": bootstrap_servers,
        "acks": "all",                  # wait for broker to confirm
        "retries": 3,                   # retry failed sends
        "retry.backoff.ms": 500,        # wait 500ms between retries
        "delivery.timeout.ms": 10000,   # give up after 10 seconds
        "linger.ms": 100,               # batch messages for 100ms
    }

    for attempt in range(1, retries + 1):
        try:
            producer = Producer(conf)
            logger.info(f"Kafka producer connected to {bootstrap_servers}")
            return producer
        except KafkaException as e:
            logger.warning(f"Attempt {attempt}/{retries} failed: {e}")
            if attempt < retries:
                time.sleep(2 ** attempt)  # exponential backoff
            else:
                raise RuntimeError(
                    f"Could not connect to Kafka after {retries} attempts"
                ) from e


def delivery_report(err: Any, msg: Any) -> None:
    """
    Callback fired after each message is delivered or fails.
    Passed to producer.produce() as on_delivery argument.
    """
    if err is not None:
        logger.error(f"Delivery failed for key={msg.key()}: {err}")
    else:
        logger.debug(
            f"Delivered to {msg.topic()} "
            f"partition={msg.partition()} "
            f"offset={msg.offset()}"
        )


def produce_message(
    producer: Producer,
    topic: str,
    key: str,
    payload: dict,
) -> None:
    """
    Serialize payload to JSON and produce to Kafka topic.

    Args:
        producer: Confluent Kafka Producer instance
        topic: Kafka topic name
        key: Message key (used for partitioning e.g. station_id)
        payload: Dict to serialize as JSON
    """
    # Always add ingestion timestamp in UTC
    payload["ingested_at"] = datetime.now(timezone.utc).isoformat()

    try:
        producer.produce(
            topic=topic,
            key=str(key).encode("utf-8"),
            value=json.dumps(payload).encode("utf-8"),
            on_delivery=delivery_report,
        )
        producer.poll(0)  # trigger delivery callbacks without blocking

    except KafkaException as e:
        logger.error(f"Failed to produce message to {topic}: {e}")
        raise


def flush_producer(producer: Producer, timeout: float = 10.0) -> None:
    """
    Wait for all pending messages to be delivered.
    Always call this before exiting a producer script.

    Args:
        producer: Confluent Kafka Producer instance
        timeout: Max seconds to wait
    """
    remaining = producer.flush(timeout=timeout)
    if remaining > 0:
        logger.warning(f"{remaining} messages were NOT delivered before timeout")
    else:
        logger.info("All messages delivered successfully")
