"""Shared Kafka producer wrapper.

TODO: implement a thin wrapper around confluent_kafka.Producer with:
  - JSON serialization
  - Retry/backoff
  - Delivery-report logging
  - Graceful shutdown via flush()
"""
