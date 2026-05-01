"""Kafka → DynamoDB hot-path consumer.

TODO:
  - Subscribe to sensor-stream topic
  - 1-minute tumbling windows: compute avg, max, min, anomaly flag
  - Write aggregates to DynamoDB (PK=sensor_id, SK=window_start_minute)
  - DynamoDB items have TTL for auto-cleanup (24h)
"""
