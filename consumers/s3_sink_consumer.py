"""Kafka → S3 Bronze sink consumer (cold path).

TODO:
  - Subscribe to topics: air-quality-raw, weather-raw, sensor-stream
  - Batch messages, write Parquet to s3://<bucket>/bronze/<topic>/date=.../hour=.../
  - Commit offsets only after successful S3 write (at-least-once)
"""
