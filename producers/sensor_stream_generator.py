"""Synthetic IoT sensor stream generator.

Modes:
  --live      produce to Kafka topic `sensor-stream` at ~1 reading/5s per sensor
  --backfill  bypass Kafka, write Parquet directly to S3 Bronze for N days

See docs/synthetic_data_design.md for the sensor model.
"""
