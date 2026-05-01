"""Air4Thai → Kafka producer.

TODO:
  - Hit Air4Thai endpoint (see docs/sample_responses/air4thai_sample.json)
  - Normalize each station reading
  - Produce to topic `air-quality-raw` with key=station_id
  - Run on a schedule (cron or EventBridge+Lambda)
"""
