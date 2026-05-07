"""
silver_to_gold_fct_sensor_anomaly.py

Input:  s3://<bucket>/gold/fct_sensor_reading/ — all sensor readings with is_anomaly flag
Output: s3://<bucket>/gold/fct_sensor_anomaly_event/ — one row per detected anomaly

Grain: one row per anomalous sensor reading (is_anomaly = true).
Surrogate keys (sensor_key, zone_key, date_key) are inherited from fct_sensor_reading
which already resolved them — no dim joins needed.

This completes the Lambda architecture loop: the hot path (DynamoDB) detects anomalies
in <1 minute for live alerting; this cold path job materialises the full anomaly history
in the warehouse for trend analysis and reporting.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
GOLD = f"s3://{BUCKET}/gold/"

print("Reading fct_sensor_reading from Gold...")
fct_sensor_reading = spark.read.parquet(f"{GOLD}fct_sensor_reading/")
total_rows = fct_sensor_reading.count()
print(f"Total sensor readings: {total_rows:,}")

print("Filtering anomalies (is_anomaly = true)...")
fct_anomaly = (
    fct_sensor_reading
    .filter(F.col("is_anomaly") == True)
    .withColumn(
        "anomaly_event_key",
        F.monotonically_increasing_id().cast(LongType()) + 1,
    )
    .select(
        F.col("anomaly_event_key"),
        F.col("date_key"),
        F.col("sensor_key"),
        F.col("zone_key"),
        F.col("value"),
        F.col("target"),
        F.col("deviation"),
        F.col("reading_ts_utc"),
        F.col("reading_date"),
    )
)

anomaly_count = fct_anomaly.count()
anomaly_rate = round(anomaly_count / total_rows * 100, 2) if total_rows > 0 else 0
print(f"Anomaly events: {anomaly_count:,} ({anomaly_rate}% of all readings)")

fct_anomaly.coalesce(4).write.mode("overwrite").parquet(f"{GOLD}fct_sensor_anomaly_event/")
print("fct_sensor_anomaly_event written to Gold.")
job.commit()
