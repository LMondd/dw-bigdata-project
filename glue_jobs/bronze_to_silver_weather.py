"""
bronze_to_silver_weather.py — Bronze to Silver ETL for weather data.
Input:  s3://<bucket>/bronze/weather/
Output: s3://<bucket>/silver/weather/
Transforms:
  - Dedupe on (city_id, reading_ts)
  - Convert Unix timestamp to datetime UTC
  - Cast types
  - Add date_key and reading_date partition
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
INPUT_PATH = f"s3://{BUCKET}/bronze/weather/"
OUTPUT_PATH = f"s3://{BUCKET}/silver/weather/"

# Read Bronze
df = spark.read.parquet(INPUT_PATH)
print(f"Bronze row count: {df.count():,}")

# Convert Unix timestamp to UTC datetime
df = df.withColumn(
    "reading_ts_utc",
    F.to_timestamp(F.col("reading_ts"))
)

# Add reading_date for partitioning
df = df.withColumn(
    "reading_date",
    F.to_date(F.col("reading_ts_utc")).cast("string")
)

# Add date_key for joining to dim_date
df = df.withColumn(
    "date_key",
    F.date_format(F.col("reading_ts_utc"), "yyyyMMdd").cast(IntegerType())
)

# Dedupe on city_id + reading_ts — keep latest ingested
window = Window.partitionBy("city_id", "reading_ts").orderBy(F.col("ingested_at").desc())
df = df.withColumn("row_num", F.row_number().over(window)) \
       .filter(F.col("row_num") == 1) \
       .drop("row_num")

print(f"Silver row count after dedup: {df.count():,}")
df.show(5)

# Write Silver partitioned by date
df.repartition(F.col("reading_date")) \
  .write \
  .mode("overwrite") \
  .partitionBy("reading_date") \
  .parquet(OUTPUT_PATH)

print("Bronze to Silver weather job complete")
job.commit()
