"""
bronze_to_silver_air.py — Bronze to Silver ETL for air quality data.
Input:  s3://<bucket>/bronze/air_quality/
Output: s3://<bucket>/silver/air_quality/
Transforms:
  - Cast types
  - Dedupe on (station_id, reading_date, reading_time)
  - Normalize timezone to UTC
  - Handle nulls (-1, -999 → null)
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
INPUT_PATH = f"s3://{BUCKET}/bronze/air_quality/"
OUTPUT_PATH = f"s3://{BUCKET}/silver/air_quality/"

# Read Bronze
df = spark.read.parquet(INPUT_PATH)
print(f"Bronze row count: {df.count():,}")

# Replace -1 and -999 with null
null_cols = ["pm25_value", "pm10_value", "o3_value", "co_value", "no2_value", "so2_value"]
for col in null_cols:
    if col in df.columns:
        df = df.withColumn(
            col,
            F.when(F.col(col) < 0, None).otherwise(F.col(col))
        )

# Dedupe on station_id + reading_date + reading_time
# Keep the latest ingested record
df = df.withColumn(
    "row_num",
    F.row_number().over(
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy("station_id", "reading_date", "reading_time")
        .orderBy(F.col("ingested_at").desc())
    )
).filter(F.col("row_num") == 1).drop("row_num")

# Build reading_ts in UTC
df = df.withColumn(
    "reading_ts_utc",
    F.to_utc_timestamp(
        F.to_timestamp(
            F.concat(F.col("reading_date"), F.lit(" "), F.col("reading_time"), F.lit(":00")),
            "yyyy-MM-dd HH:mm:ss"
        ),
        "Asia/Bangkok"
    )
)

# Cast lat/lon to double
df = df.withColumn("lat", F.col("lat").cast(DoubleType()))
df = df.withColumn("lon", F.col("lon").cast(DoubleType()))

# Add date_key for joining to dim_date
df = df.withColumn(
    "date_key",
    F.date_format(F.col("reading_ts_utc"), "yyyyMMdd").cast(IntegerType())
)

print(f"Silver row count after dedup: {df.count():,}")
df.show(5)

# Write Silver partitioned by date
df.repartition(F.col("reading_date")) \
  .write \
  .mode("overwrite") \
  .partitionBy("reading_date") \
  .parquet(OUTPUT_PATH)

print("Bronze→Silver air quality job complete")
job.commit()
