"""
silver_to_gold_facts.py — Load all fact tables to Gold.
Facts: fct_air_quality_hourly, fct_pollution_daily_summary, fct_sensor_reading
Joins Silver data with Gold dims to get surrogate keys.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
SILVER_AIR    = f"s3://{BUCKET}/silver/air_quality/"
SILVER_SENSOR = f"s3://{BUCKET}/silver/sensor_readings/"
GOLD          = f"s3://{BUCKET}/gold/"

# ── Load dims ─────────────────────────────────────────────────────────────────
print("Loading Gold dimensions...")
dim_station = spark.read.parquet(f"{GOLD}dim_station/").filter(F.col("is_current") == True)
dim_sensor  = spark.read.parquet(f"{GOLD}dim_sensor/").filter(F.col("is_current") == True)
dim_region  = spark.read.parquet(f"{GOLD}dim_region/")
dim_weather = spark.read.parquet(f"{GOLD}dim_weather_condition/")
dim_zone    = spark.read.parquet(f"{GOLD}dim_zone/")

# ── fct_air_quality_hourly ────────────────────────────────────────────────────
print("Building fct_air_quality_hourly...")
air_df = spark.read.parquet(SILVER_AIR)

fct_air = air_df \
    .join(dim_station.select("station_key", "station_id"),
          on="station_id", how="left") \
    .join(dim_region.select("region_key", "region_name"),
          air_df.area_en == dim_region.region_name, how="left") \
    .withColumn("reading_hour", F.hour(F.col("reading_ts_utc"))) \
    .withColumn("air_quality_key",
                F.monotonically_increasing_id().cast(LongType()) + 1) \
    .select(
        F.col("air_quality_key"),
        F.col("date_key"),
        F.col("station_key"),
        F.col("region_key"),
        F.col("pm25_value"),
        F.col("pm10_value"),
        F.col("o3_value"),
        F.col("co_value"),
        F.col("no2_value"),
        F.col("so2_value"),
        F.col("overall_aqi"),
        F.col("aqi_param"),
        F.col("reading_hour"),
        F.col("reading_date"),
    )

fct_air.write.mode("overwrite") \
    .partitionBy("reading_date") \
    .parquet(f"{GOLD}fct_air_quality_hourly/")
print(f"fct_air_quality_hourly: {fct_air.count():,} rows")

# ── fct_pollution_daily_summary ───────────────────────────────────────────────
print("Building fct_pollution_daily_summary...")
fct_daily = air_df \
    .join(dim_station.select("station_key", "station_id"),
          on="station_id", how="left") \
    .join(dim_region.select("region_key", "region_name"),
          air_df.area_en == dim_region.region_name, how="left") \
    .groupBy("date_key", "station_key", "region_key", "reading_date") \
    .agg(
        F.round(F.avg("pm25_value"), 2).alias("avg_pm25"),
        F.max("pm25_value").alias("max_pm25"),
        F.min("pm25_value").alias("min_pm25"),
        F.sum(F.when(F.col("pm25_value") > 50, 1).otherwise(0))
         .alias("hours_exceeding_threshold"),
        F.first("aqi_param").alias("dominant_aqi_param"),
    ) \
    .withColumn("daily_summary_key",
                F.monotonically_increasing_id().cast(LongType()) + 1)

fct_daily.write.mode("overwrite") \
    .partitionBy("reading_date") \
    .parquet(f"{GOLD}fct_pollution_daily_summary/")
print(f"fct_pollution_daily_summary: {fct_daily.count():,} rows")

# ── fct_sensor_reading ────────────────────────────────────────────────────────
print("Building fct_sensor_reading...")
sensor_df = spark.read.parquet(SILVER_SENSOR)

fct_sensor = sensor_df \
    .join(dim_sensor.select("sensor_key", "sensor_id"),
          on="sensor_id", how="left") \
    .join(dim_zone.select("zone_key", "zone_id"),
          on="zone_id", how="left") \
    .withColumn("sensor_reading_key",
                F.monotonically_increasing_id().cast(LongType()) + 1) \
    .select(
        F.col("sensor_reading_key"),
        F.col("date_key"),
        F.col("sensor_key"),
        F.col("zone_key"),
        F.col("value"),
        F.col("target"),
        F.col("deviation"),
        F.col("is_anomaly"),
        F.col("reading_ts_utc"),
        F.col("reading_date"),
    )

fct_sensor.write.mode("overwrite") \
    .partitionBy("reading_date") \
    .parquet(f"{GOLD}fct_sensor_reading/")
print(f"fct_sensor_reading: {fct_sensor.count():,} rows")

print("All fact tables loaded successfully")
job.commit()
