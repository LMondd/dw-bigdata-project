"""
silver_to_gold_fct_weather_hourly.py

Input:  s3://<bucket>/silver/weather/        — cleaned hourly weather readings
        s3://<bucket>/gold/dim_date/          — date surrogate keys
        s3://<bucket>/gold/dim_weather_condition/ — weather condition surrogate keys
Output: s3://<bucket>/gold/fct_weather_hourly/ — one row per city per hour

Grain: one reading per city per hourly API pull.
Conformed dim: dim_date (shared with all other fact tables).
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
SILVER_WEATHER = f"s3://{BUCKET}/silver/weather/"
GOLD = f"s3://{BUCKET}/gold/"

print("Reading Silver weather...")
weather_df = spark.read.parquet(SILVER_WEATHER)
print(f"Silver weather rows: {weather_df.count():,}")

print("Reading Gold dims...")
dim_date = spark.read.parquet(f"{GOLD}dim_date/")
dim_weather_cond = spark.read.parquet(f"{GOLD}dim_weather_condition/")

fct_weather = (
    weather_df
    .join(dim_date.select("date_key"), on="date_key", how="left")
    .join(
        dim_weather_cond.select("weather_condition_key", "weather_main"),
        on="weather_main",
        how="left",
    )
    .withColumn(
        "weather_hourly_key",
        F.monotonically_increasing_id().cast(LongType()) + 1,
    )
    .select(
        F.col("weather_hourly_key"),
        F.col("date_key"),
        F.col("weather_condition_key"),
        F.col("city_id"),
        F.col("city_name"),
        F.col("temp_c"),
        F.col("feels_like_c"),
        F.col("pressure_hpa"),
        F.col("humidity_pct"),
        F.col("wind_speed_ms"),
        F.col("wind_deg"),
        F.col("visibility_m"),
        F.col("reading_ts_utc"),
        F.col("reading_date"),
    )
)

row_count = fct_weather.count()
print(f"fct_weather_hourly rows: {row_count:,}")

fct_weather.coalesce(4).write.mode("overwrite").parquet(f"{GOLD}fct_weather_hourly/")
print("fct_weather_hourly written to Gold.")
job.commit()
