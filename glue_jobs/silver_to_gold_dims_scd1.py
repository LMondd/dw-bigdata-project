"""
silver_to_gold_dims_scd1.py — Load all SCD1 dimensions to Gold.
Pattern: truncate-and-reload from Silver data.
Dims: dim_region, dim_zone, dim_weather_condition, dim_sensor_type
Run once, then on schedule.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
SILVER_AIR = f"s3://{BUCKET}/silver/air_quality/"
SILVER_WEATHER = f"s3://{BUCKET}/silver/weather/"
SILVER_SENSOR = f"s3://{BUCKET}/silver/sensor_readings/"
GOLD = f"s3://{BUCKET}/gold/"

# ── dim_region ────────────────────────────────────────────────────────────────
print("Building dim_region...")
air_df = spark.read.parquet(SILVER_AIR)

dim_region = air_df.select(
    F.col("area_en").alias("region_name")
).filter(F.col("region_name").isNotNull()) \
 .distinct() \
 .withColumn("region_key", F.monotonically_increasing_id().cast(IntegerType()) + 1) \
 .withColumn("country", F.lit("Thailand"))

dim_region.write.mode("overwrite").parquet(f"{GOLD}dim_region/")
print(f"dim_region: {dim_region.count()} rows")

# ── dim_weather_condition ─────────────────────────────────────────────────────
print("Building dim_weather_condition...")
weather_df = spark.read.parquet(SILVER_WEATHER)

dim_weather = weather_df.select(
    F.col("weather_main"),
    F.col("weather_desc").alias("weather_description")
).filter(F.col("weather_main").isNotNull()) \
 .distinct() \
 .withColumn("weather_condition_key", F.monotonically_increasing_id().cast(IntegerType()) + 1)

dim_weather.write.mode("overwrite").parquet(f"{GOLD}dim_weather_condition/")
print(f"dim_weather_condition: {dim_weather.count()} rows")

# ── dim_zone ──────────────────────────────────────────────────────────────────
print("Building dim_zone...")
sensor_df = spark.read.parquet(SILVER_SENSOR)

ZONE_NAMES = {
    "Z01": "Production A",
    "Z02": "Production B",
    "Z03": "Warehouse",
    "Z04": "Utilities",
    "Z05": "Office",
}

dim_zone = sensor_df.select(
    F.col("zone_id")
).distinct() \
 .withColumn("zone_key", F.monotonically_increasing_id().cast(IntegerType()) + 1) \
 .withColumn("zone_name", F.col("zone_id")) \
 .withColumn("description", F.lit("Bangkok facility zone"))

dim_zone.write.mode("overwrite").parquet(f"{GOLD}dim_zone/")
print(f"dim_zone: {dim_zone.count()} rows")

# ── dim_sensor_type ───────────────────────────────────────────────────────────
print("Building dim_sensor_type...")
dim_sensor_type = sensor_df.select(
    F.col("sensor_type"),
    F.col("unit")
).distinct() \
 .withColumn("sensor_type_key", F.monotonically_increasing_id().cast(IntegerType()) + 1) \
 .withColumn("typical_range_min", F.lit(0.0)) \
 .withColumn("typical_range_max", F.lit(999.0))

dim_sensor_type.write.mode("overwrite").parquet(f"{GOLD}dim_sensor_type/")
print(f"dim_sensor_type: {dim_sensor_type.count()} rows")

print("All SCD1 dims loaded successfully")
job.commit()
