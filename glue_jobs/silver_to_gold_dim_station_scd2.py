"""
silver_to_gold_dim_station_scd2.py — SCD2 load for dim_station.
Pattern:
  1. Read current dim_station from Gold
  2. Read latest station metadata from Silver
  3. Identify new and changed stations
  4. Close old records (expiry_date, is_current=false)
  5. Insert new records with new surrogate keys
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
from datetime import date

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
SILVER_AIR = f"s3://{BUCKET}/silver/air_quality/"
GOLD_STATION = f"s3://{BUCKET}/gold/dim_station/"
TODAY = str(date.today())
FAR_FUTURE = "9999-12-31"

# ── Read latest station metadata from Silver ──────────────────────────────────
print("Reading Silver air quality for station metadata...")
silver_df = spark.read.parquet(SILVER_AIR)

incoming = silver_df.select(
    F.col("station_id"),
    F.col("station_name_en"),
    F.col("station_name_th"),
    F.col("area_en").alias("area_en"),
    F.col("station_type"),
    F.col("lat"),
    F.col("lon"),
).distinct()

# Keep only latest record per station
from pyspark.sql import Window
w = Window.partitionBy("station_id").orderBy(F.col("station_id"))
incoming = incoming.withColumn("rn", F.row_number().over(w)) \
                   .filter(F.col("rn") == 1).drop("rn")

print(f"Incoming stations: {incoming.count()}")

# ── Check if dim_station already exists ───────────────────────────────────────
import boto3
s3 = boto3.client("s3", region_name="ap-southeast-1")
prefix = "gold/dim_station/"

try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    has_existing = resp.get("KeyCount", 0) > 0
except:
    has_existing = False

if not has_existing:
    # First load — insert all as new current records
    print("First load — inserting all stations as new records...")
    dim_station = incoming \
        .withColumn("station_key", F.monotonically_increasing_id().cast(IntegerType()) + 1) \
        .withColumn("effective_date", F.lit(TODAY)) \
        .withColumn("expiry_date", F.lit(FAR_FUTURE)) \
        .withColumn("is_current", F.lit(True))

    dim_station.write.mode("overwrite").parquet(GOLD_STATION)
    print(f"dim_station first load: {dim_station.count()} rows")

else:
    # Subsequent load — SCD2 merge
    print("Subsequent load — performing SCD2 merge...")
    # Cache the existing dim so Spark doesn't try to re-read S3 during the
    # overwrite write (which deletes the source files first).
    existing = spark.read.parquet(GOLD_STATION).cache()
    existing.count()
    current_dim = existing.filter(F.col("is_current") == True)
    historical_dim = existing.filter(F.col("is_current") == False)

    # Find changed stations: rename current-side columns so the join doesn't
    # produce two columns named "station_name_en" (which Spark rejects as
    # ambiguous), then compare incoming vs current with null-safe equality.
    current_subset = (
        current_dim.select("station_id", "station_name_en", "area_en", "lat", "lon")
        .withColumnRenamed("station_name_en", "cur_station_name_en")
        .withColumnRenamed("area_en", "cur_area_en")
        .withColumnRenamed("lat", "cur_lat")
        .withColumnRenamed("lon", "cur_lon")
    )
    joined = incoming.join(current_subset, on="station_id", how="inner")

    changed = joined.filter(
        (~F.col("station_name_en").eqNullSafe(F.col("cur_station_name_en"))) |
        (~F.col("area_en").eqNullSafe(F.col("cur_area_en"))) |
        (~F.col("lat").eqNullSafe(F.col("cur_lat"))) |
        (~F.col("lon").eqNullSafe(F.col("cur_lon")))
    ).select(incoming.columns)

    # New stations not in current dim
    new_stations = incoming.join(
        current_dim.select("station_id"),
        on="station_id",
        how="left_anti"
    )

    # Close old records for changed stations
    closed = current_dim.join(
        changed.select("station_id"),
        on="station_id",
        how="inner"
    ).withColumn("expiry_date", F.lit(TODAY)) \
     .withColumn("is_current", F.lit(False))

    unchanged = current_dim.join(
        changed.select("station_id"),
        on="station_id",
        how="left_anti"
    )

    # Get max surrogate key
    max_key = current_dim.agg(F.max("station_key")).collect()[0][0] or 0

    # New records for changed + new stations
    to_insert = changed.union(new_stations) \
        .withColumn("station_key", (F.monotonically_increasing_id() + max_key + 1).cast(IntegerType())) \
        .withColumn("effective_date", F.lit(TODAY)) \
        .withColumn("expiry_date", F.lit(FAR_FUTURE)) \
        .withColumn("is_current", F.lit(True))

    # Combine all and force materialization before overwriting source.
    final_dim = unchanged.union(closed).union(historical_dim).union(to_insert).cache()
    row_count = final_dim.count()
    final_dim.write.mode("overwrite").parquet(GOLD_STATION)
    print(f"dim_station after SCD2 merge: {row_count} rows")

print("dim_station SCD2 load complete")
job.commit()
