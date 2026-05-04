"""
silver_to_gold_dim_sensor_scd2.py — SCD2 load for dim_sensor.
Same pattern as dim_station but for synthetic sensors.
"""

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import IntegerType
from datetime import date
import boto3

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]
SILVER_SENSOR = f"s3://{BUCKET}/silver/sensor_readings/"
GOLD_SENSOR = f"s3://{BUCKET}/gold/dim_sensor/"
TODAY = str(date.today())
FAR_FUTURE = "9999-12-31"

# Read latest sensor metadata from Silver
print("Reading Silver sensor data for sensor metadata...")
silver_df = spark.read.parquet(SILVER_SENSOR)

incoming = silver_df.select(
    F.col("sensor_id"),
    F.col("sensor_type"),
    F.col("zone_id"),
    F.col("target").alias("target_value"),
    F.col("unit"),
).distinct()

w = Window.partitionBy("sensor_id").orderBy(F.col("sensor_id"))
incoming = incoming.withColumn("rn", F.row_number().over(w)) \
                   .filter(F.col("rn") == 1).drop("rn")

print(f"Incoming sensors: {incoming.count()}")

# Check if dim_sensor exists
s3 = boto3.client("s3", region_name="ap-southeast-1")
prefix = "gold/dim_sensor/"

try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1)
    has_existing = resp.get("KeyCount", 0) > 0
except:
    has_existing = False

if not has_existing:
    print("First load — inserting all sensors as new records...")
    dim_sensor = incoming \
        .withColumn("sensor_key", F.monotonically_increasing_id().cast(IntegerType()) + 1) \
        .withColumn("effective_date", F.lit(TODAY)) \
        .withColumn("expiry_date", F.lit(FAR_FUTURE)) \
        .withColumn("is_current", F.lit(True))

    dim_sensor.write.mode("overwrite").parquet(GOLD_SENSOR)
    print(f"dim_sensor first load: {dim_sensor.count()} rows")

else:
    print("Subsequent load — performing SCD2 merge...")
    current_dim = spark.read.parquet(GOLD_SENSOR).filter(F.col("is_current") == True)
    historical_dim = spark.read.parquet(GOLD_SENSOR).filter(F.col("is_current") == False)

    new_sensors = incoming.join(
        current_dim.select("sensor_id"),
        on="sensor_id",
        how="left_anti"
    )

    max_key = current_dim.agg(F.max("sensor_key")).collect()[0][0] or 0

    if new_sensors.count() > 0:
        to_insert = new_sensors \
            .withColumn("sensor_key", (F.monotonically_increasing_id() + max_key + 1).cast(IntegerType())) \
            .withColumn("effective_date", F.lit(TODAY)) \
            .withColumn("expiry_date", F.lit(FAR_FUTURE)) \
            .withColumn("is_current", F.lit(True))

        final_dim = current_dim.union(historical_dim).union(to_insert)
    else:
        final_dim = current_dim.union(historical_dim)

    final_dim.write.mode("overwrite").parquet(GOLD_SENSOR)
    print(f"dim_sensor after merge: {final_dim.count()} rows")

print("dim_sensor SCD2 load complete")
job.commit()
