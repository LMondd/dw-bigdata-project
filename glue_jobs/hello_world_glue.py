"""
Hello-world Glue job.
Input:  s3://<bucket>/bronze/test/test.csv
Output: s3://<bucket>/silver/test/ (Parquet)
Purpose: Verify Glue + S3 plumbing works end-to-end.
"""

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.types import StructType, StructField, StringType, FloatType

args = getResolvedOptions(sys.argv, ["JOB_NAME", "bucket_name"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

BUCKET = args["bucket_name"]

# Define schema explicitly — never rely on inference in production
schema = StructType([
    StructField("station_id", StringType(), True),
    StructField("station_name", StringType(), True),
    StructField("pm25", FloatType(), True),
    StructField("reading_time", StringType(), True),
])

# Read CSV from Bronze
df = spark.read.schema(schema).option("header", True).csv(
    f"s3://{BUCKET}/bronze/test/test.csv"
)

print(f"Row count: {df.count()}")
df.show()

# Write Parquet to Silver
df.coalesce(1).write.mode("overwrite").parquet(
    f"s3://{BUCKET}/silver/test/"
)

print("Hello-world Glue job completed successfully.")
job.commit()
