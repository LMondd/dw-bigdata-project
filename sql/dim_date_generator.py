"""
dim_date_generator.py — Generate dim_date table for years 2020-2030.
Run once to populate Gold layer with date dimension.
Output: s3://<bucket>/gold/dim_date/
"""

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

S3_BUCKET = os.environ["S3_BUCKET"]

THAI_HOLIDAYS_2026 = {
    "2026-01-01", "2026-02-03", "2026-04-06",
    "2026-04-13", "2026-04-14", "2026-04-15",
    "2026-05-01", "2026-05-04", "2026-06-03",
    "2026-07-28", "2026-08-12", "2026-10-13",
    "2026-10-23", "2026-12-05", "2026-12-10",
    "2026-12-31",
}


def generate_dates(start: date, end: date) -> list[dict]:
    rows = []
    current = start
    while current <= end:
        date_key = int(current.strftime("%Y%m%d"))
        rows.append({
            "date_key": date_key,
            "full_date": current.isoformat(),
            "year": current.year,
            "quarter": (current.month - 1) // 3 + 1,
            "month": current.month,
            "month_name": current.strftime("%B"),
            "day_of_month": current.day,
            "day_of_week": current.isoweekday(),
            "day_name": current.strftime("%A"),
            "is_weekend": current.isoweekday() >= 6,
            "is_thai_holiday": current.isoformat() in THAI_HOLIDAYS_2026,
        })
        current += timedelta(days=1)
    return rows


def main():
    print("Generating dim_date 2020-2030...")
    rows = generate_dates(date(2020, 1, 1), date(2030, 12, 31))
    print(f"Generated {len(rows):,} date rows")

    table = pa.Table.from_pylist(rows)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")

    s3_client = boto3.client("s3", region_name="ap-southeast-1")
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key="gold/dim_date/dim_date.parquet",
        Body=buf.getvalue().to_pybytes(),
    )
    print("Done — dim_date written to s3://gold/dim_date/")


if __name__ == "__main__":
    main()
