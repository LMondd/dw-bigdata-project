# Bronze Schema

> Raw data as received from each source, minimally typed, partitioned by date/hour.
> TODO: fill in actual schemas after exploring each API.

## air_quality (from Air4Thai)

S3 path: `s3://<bucket>/bronze/air_quality/date=YYYY-MM-DD/hour=HH/`

Schema: TODO

## weather (from OpenWeather)

S3 path: `s3://<bucket>/bronze/weather/date=YYYY-MM-DD/hour=HH/`

Schema: TODO

## sensor_readings (from synthetic generator)

S3 path: `s3://<bucket>/bronze/sensor_readings/date=YYYY-MM-DD/hour=HH/`

Schema: TODO
