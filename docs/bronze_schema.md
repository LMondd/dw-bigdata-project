# Bronze Schema Design

> Bronze is append-only raw data. Minimal transformation — 
> just flatten JSON and add ingestion metadata.

## air_quality (from Air4Thai)

| Column | Type | Source field | Notes |
|---|---|---|---|
| station_id | STRING | stationID | Partition key |
| station_name_en | STRING | nameEN | |
| station_name_th | STRING | nameTH | |
| area_en | STRING | areaEN | |
| area_th | STRING | areaTH | |
| station_type | STRING | stationType | |
| lat | FLOAT | lat | Cast from string |
| lon | FLOAT | long | Cast from string |
| reading_date | STRING | AQILast.date | |
| reading_time | STRING | AQILast.time | |
| pm25_value | FLOAT | AQILast.PM25.value | -1/-999 → null |
| pm25_aqi | INT | AQILast.PM25.aqi | -1 → null |
| pm10_value | FLOAT | AQILast.PM10.value | -1/-999 → null |
| o3_value | FLOAT | AQILast.O3.value | -1/-999 → null |
| co_value | FLOAT | AQILast.CO.value | -1/-999 → null |
| no2_value | FLOAT | AQILast.NO2.value | -1/-999 → null |
| so2_value | FLOAT | AQILast.SO2.value | -1/-999 → null |
| overall_aqi | INT | AQILast.AQI.aqi | |
| aqi_param | STRING | AQILast.AQI.param | Which pollutant drives AQI |
| ingested_at | TIMESTAMP | added by producer | UTC ingestion time |

## weather (from OpenWeather)

| Column | Type | Source field | Notes |
|---|---|---|---|
| city_id | INT | id | Partition key |
| city_name | STRING | name | |
| country | STRING | sys.country | |
| lat | FLOAT | coord.lat | |
| lon | FLOAT | coord.lon | |
| reading_ts | INT | dt | Unix timestamp UTC |
| temp_c | FLOAT | main.temp | |
| feels_like_c | FLOAT | main.feels_like | |
| pressure_hpa | INT | main.pressure | |
| humidity_pct | INT | main.humidity | |
| wind_speed_ms | FLOAT | wind.speed | |
| wind_deg | INT | wind.deg | |
| visibility_m | INT | visibility | |
| weather_main | STRING | weather[0].main | |
| weather_desc | STRING | weather[0].description | |
| weather_icon | STRING | weather[0].icon | |
| ingested_at | TIMESTAMP | added by producer | UTC ingestion time |
