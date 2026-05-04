# Dimensional Model Design

## Business Questions (Step 26)

### Cold Path — Air Quality Analytics
1. What is the average PM2.5 by district by month?
2. Which stations exceeded the safe PM2.5 threshold (>50 µg/m³) most frequently?
3. How does air quality correlate with weather conditions (humidity, wind speed)?
4. What time of day has the worst air quality in Bangkok?
5. Which pollutant (PM2.5, PM10, O3, NO2) drives the overall AQI most often?

### Hot Path — Sensor Analytics
6. Which zones have the highest anomaly rates over the past 7 days?
7. What is the average temperature trend across zones over the past 30 days?
8. Which sensors have drifted furthest from their target values?
9. How many anomaly events occurred per sensor type per day?
10. What is the correlation between pressure and flow readings by zone?

---

## Step 27: Kimball 4-Step Method

### Business Process 1: Air Quality Monitoring
- **Process:** Hourly air quality readings from monitoring stations
- **Grain:** One row per station per hour
- **Dimensions:** dim_date, dim_station, dim_region, dim_weather_condition
- **Facts:** pm25_value, pm10_value, o3_value, co_value, no2_value, overall_aqi

### Business Process 2: Daily Pollution Summary
- **Process:** Daily aggregation of air quality (different grain from above)
- **Grain:** One row per station per day
- **Dimensions:** dim_date, dim_station, dim_region
- **Facts:** avg_pm25, max_pm25, min_pm25, hours_exceeding_threshold

### Business Process 3: IoT Sensor Reading
- **Process:** 5-second sensor readings from facility sensors
- **Grain:** One row per sensor per 5-second tick
- **Dimensions:** dim_date, dim_sensor, dim_zone, dim_sensor_type
- **Facts:** value, target, deviation_from_target, is_anomaly

### Business Process 4: Sensor Anomaly Event
- **Process:** Each detected anomaly from the hot path
- **Grain:** One row per anomaly event
- **Dimensions:** dim_date, dim_sensor, dim_zone
- **Facts:** anomaly_value, expected_value, deviation_sigma

---

## Step 28: Star Schema

### Fact Tables

#### fct_air_quality_hourly
| Column | Type | Description |
|---|---|---|
| air_quality_key | BIGINT | Surrogate key (PK) |
| date_key | INT | FK to dim_date |
| station_key | INT | FK to dim_station |
| region_key | INT | FK to dim_region |
| weather_condition_key | INT | FK to dim_weather_condition |
| pm25_value | DOUBLE | PM2.5 reading µg/m³ |
| pm10_value | DOUBLE | PM10 reading µg/m³ |
| o3_value | DOUBLE | Ozone reading ppb |
| co_value | DOUBLE | CO reading ppm |
| no2_value | DOUBLE | NO2 reading ppb |
| so2_value | DOUBLE | SO2 reading ppb |
| overall_aqi | INT | Overall AQI score |
| aqi_param | STRING | Pollutant driving AQI |
| reading_hour | INT | Hour of reading (0-23) |

#### fct_pollution_daily_summary
| Column | Type | Description |
|---|---|---|
| daily_summary_key | BIGINT | Surrogate key (PK) |
| date_key | INT | FK to dim_date |
| station_key | INT | FK to dim_station |
| region_key | INT | FK to dim_region |
| avg_pm25 | DOUBLE | Daily average PM2.5 |
| max_pm25 | DOUBLE | Daily max PM2.5 |
| min_pm25 | DOUBLE | Daily min PM2.5 |
| hours_exceeding_threshold | INT | Hours where PM2.5 > 50 |
| dominant_aqi_param | STRING | Most common AQI driver |

#### fct_sensor_reading
| Column | Type | Description |
|---|---|---|
| sensor_reading_key | BIGINT | Surrogate key (PK) |
| date_key | INT | FK to dim_date |
| sensor_key | INT | FK to dim_sensor |
| zone_key | INT | FK to dim_zone |
| sensor_type_key | INT | FK to dim_sensor_type |
| value | DOUBLE | Sensor reading |
| target | DOUBLE | Expected target value |
| deviation | DOUBLE | value - target |
| is_anomaly | BOOLEAN | Anomaly flag |
| reading_ts | TIMESTAMP | Exact reading timestamp |

#### fct_sensor_anomaly_event
| Column | Type | Description |
|---|---|---|
| anomaly_key | BIGINT | Surrogate key (PK) |
| date_key | INT | FK to dim_date |
| sensor_key | INT | FK to dim_sensor |
| zone_key | INT | FK to dim_zone |
| anomaly_value | DOUBLE | Actual anomaly value |
| expected_value | DOUBLE | Target value |
| deviation | DOUBLE | How far from target |
| detected_at | TIMESTAMP | When anomaly was detected |

---

### Dimension Tables

#### dim_date (Static)
| Column | Type | Description |
|---|---|---|
| date_key | INT | PK e.g. 20260101 |
| full_date | DATE | |
| year | INT | |
| quarter | INT | 1-4 |
| month | INT | 1-12 |
| month_name | STRING | January etc |
| day_of_month | INT | |
| day_of_week | INT | 1=Monday |
| day_name | STRING | Monday etc |
| is_weekend | BOOLEAN | |
| is_thai_holiday | BOOLEAN | |

#### dim_station (SCD2)
| Column | Type | Description |
|---|---|---|
| station_key | INT | Surrogate PK |
| station_id | STRING | Natural key |
| station_name_en | STRING | |
| station_name_th | STRING | |
| area_en | STRING | |
| region_key | INT | FK to dim_region |
| station_type | STRING | |
| lat | DOUBLE | |
| lon | DOUBLE | |
| effective_date | DATE | SCD2 start |
| expiry_date | DATE | SCD2 end (null=current) |
| is_current | BOOLEAN | |

#### dim_sensor (SCD2)
| Column | Type | Description |
|---|---|---|
| sensor_key | INT | Surrogate PK |
| sensor_id | STRING | Natural key |
| sensor_type | STRING | |
| zone_id | STRING | |
| target_value | DOUBLE | |
| unit | STRING | |
| effective_date | DATE | SCD2 start |
| expiry_date | DATE | SCD2 end |
| is_current | BOOLEAN | |

#### dim_zone (SCD1)
| Column | Type | Description |
|---|---|---|
| zone_key | INT | Surrogate PK |
| zone_id | STRING | Natural key |
| zone_name | STRING | |
| description | STRING | |

#### dim_region (SCD1)
| Column | Type | Description |
|---|---|---|
| region_key | INT | Surrogate PK |
| region_name | STRING | |
| province | STRING | |
| country | STRING | |

#### dim_weather_condition (SCD1)
| Column | Type | Description |
|---|---|---|
| weather_condition_key | INT | Surrogate PK |
| weather_main | STRING | e.g. Clouds, Rain |
| weather_description | STRING | |

#### dim_sensor_type (SCD1)
| Column | Type | Description |
|---|---|---|
| sensor_type_key | INT | Surrogate PK |
| sensor_type | STRING | temperature/pressure etc |
| unit | STRING | |
| typical_range_min | DOUBLE | |
| typical_range_max | DOUBLE | |

---

## Step 29: Conformed Bus Matrix

| Business Process | dim_date | dim_station | dim_region | dim_sensor | dim_zone | dim_sensor_type | dim_weather_condition |
|---|---|---|---|---|---|---|---|
| fct_air_quality_hourly | ✓ | ✓ | ✓ | | | | ✓ |
| fct_pollution_daily_summary | ✓ | ✓ | ✓ | | | | |
| fct_sensor_reading | ✓ | | | ✓ | ✓ | ✓ | |
| fct_sensor_anomaly_event | ✓ | | | ✓ | ✓ | | |

**Conformed dimensions:**
- dim_date is shared across ALL 4 fact tables
- dim_station is shared across both air quality facts
- dim_sensor is shared across both sensor facts
- dim_zone is shared across both sensor facts

---

## Step 30: SCD Type Decisions

| Dimension | SCD Type | Reasoning |
|---|---|---|
| dim_date | Static | Generated once, never changes |
| dim_station | **SCD2** | Stations get recalibrated and relocated over time. History matters for fair time-series comparison — a station moved to a different location should not pollute historical trends |
| dim_sensor | **SCD2** | Sensors get replaced/relocated. Need history to compare readings before and after replacement |
| dim_zone | SCD1 | Administrative groupings rarely change. No historical comparison needed |
| dim_region | SCD1 | Provincial boundaries rarely change. SCD1 sufficient |
| dim_weather_condition | SCD1 | Simple lookup table, no history needed |
| dim_sensor_type | SCD1 | Static reference data |
