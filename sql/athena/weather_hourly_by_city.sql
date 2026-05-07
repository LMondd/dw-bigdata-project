-- Weather conditions by city — demonstrates OpenWeather source in Gold warehouse
-- Joins fct_weather_hourly with dim_weather_condition surrogate key
-- Use in DE video: proves all 3 sources (air quality, weather, IoT) reached Gold

SELECT
    w.city_name,
    w.reading_date,
    wc.weather_main,
    wc.weather_description,
    ROUND(AVG(w.temp_c), 1)        AS avg_temp_c,
    ROUND(AVG(w.humidity_pct), 0)  AS avg_humidity_pct,
    ROUND(AVG(w.wind_speed_ms), 1) AS avg_wind_ms,
    COUNT(*)                        AS hourly_readings
FROM gold_db.fct_weather_hourly w
JOIN gold_db.dim_weather_condition wc
    ON w.weather_condition_key = wc.weather_condition_key
GROUP BY
    w.city_name,
    w.reading_date,
    wc.weather_main,
    wc.weather_description
ORDER BY
    w.city_name,
    w.reading_date;
