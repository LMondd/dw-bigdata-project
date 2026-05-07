-- Top 10 most polluted stations by average PM2.5
-- Source: fct_air_quality_hourly + dim_station (Gold layer)
-- Use in video: demonstrates Athena querying the Kimball warehouse

SELECT
    s.station_name_en,
    s.area_en,
    ROUND(AVG(h.pm25_value), 2)  AS avg_pm25,
    ROUND(MAX(h.pm25_value), 2)  AS max_pm25,
    COUNT(*)                      AS hourly_readings
FROM gold_db.fct_air_quality_hourly h
JOIN gold_db.dim_station s
    ON h.station_key = s.station_key AND s.is_current = true
WHERE h.pm25_value IS NOT NULL
GROUP BY s.station_name_en, s.area_en
ORDER BY avg_pm25 DESC
LIMIT 10;
