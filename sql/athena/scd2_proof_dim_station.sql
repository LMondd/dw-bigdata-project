-- SCD2 proof: show historical (expired) station records
-- is_current = false means this row was superseded by a newer version
-- effective_date + expiry_date show the period this version was active

SELECT
    station_id,
    station_name_en,
    area_en,
    effective_date,
    expiry_date,
    is_current
FROM gold_db.dim_station
WHERE is_current = false
ORDER BY station_id, effective_date;

-- Companion: show current + historical side by side for the same station
-- (uncomment and replace '02t' with a station_id that has history)
/*
SELECT
    station_id,
    station_name_en,
    area_en,
    effective_date,
    expiry_date,
    is_current
FROM gold_db.dim_station
WHERE station_id = '02t'
ORDER BY effective_date;
*/
