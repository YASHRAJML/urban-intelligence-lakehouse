-- stg_traffic.sql
-- Staging model for urban traffic data from bronze layer

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

SELECT
    event_timestamp,
    DATE_TRUNC('hour', event_timestamp)         AS event_hour,
    DATE_TRUNC('day',  event_timestamp)         AS event_date,
    EXTRACT(hour FROM event_timestamp)::INT     AS hour_of_day,
    EXTRACT(dow  FROM event_timestamp)::INT     AS day_of_week,
    CASE WHEN EXTRACT(dow FROM event_timestamp) IN (0,6)
         THEN TRUE ELSE FALSE END               AS is_weekend,
    COALESCE(holiday_name, 'None')              AS holiday_name,
    COALESCE(temperature_kelvin - 273.15, 20.0) AS temperature_celsius,
    COALESCE(rainfall_mm, 0.0)                  AS rainfall_mm,
    COALESCE(snowfall_mm, 0.0)                  AS snowfall_mm,
    COALESCE(cloud_coverage_pct, 0)             AS cloud_coverage_pct,
    UPPER(TRIM(weather_condition))              AS weather_condition,
    traffic_volume,
    _ingestion_timestamp,
    _source_id,
    _batch_id
FROM {{ source('bronze', 'bronze_urban_traffic') }}
WHERE traffic_volume IS NOT NULL
  AND traffic_volume BETWEEN 0 AND 100000
  AND event_timestamp IS NOT NULL
