-- stg_air_quality.sql
-- Staging model for air quality data

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

SELECT
    UPPER(TRIM(city))                           AS city,
    CAST(measurement_timestamp AS TIMESTAMPTZ) AS measurement_timestamp,
    DATE_TRUNC('hour', measurement_timestamp)  AS measurement_hour,
    DATE_TRUNC('day',  measurement_timestamp)  AS measurement_date,
    COALESCE(pm25, 0.0)                        AS pm25,
    COALESCE(pm10, 0.0)                        AS pm10,
    COALESCE(aqi, 0.0)                         AS aqi,
    COALESCE(aqi_category, 'Unknown')          AS aqi_category,
    COALESCE(ozone, 0.0)                       AS ozone,
    COALESCE(carbon_monoxide, 0.0)             AS carbon_monoxide,
    COALESCE(sulfur_dioxide, 0.0)              AS sulfur_dioxide,
    COALESCE(nitrogen_dioxide, 0.0)            AS nitrogen_dioxide,
    CASE
        WHEN aqi > 300 THEN TRUE ELSE FALSE
    END                                        AS is_hazardous,
    _ingestion_timestamp,
    _source_id
FROM {{ source('bronze', 'bronze_air_quality') }}
WHERE city IS NOT NULL
  AND measurement_timestamp IS NOT NULL
