-- fact_traffic_hourly.sql
-- Hourly traffic fact table: aggregated measurements per hour

{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key='hour_key',
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    event_hour                                          AS hour_key,
    event_date                                          AS date_key,
    hour_of_day,
    day_of_week,
    is_weekend,
    weather_condition,
    ROUND(AVG(temperature_celsius), 2)                  AS avg_temperature_celsius,
    ROUND(AVG(rainfall_mm), 3)                          AS avg_rainfall_mm,
    ROUND(AVG(snowfall_mm), 3)                          AS avg_snowfall_mm,
    ROUND(AVG(traffic_volume), 0)::INT                  AS avg_traffic_volume,
    MAX(traffic_volume)                                 AS max_traffic_volume,
    MIN(traffic_volume)                                 AS min_traffic_volume,
    SUM(traffic_volume)                                 AS total_traffic_volume,
    COUNT(*)                                            AS measurement_count,
    ROUND(STDDEV(traffic_volume), 2)                    AS traffic_stddev,
    CASE
        WHEN AVG(traffic_volume) < 1000 THEN 'Low'
        WHEN AVG(traffic_volume) < 3000 THEN 'Medium'
        WHEN AVG(traffic_volume) < 5000 THEN 'High'
        ELSE 'Very High'
    END                                                 AS traffic_level,
    CURRENT_TIMESTAMP                                   AS dbt_updated_at
FROM {{ ref('stg_traffic') }}

{% if is_incremental() %}
WHERE event_hour > (SELECT MAX(hour_key) FROM {{ this }})
{% endif %}

GROUP BY 1,2,3,4,5,6
