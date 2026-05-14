-- fact_air_quality_daily.sql
-- Daily air quality fact table per city

{{
    config(
        materialized='incremental',
        schema='gold',
        unique_key=['city', 'date_key'],
        on_schema_change='sync_all_columns'
    )
}}

SELECT
    city,
    measurement_date                            AS date_key,
    ROUND(AVG(aqi), 2)                          AS avg_aqi,
    ROUND(MAX(aqi), 2)                          AS max_aqi,
    ROUND(MIN(aqi), 2)                          AS min_aqi,
    ROUND(AVG(pm25), 2)                         AS avg_pm25,
    ROUND(MAX(pm25), 2)                         AS max_pm25,
    ROUND(AVG(pm10), 2)                         AS avg_pm10,
    ROUND(AVG(ozone), 2)                        AS avg_ozone,
    ROUND(AVG(carbon_monoxide), 3)              AS avg_carbon_monoxide,
    ROUND(AVG(sulfur_dioxide), 2)               AS avg_sulfur_dioxide,
    ROUND(AVG(nitrogen_dioxide), 2)             AS avg_nitrogen_dioxide,
    COUNT(*)                                    AS measurement_count,
    SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) AS hazardous_hours_count,
    ROUND(
        SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                           AS hazardous_hours_pct,
    MODE(aqi_category)                          AS dominant_aqi_category,
    CURRENT_TIMESTAMP                           AS dbt_updated_at
FROM {{ ref('stg_air_quality') }}

{% if is_incremental() %}
WHERE measurement_date > (SELECT MAX(date_key) FROM {{ this }})
{% endif %}

GROUP BY 1, 2
