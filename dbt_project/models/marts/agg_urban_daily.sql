-- agg_urban_daily.sql
-- Cross-source urban intelligence daily aggregation table

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

WITH traffic_daily AS (
    SELECT
        date_key,
        AVG(avg_traffic_volume)::DOUBLE     AS avg_daily_traffic,
        SUM(total_traffic_volume)::BIGINT   AS total_daily_traffic,
        MAX(max_traffic_volume)             AS peak_traffic_volume,
        MODE(traffic_level)                 AS dominant_traffic_level,
        COUNT(*)                            AS traffic_hours_measured
    FROM {{ ref('fact_traffic_hourly') }}
    GROUP BY 1
),

air_daily AS (
    SELECT
        date_key,
        AVG(avg_aqi)::DOUBLE                AS avg_daily_aqi,
        MAX(max_aqi)                        AS peak_aqi,
        SUM(hazardous_hours_count)          AS total_hazardous_hours,
        COUNT(DISTINCT city)                AS cities_measured
    FROM {{ ref('fact_air_quality_daily') }}
    GROUP BY 1
),

events_daily AS (
    SELECT
        DATE_TRUNC('day', hour_key)::DATE   AS date_key,
        SUM(event_count)::BIGINT            AS total_stream_events,
        SUM(unique_sensors)                 AS sensors_active,
        COUNT(DISTINCT event_category)      AS event_categories
    FROM {{ source('gold', 'fact_stream_events_hourly') }}
    GROUP BY 1
)

SELECT
    COALESCE(t.date_key, a.date_key, e.date_key)    AS date_key,
    d.day_name,
    d.is_weekend,
    d.season,
    d.month_name,

    -- Traffic metrics
    COALESCE(t.avg_daily_traffic, 0)                AS avg_daily_traffic,
    COALESCE(t.total_daily_traffic, 0)              AS total_daily_traffic,
    COALESCE(t.peak_traffic_volume, 0)              AS peak_traffic_volume,
    COALESCE(t.dominant_traffic_level, 'Unknown')   AS traffic_level,
    COALESCE(t.traffic_hours_measured, 0)           AS traffic_hours_measured,

    -- Air quality metrics
    COALESCE(a.avg_daily_aqi, 0)                    AS avg_daily_aqi,
    COALESCE(a.peak_aqi, 0)                         AS peak_aqi,
    COALESCE(a.total_hazardous_hours, 0)            AS total_hazardous_hours,
    COALESCE(a.cities_measured, 0)                  AS cities_measured,

    -- Streaming metrics
    COALESCE(e.total_stream_events, 0)              AS total_stream_events,
    COALESCE(e.sensors_active, 0)                   AS sensors_active,

    -- Composite urban health score (0-100)
    ROUND(GREATEST(0, LEAST(100,
        100
        - (COALESCE(a.avg_daily_aqi, 0) / 10)       -- AQI penalty
        + (CASE WHEN d.is_weekend THEN 5 ELSE 0 END) -- Weekend bonus
    )), 1)                                           AS urban_health_score,

    CURRENT_TIMESTAMP                               AS dbt_updated_at
FROM traffic_daily t
FULL OUTER JOIN air_daily a
    ON t.date_key = a.date_key
FULL OUTER JOIN events_daily e
    ON COALESCE(t.date_key, a.date_key) = e.date_key
LEFT JOIN {{ ref('dim_date') }} d
    ON COALESCE(t.date_key, a.date_key, e.date_key) = d.date_key
WHERE COALESCE(t.date_key, a.date_key, e.date_key) IS NOT NULL
ORDER BY date_key
