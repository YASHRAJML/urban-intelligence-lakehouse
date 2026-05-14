-- stg_stream_events.sql
-- Staging model for Kafka streaming events

{{
    config(
        materialized='view',
        schema='staging'
    )
}}

SELECT
    event_id,
    UPPER(TRIM(event_type))                     AS event_type,
    sensor_id,
    location_id,
    ROUND(CAST(latitude  AS DOUBLE), 6)         AS latitude,
    ROUND(CAST(longitude AS DOUBLE), 6)         AS longitude,
    CAST(event_timestamp AS TIMESTAMPTZ)        AS event_timestamp,
    DATE_TRUNC('minute', CAST(event_timestamp AS TIMESTAMPTZ)) AS event_minute,
    DATE_TRUNC('hour',   CAST(event_timestamp AS TIMESTAMPTZ)) AS event_hour,
    ROUND(CAST(metric_value AS DOUBLE), 4)      AS metric_value,
    TRIM(metric_unit)                           AS metric_unit,
    kafka_topic,
    kafka_partition,
    kafka_offset,
    CASE
        WHEN event_type ILIKE '%traffic%' OR event_type ILIKE '%vehicle%' THEN 'TRAFFIC'
        WHEN event_type ILIKE '%pedestrian%'                               THEN 'PEDESTRIAN'
        WHEN event_type ILIKE '%air%' OR event_type ILIKE '%quality%'     THEN 'AIR_QUALITY'
        WHEN event_type ILIKE '%order%'                                    THEN 'ORDER'
        ELSE 'OTHER'
    END                                         AS event_category,
    _ingestion_timestamp,
    _source_id,
    _batch_id
FROM {{ source('bronze', 'bronze_urban_stream_events') }}
WHERE event_id IS NOT NULL
  AND event_type IS NOT NULL
