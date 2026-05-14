-- dim_date.sql
-- Complete date dimension table from 2020 to 2026

{{
    config(
        materialized='table',
        schema='gold'
    )
}}

SELECT
    CAST(d AS DATE)                                     AS date_key,
    EXTRACT(year    FROM d)::INT                        AS year,
    EXTRACT(month   FROM d)::INT                        AS month_num,
    EXTRACT(day     FROM d)::INT                        AS day_num,
    EXTRACT(dow     FROM d)::INT                        AS day_of_week,     -- 0=Sun
    EXTRACT(doy     FROM d)::INT                        AS day_of_year,
    EXTRACT(week    FROM d)::INT                        AS week_of_year,
    EXTRACT(quarter FROM d)::INT                        AS quarter,
    STRFTIME(d, '%A')                                   AS day_name,
    STRFTIME(d, '%a')                                   AS day_name_short,
    STRFTIME(d, '%B')                                   AS month_name,
    STRFTIME(d, '%b')                                   AS month_name_short,
    CAST(STRFTIME(d, '%Y%m%d') AS INT)                  AS date_int,
    CASE WHEN EXTRACT(dow FROM d) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend,
    CASE WHEN EXTRACT(dow FROM d) NOT IN (0,6) THEN TRUE ELSE FALSE END AS is_weekday,
    CASE
        WHEN EXTRACT(month FROM d) IN (12,1,2) THEN 'Winter'
        WHEN EXTRACT(month FROM d) IN (3,4,5)  THEN 'Spring'
        WHEN EXTRACT(month FROM d) IN (6,7,8)  THEN 'Summer'
        ELSE 'Autumn'
    END                                                 AS season,
    CASE
        WHEN EXTRACT(month FROM d) IN (12,1,2) THEN 1
        WHEN EXTRACT(month FROM d) IN (3,4,5)  THEN 2
        WHEN EXTRACT(month FROM d) IN (6,7,8)  THEN 3
        ELSE 4
    END                                                 AS season_num,
    DATE_TRUNC('month', d)                              AS first_day_of_month,
    LAST_DAY(d)                                         AS last_day_of_month,
    DATE_TRUNC('quarter', d)                            AS first_day_of_quarter,
    DATE_TRUNC('year', d)                               AS first_day_of_year
FROM (
    SELECT UNNEST(generate_series(
        DATE '{{ var("data_start_date") }}',
        DATE '2026-12-31',
        INTERVAL '1 day'
    )) AS d
)
