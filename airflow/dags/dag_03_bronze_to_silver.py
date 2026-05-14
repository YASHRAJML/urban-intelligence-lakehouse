"""
DAG: Bronze → Silver Transformation
=====================================
Cleans and standardizes bronze data into silver layer.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}


def install_deps():
    import subprocess
    for pkg in ["duckdb==0.10.3", "pandas==2.2.2"]:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], capture_output=True)


def _table_exists(conn, table_name: str) -> bool:
    try:
        conn.execute(f"SELECT 1 FROM {table_name} LIMIT 1")
        return True
    except Exception:
        return False


def transform_traffic(**context):
    install_deps()
    import duckdb, os
    db = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db)
    try:
        if not _table_exists(conn, "bronze.bronze_urban_traffic"):
            print("bronze_urban_traffic not found — skipping")
            return {"status": "skipped"}
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.silver_traffic AS
            SELECT
                CAST(event_timestamp AS TIMESTAMPTZ)        AS event_timestamp,
                DATE_TRUNC('hour', event_timestamp)         AS event_hour,
                DATE_TRUNC('day',  event_timestamp)         AS event_date,
                EXTRACT(hour FROM event_timestamp)::INT     AS hour_of_day,
                EXTRACT(dow  FROM event_timestamp)::INT     AS day_of_week,
                CASE WHEN EXTRACT(dow FROM event_timestamp) IN (0,6)
                     THEN TRUE ELSE FALSE END               AS is_weekend,
                COALESCE(holiday_name, 'None')              AS holiday_name,
                ROUND(temperature_kelvin - 273.15, 2)       AS temperature_celsius,
                COALESCE(rainfall_mm, 0.0)                  AS rainfall_mm,
                COALESCE(snowfall_mm, 0.0)                  AS snowfall_mm,
                COALESCE(cloud_coverage_pct, 0)             AS cloud_coverage_pct,
                UPPER(TRIM(weather_condition))              AS weather_condition,
                LOWER(TRIM(weather_description))            AS weather_description,
                traffic_volume,
                CASE
                    WHEN traffic_volume < 1000 THEN 'Low'
                    WHEN traffic_volume < 3000 THEN 'Medium'
                    WHEN traffic_volume < 5000 THEN 'High'
                    ELSE 'Very High'
                END                                         AS traffic_level,
                _ingestion_timestamp,
                _source_id,
                _batch_id,
                CURRENT_TIMESTAMP                           AS silver_processed_at
            FROM bronze.bronze_urban_traffic
            WHERE traffic_volume IS NOT NULL
              AND traffic_volume BETWEEN 0 AND 100000
              AND event_timestamp IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM silver.silver_traffic").fetchone()[0]
        conn.commit()
        print(f"✅ silver_traffic: {rows} rows")
        return {"status": "success", "rows": rows}
    finally:
        conn.close()


def transform_air_quality(**context):
    install_deps()
    import duckdb, os
    db = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db)
    try:
        if not _table_exists(conn, "bronze.bronze_air_quality"):
            print("bronze_air_quality not found — skipping")
            return {"status": "skipped"}
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.silver_air_quality AS
            SELECT
                UPPER(TRIM(city))                           AS city,
                CAST(measurement_timestamp AS TIMESTAMPTZ)  AS measurement_timestamp,
                DATE_TRUNC('hour', measurement_timestamp)   AS measurement_hour,
                DATE_TRUNC('day',  measurement_timestamp)   AS measurement_date,
                COALESCE(pm25, 0.0)                         AS pm25,
                COALESCE(pm10, 0.0)                         AS pm10,
                COALESCE(aqi,  0.0)                         AS aqi,
                COALESCE(aqi_category, 'Unknown')           AS aqi_category,
                COALESCE(ozone, 0.0)                        AS ozone,
                COALESCE(carbon_monoxide, 0.0)              AS carbon_monoxide,
                COALESCE(sulfur_dioxide, 0.0)               AS sulfur_dioxide,
                COALESCE(nitrogen_dioxide, 0.0)             AS nitrogen_dioxide,
                CASE WHEN aqi > 300 THEN TRUE ELSE FALSE END AS is_hazardous,
                _ingestion_timestamp,
                _source_id,
                CURRENT_TIMESTAMP                           AS silver_processed_at
            FROM bronze.bronze_air_quality
            WHERE city IS NOT NULL AND measurement_timestamp IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM silver.silver_air_quality").fetchone()[0]
        conn.commit()
        print(f"✅ silver_air_quality: {rows} rows")
        return {"status": "success", "rows": rows}
    finally:
        conn.close()


def transform_weather(**context):
    install_deps()
    import duckdb, os
    db = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db)
    try:
        if not _table_exists(conn, "bronze.bronze_weather_api"):
            print("bronze_weather_api not found — skipping")
            return {"status": "skipped"}
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.silver_weather AS
            SELECT
                UPPER(TRIM(city_name))                      AS city_name,
                CAST(TO_TIMESTAMP(observation_timestamp) AS TIMESTAMPTZ) AS observation_timestamp,
                DATE_TRUNC('hour', TO_TIMESTAMP(observation_timestamp))  AS observation_hour,
                ROUND(temperature_celsius, 2)               AS temperature_celsius,
                ROUND(feels_like_celsius, 2)                AS feels_like_celsius,
                COALESCE(humidity_pct, 0)                   AS humidity_pct,
                COALESCE(pressure_hpa, 1013)                AS pressure_hpa,
                COALESCE(wind_speed_ms, 0.0)                AS wind_speed_ms,
                UPPER(TRIM(weather_condition))              AS weather_condition,
                latitude,
                longitude,
                CASE
                    WHEN temperature_celsius >= 35 THEN 'Extreme Heat'
                    WHEN temperature_celsius >= 25 THEN 'Warm'
                    WHEN temperature_celsius >= 10 THEN 'Mild'
                    WHEN temperature_celsius >= 0  THEN 'Cool'
                    ELSE 'Cold'
                END                                         AS temperature_category,
                _ingestion_timestamp,
                _source_id,
                CURRENT_TIMESTAMP                           AS silver_processed_at
            FROM bronze.bronze_weather_api
            WHERE city_name IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM silver.silver_weather").fetchone()[0]
        conn.commit()
        print(f"✅ silver_weather: {rows} rows")
        return {"status": "success", "rows": rows}
    finally:
        conn.close()


def transform_stream_events(**context):
    install_deps()
    import duckdb, os
    db = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db)
    try:
        if not _table_exists(conn, "bronze.bronze_urban_stream_events"):
            print("bronze_urban_stream_events not found — skipping")
            return {"status": "skipped"}
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.silver_stream_events AS
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
                CURRENT_TIMESTAMP                           AS silver_processed_at
            FROM bronze.bronze_urban_stream_events
            WHERE event_id IS NOT NULL AND event_type IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM silver.silver_stream_events").fetchone()[0]
        conn.commit()
        print(f"✅ silver_stream_events: {rows} rows")
        return {"status": "success", "rows": rows}
    finally:
        conn.close()


def transform_demographics(**context):
    install_deps()
    import duckdb, os
    db = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db)
    try:
        if not _table_exists(conn, "bronze.bronze_city_demographics"):
            print("bronze_city_demographics not found — skipping")
            return {"status": "skipped"}
        conn.execute("CREATE SCHEMA IF NOT EXISTS silver")
        conn.execute("""
            CREATE OR REPLACE TABLE silver.silver_demographics AS
            SELECT
                UPPER(TRIM(city_name))                      AS city_name,
                UPPER(TRIM(state_code))                     AS state_code,
                state_name,
                COALESCE(median_age, 35.0)                  AS median_age,
                COALESCE(total_population, 0)               AS total_population,
                COALESCE(male_population, 0)                AS male_population,
                COALESCE(female_population, 0)              AS female_population,
                COALESCE(avg_household_size, 2.5)           AS avg_household_size,
                race,
                COALESCE(race_count, 0)                     AS race_count,
                CASE
                    WHEN total_population < 100000   THEN 'Small'
                    WHEN total_population < 500000   THEN 'Medium'
                    WHEN total_population < 1000000  THEN 'Large'
                    ELSE 'Mega'
                END                                         AS city_size_category,
                _ingestion_timestamp,
                _source_id,
                CURRENT_TIMESTAMP                           AS silver_processed_at
            FROM bronze.bronze_city_demographics
            WHERE city_name IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM silver.silver_demographics").fetchone()[0]
        conn.commit()
        print(f"✅ silver_demographics: {rows} rows")
        return {"status": "success", "rows": rows}
    finally:
        conn.close()


with DAG(
    dag_id="03_bronze_to_silver_transformation",
    description="Clean and standardize bronze → silver layer",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["silver", "transformation"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    t1 = PythonOperator(task_id="transform_traffic", python_callable=transform_traffic)
    t2 = PythonOperator(task_id="transform_air_quality", python_callable=transform_air_quality)
    t3 = PythonOperator(task_id="transform_weather", python_callable=transform_weather)
    t4 = PythonOperator(task_id="transform_stream_events", python_callable=transform_stream_events)
    t5 = PythonOperator(task_id="transform_demographics", python_callable=transform_demographics)

    start >> [t1, t2, t3, t4, t5] >> end
