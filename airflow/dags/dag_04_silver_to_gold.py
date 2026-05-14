"""
DAG: Silver → Gold Transformation
===================================
Builds analytics-ready Gold layer tables (facts, dims, aggregates).
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


def build_fact_traffic_hourly(**context):
    """Build fact_traffic_hourly gold table."""
    install_deps()
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("""
            CREATE OR REPLACE TABLE gold.fact_traffic_hourly AS
            SELECT
                event_hour                              AS hour_key,
                event_date                              AS date_key,
                hour_of_day,
                day_of_week,
                is_weekend,
                is_holiday,
                weather_condition,
                temperature_celsius,
                rainfall_mm,
                snowfall_mm,
                AVG(traffic_volume)::INT                AS avg_traffic_volume,
                MAX(traffic_volume)                     AS max_traffic_volume,
                MIN(traffic_volume)                     AS min_traffic_volume,
                SUM(traffic_volume)                     AS total_traffic_volume,
                COUNT(*)                                AS measurement_count,
                STDDEV(traffic_volume)::DOUBLE          AS traffic_stddev,
                CURRENT_TIMESTAMP                       AS gold_processed_at
            FROM silver.silver_traffic
            GROUP BY 1,2,3,4,5,6,7,8,9,10
        """)
        rows = conn.execute("SELECT COUNT(*) FROM gold.fact_traffic_hourly").fetchone()[0]
        print(f"✅ fact_traffic_hourly: {rows} rows")
        return rows
    finally:
        conn.close()


def build_fact_air_quality_daily(**context):
    """Build fact_air_quality_daily gold table."""
    install_deps()
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("""
            CREATE OR REPLACE TABLE gold.fact_air_quality_daily AS
            SELECT
                city,
                measurement_date                        AS date_key,
                AVG(aqi)::DOUBLE                        AS avg_aqi,
                MAX(aqi)                                AS max_aqi,
                MIN(aqi)                                AS min_aqi,
                AVG(pm25)::DOUBLE                       AS avg_pm25,
                AVG(pm10)::DOUBLE                       AS avg_pm10,
                AVG(ozone)::DOUBLE                      AS avg_ozone,
                AVG(carbon_monoxide)::DOUBLE            AS avg_co,
                SUM(CASE WHEN is_hazardous THEN 1 ELSE 0 END) AS hazardous_hours,
                COUNT(*)                                AS measurement_count,
                MODE(aqi_category)                      AS dominant_aqi_category,
                CURRENT_TIMESTAMP                       AS gold_processed_at
            FROM silver.silver_air_quality
            GROUP BY 1, 2
        """)
        rows = conn.execute("SELECT COUNT(*) FROM gold.fact_air_quality_daily").fetchone()[0]
        print(f"✅ fact_air_quality_daily: {rows} rows")
        return rows
    finally:
        conn.close()


def build_fact_stream_events_hourly(**context):
    """Build fact_stream_events_hourly gold table."""
    install_deps()
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("""
            CREATE OR REPLACE TABLE gold.fact_stream_events_hourly AS
            SELECT
                event_hour                              AS hour_key,
                event_category,
                kafka_topic,
                COUNT(*)                                AS event_count,
                COUNT(DISTINCT sensor_id)               AS unique_sensors,
                COUNT(DISTINCT location_id)             AS unique_locations,
                AVG(metric_value)::DOUBLE               AS avg_metric_value,
                MAX(metric_value)                       AS max_metric_value,
                MIN(metric_value)                       AS min_metric_value,
                CURRENT_TIMESTAMP                       AS gold_processed_at
            FROM silver.silver_stream_events
            GROUP BY 1, 2, 3
        """)
        rows = conn.execute("SELECT COUNT(*) FROM gold.fact_stream_events_hourly").fetchone()[0]
        print(f"✅ fact_stream_events_hourly: {rows} rows")
        return rows
    finally:
        conn.close()


def build_dim_date(**context):
    """Build dim_date dimension table."""
    install_deps()
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        conn.execute("""
            CREATE OR REPLACE TABLE gold.dim_date AS
            SELECT
                CAST(d AS DATE)                                 AS date_key,
                EXTRACT(year  FROM d)::INT                      AS year,
                EXTRACT(month FROM d)::INT                      AS month,
                EXTRACT(day   FROM d)::INT                      AS day,
                EXTRACT(dow   FROM d)::INT                      AS day_of_week,
                EXTRACT(doy   FROM d)::INT                      AS day_of_year,
                EXTRACT(week  FROM d)::INT                      AS week_of_year,
                EXTRACT(quarter FROM d)::INT                    AS quarter,
                STRFTIME(d, '%A')                               AS day_name,
                STRFTIME(d, '%B')                               AS month_name,
                CASE WHEN EXTRACT(dow FROM d) IN (0,6)
                     THEN TRUE ELSE FALSE END                   AS is_weekend,
                CASE WHEN EXTRACT(month FROM d) IN (12,1,2)
                     THEN 'Winter'
                     WHEN EXTRACT(month FROM d) IN (3,4,5)
                     THEN 'Spring'
                     WHEN EXTRACT(month FROM d) IN (6,7,8)
                     THEN 'Summer'
                     ELSE 'Autumn' END                         AS season
            FROM (
                SELECT UNNEST(generate_series(
                    DATE '2020-01-01',
                    DATE '2026-12-31',
                    INTERVAL '1 day'
                )) AS d
            )
        """)
        rows = conn.execute("SELECT COUNT(*) FROM gold.dim_date").fetchone()[0]
        print(f"✅ dim_date: {rows} rows")
        return rows
    finally:
        conn.close()


def build_gold_urban_intelligence_summary(**context):
    """Build comprehensive urban intelligence summary table."""
    install_deps()
    import duckdb, os
    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS gold")
        # Cross-source analytics table
        conn.execute("""
            CREATE OR REPLACE TABLE gold.urban_intelligence_daily AS
            WITH traffic_daily AS (
                SELECT
                    date_key,
                    AVG(avg_traffic_volume)::DOUBLE AS avg_daily_traffic,
                    SUM(total_traffic_volume)       AS total_daily_traffic
                FROM gold.fact_traffic_hourly
                GROUP BY 1
            ),
            events_daily AS (
                SELECT
                    DATE_TRUNC('day', hour_key) AS date_key,
                    SUM(event_count)            AS total_stream_events,
                    SUM(unique_sensors)         AS total_sensors_active
                FROM gold.fact_stream_events_hourly
                GROUP BY 1
            )
            SELECT
                COALESCE(t.date_key, e.date_key)    AS date_key,
                COALESCE(t.avg_daily_traffic, 0)    AS avg_daily_traffic,
                COALESCE(t.total_daily_traffic, 0)  AS total_daily_traffic,
                COALESCE(e.total_stream_events, 0)  AS total_stream_events,
                COALESCE(e.total_sensors_active, 0) AS total_sensors_active,
                CURRENT_TIMESTAMP                   AS gold_processed_at
            FROM traffic_daily t
            FULL OUTER JOIN events_daily e ON t.date_key = e.date_key
            WHERE COALESCE(t.date_key, e.date_key) IS NOT NULL
        """)
        rows = conn.execute("SELECT COUNT(*) FROM gold.urban_intelligence_daily").fetchone()[0]
        print(f"✅ urban_intelligence_daily: {rows} rows")
        return rows
    finally:
        conn.close()


with DAG(
    dag_id="04_silver_to_gold_transformation",
    description="Build analytics-ready Gold layer tables from Silver",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["gold", "analytics", "facts", "dimensions"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    dim_date = PythonOperator(task_id="build_dim_date", python_callable=build_dim_date)
    fact_traffic = PythonOperator(task_id="build_fact_traffic_hourly", python_callable=build_fact_traffic_hourly)
    fact_air = PythonOperator(task_id="build_fact_air_quality_daily", python_callable=build_fact_air_quality_daily)
    fact_stream = PythonOperator(task_id="build_fact_stream_events_hourly", python_callable=build_fact_stream_events_hourly)
    summary = PythonOperator(task_id="build_urban_intelligence_summary", python_callable=build_gold_urban_intelligence_summary)

    start >> dim_date
    start >> [fact_traffic, fact_air, fact_stream]
    [fact_traffic, fact_stream] >> summary
    [dim_date, fact_air, summary] >> end
