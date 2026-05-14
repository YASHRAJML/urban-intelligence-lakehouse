"""
DAG: Kafka Stream Ingestion
===========================
Polls Kafka topics and writes streaming events to the Bronze layer.
Runs every 5 minutes to create micro-batch ingestion from Kafka.
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
    "retries": 5,
    "retry_delay": timedelta(minutes=1),
    "email_on_failure": False,
    "depends_on_past": False,
}


def install_dependencies():
    import subprocess
    packages = [
        "duckdb==0.10.3", "kafka-python==2.0.2",
        "pandas==2.2.2", "pyarrow==16.1.0",
        "pyyaml==6.0.1", "tenacity==8.3.0", "faker==25.2.0",
    ]
    for pkg in packages:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], capture_output=True)


def consume_kafka_events(**context):
    """Poll all Kafka topics and write events to bronze layer."""
    install_dependencies()
    from ingestion_engine.engine import IngestionEngine
    engine = IngestionEngine()
    results = engine.run_by_type("kafka")
    total_rows = sum(r.get("rows_written", 0) for r in results)
    context["ti"].xcom_push(key="kafka_rows_written", value=total_rows)
    print(f"Kafka ingestion complete: {total_rows} events written")
    return {"results": results, "total_rows": total_rows}


def validate_stream_freshness(**context):
    """Check that streaming data is being received and is fresh."""
    install_dependencies()
    import duckdb
    import os
    from datetime import timezone

    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")

    try:
        conn = duckdb.connect(db_path)
        result = conn.execute("""
            SELECT
                COUNT(*) as total_events,
                MAX(event_timestamp) as latest_event,
                MIN(event_timestamp) as oldest_event,
                COUNT(DISTINCT event_type) as event_types
            FROM bronze.bronze_urban_stream_events
            WHERE _ingestion_timestamp >= NOW() - INTERVAL '10 minutes'
        """).fetchone()
        conn.close()

        if result and result[0] > 0:
            print(f"Stream freshness OK: {result[0]} events, latest={result[1]}")
        else:
            print("WARNING: No recent streaming events found in the last 10 minutes")
    except Exception as e:
        print(f"Stream freshness check error (non-fatal): {e}")


with DAG(
    dag_id="02_kafka_stream_ingestion",
    description="Micro-batch Kafka event consumer for urban IoT streaming data",
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
    tags=["bronze", "streaming", "kafka", "iot"],
    doc_md="""
## Kafka Stream Ingestion DAG

Polls Kafka topics every 5 minutes and writes IoT urban events to the bronze layer.

**Topics consumed:**
- `urban.traffic.events` — vehicle detection events
- `urban.pedestrian.events` — pedestrian counting
- `urban.air.events` — air quality sensor readings
- `urban.orders.events` — delivery/order events

All events land in: `bronze.bronze_urban_stream_events`
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    consume_task = PythonOperator(
        task_id="consume_kafka_topics",
        python_callable=consume_kafka_events,
    )

    freshness_task = PythonOperator(
        task_id="validate_stream_freshness",
        python_callable=validate_stream_freshness,
    )

    start >> consume_task >> freshness_task >> end
