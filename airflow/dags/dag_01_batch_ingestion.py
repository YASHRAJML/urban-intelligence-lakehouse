"""
DAG: Metadata-Driven Batch Ingestion Pipeline
==============================================
Reads all enabled source YAML configs and runs the appropriate
ingestion handler for each source.

Supports: kaggle, file, api sources
Schedule: Every hour (individual sources have their own schedule defined in metadata)
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, "/opt/airflow/ingestion_engine")
sys.path.insert(0, "/opt/airflow")

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": False,
}


def install_dependencies():
    """Ensure all required packages are installed at runtime."""
    import subprocess
    packages = [
        "duckdb==0.10.3", "kafka-python==2.0.2",
        "dbt-duckdb==1.8.1", "pandas==2.2.2",
        "pyarrow==16.1.0", "pyyaml==6.0.1",
        "requests==2.32.3", "tenacity==8.3.0",
        "faker==25.2.0",
    ]
    for pkg in packages:
        subprocess.run(
            [sys.executable, "-m", "pip", "install", "-q", pkg],
            capture_output=True
        )


def ingest_kaggle_sources(**context):
    """Run all Kaggle dataset ingestion sources."""
    install_dependencies()
    from ingestion_engine.engine import IngestionEngine
    engine = IngestionEngine()
    results = engine.run_by_type("kaggle")
    context["ti"].xcom_push(key="kaggle_results", value=results)
    failed = [r for r in results if r.get("status") == "failed"]
    if failed:
        raise RuntimeError(f"Kaggle ingestion failures: {[r['source_id'] for r in failed]}")
    return {"ingested": len(results), "results": results}


def ingest_file_sources(**context):
    """Run all file-based ingestion sources."""
    install_dependencies()
    from ingestion_engine.engine import IngestionEngine
    engine = IngestionEngine()
    results = engine.run_by_type("file")
    context["ti"].xcom_push(key="file_results", value=results)
    return {"ingested": len(results), "results": results}


def ingest_api_sources(**context):
    """Run all REST API ingestion sources."""
    install_dependencies()
    from ingestion_engine.engine import IngestionEngine
    engine = IngestionEngine()
    results = engine.run_by_type("api")
    context["ti"].xcom_push(key="api_results", value=results)
    return {"ingested": len(results), "results": results}


def log_ingestion_summary(**context):
    """Log a summary of all ingestion results from upstream tasks."""
    ti = context["ti"]
    all_results = []
    for key in ["kaggle_results", "file_results", "api_results"]:
        results = ti.xcom_pull(key=key) or []
        all_results.extend(results)

    total = len(all_results)
    success = sum(1 for r in all_results if r.get("status") == "success")
    failed = sum(1 for r in all_results if r.get("status") == "failed")
    total_rows = sum(r.get("rows_written", 0) for r in all_results)

    print(f"""
╔══════════════════════════════════════════════════════════╗
║          BATCH INGESTION SUMMARY                         ║
╠══════════════════════════════════════════════════════════╣
║  Total Sources  : {total:<40}║
║  Successful     : {success:<40}║
║  Failed         : {failed:<40}║
║  Total Rows     : {total_rows:<40}║
╚══════════════════════════════════════════════════════════╝
    """)

    if failed > 0:
        print(f"FAILED SOURCES: {[r['source_id'] for r in all_results if r.get('status') == 'failed']}")


with DAG(
    dag_id="01_metadata_driven_batch_ingestion",
    description="Metadata-driven batch ingestion for all configured sources",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["bronze", "batch", "ingestion", "metadata-driven"],
    doc_md="""
## Metadata-Driven Batch Ingestion Pipeline

This DAG reads YAML metadata configs from `/opt/airflow/metadata/sources/`
and dynamically routes each source to the appropriate ingestion handler.

### Adding a New Source
1. Create a new YAML file in `metadata/sources/`
2. Set `source_type` to one of: `kaggle`, `file`, `api`, `kafka`
3. The DAG will automatically pick it up on the next run

### Layers
- All sources land in the **Bronze** layer as-is
    """,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.ALL_DONE)

    kaggle_task = PythonOperator(
        task_id="ingest_kaggle_datasets",
        python_callable=ingest_kaggle_sources,
        doc_md="Ingest all enabled Kaggle dataset sources",
    )

    file_task = PythonOperator(
        task_id="ingest_file_sources",
        python_callable=ingest_file_sources,
        doc_md="Ingest all enabled file-based sources (CSV, JSON, Parquet)",
    )

    api_task = PythonOperator(
        task_id="ingest_api_sources",
        python_callable=ingest_api_sources,
        doc_md="Ingest all enabled REST API sources",
    )

    summary_task = PythonOperator(
        task_id="log_ingestion_summary",
        python_callable=log_ingestion_summary,
        trigger_rule=TriggerRule.ALL_DONE,
        doc_md="Log overall ingestion summary from all sources",
    )

    # Sequential ingestion to avoid DuckDB write lock conflicts
    start >> kaggle_task >> file_task >> api_task >> summary_task >> end
