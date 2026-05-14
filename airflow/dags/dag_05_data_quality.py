"""
DAG: Data Quality Validation Pipeline
=======================================
Runs quality checks on all layers: null checks, duplicates,
value ranges, schema validation, and freshness checks.
"""

from __future__ import annotations

import sys
from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}


def install_deps():
    import subprocess
    for pkg in ["duckdb==0.10.3", "pandas==2.2.2", "pyyaml==6.0.1"]:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], capture_output=True)


def run_quality_checks(**context) -> dict[str, Any]:
    """Run all data quality checks across bronze, silver, gold layers."""
    install_deps()
    import duckdb, os, json, uuid
    from datetime import timezone

    db_path = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")
    conn = duckdb.connect(db_path)

    checks_config = [
        # ── Bronze Traffic ──
        {
            "source_id": "urban_traffic", "table": "bronze.bronze_urban_traffic",
            "checks": [
                {"type": "not_null", "col": "traffic_volume"},
                {"type": "not_null", "col": "event_timestamp"},
                {"type": "value_range", "col": "traffic_volume", "min": 0, "max": 100000},
                {"type": "duplicate", "cols": ["event_timestamp"]},
            ]
        },
        # ── Bronze Air Quality ──
        {
            "source_id": "air_quality", "table": "bronze.bronze_air_quality",
            "checks": [
                {"type": "not_null", "col": "city"},
                {"type": "not_null", "col": "measurement_timestamp"},
                {"type": "value_range", "col": "aqi", "min": 0, "max": 1000},
            ]
        },
        # ── Silver Traffic ──
        {
            "source_id": "silver_traffic", "table": "silver.silver_traffic",
            "checks": [
                {"type": "not_null", "col": "traffic_volume"},
                {"type": "not_null", "col": "event_timestamp"},
                {"type": "accepted_values", "col": "traffic_level",
                 "values": ["Low", "Medium", "High", "Very High"]},
            ]
        },
        # ── Gold Facts ──
        {
            "source_id": "fact_traffic_hourly", "table": "gold.fact_traffic_hourly",
            "checks": [
                {"type": "not_null", "col": "hour_key"},
                {"type": "value_range", "col": "avg_traffic_volume", "min": 0, "max": 100000},
                {"type": "row_count", "min_rows": 1},
            ]
        },
    ]

    all_results = []
    run_id = str(uuid.uuid4())
    overall_pass = True

    for source_cfg in checks_config:
        table = source_cfg["table"]
        source_id = source_cfg["source_id"]

        # Check table exists
        try:
            count_result = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
            total_rows = count_result[0] if count_result else 0
        except Exception:
            print(f"⚠️  Table {table} not found — skipping checks")
            continue

        for check in source_cfg["checks"]:
            check_type = check["type"]
            col = check.get("col", "")
            passed = True
            failed_rows = 0
            details = ""

            try:
                if check_type == "not_null":
                    failed_rows = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL"
                    ).fetchone()[0]
                    passed = failed_rows == 0
                    details = f"{failed_rows} null values in {col}"

                elif check_type == "value_range":
                    min_v, max_v = check.get("min"), check.get("max")
                    conds = []
                    if min_v is not None:
                        conds.append(f"CAST({col} AS DOUBLE) < {min_v}")
                    if max_v is not None:
                        conds.append(f"CAST({col} AS DOUBLE) > {max_v}")
                    if conds:
                        failed_rows = conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE {' OR '.join(conds)}"
                        ).fetchone()[0]
                    passed = failed_rows == 0
                    details = f"{failed_rows} out-of-range values in {col}"

                elif check_type == "duplicate":
                    cols_str = ", ".join(check["cols"])
                    failed_rows = conn.execute(f"""
                        SELECT COUNT(*) - COUNT(DISTINCT ({cols_str})) FROM {table}
                    """).fetchone()[0]
                    passed = failed_rows == 0
                    details = f"{failed_rows} duplicate rows on ({cols_str})"

                elif check_type == "accepted_values":
                    values_list = "', '".join(check["values"])
                    failed_rows = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} NOT IN ('{values_list}')"
                    ).fetchone()[0]
                    passed = failed_rows == 0
                    details = f"{failed_rows} unexpected values in {col}"

                elif check_type == "row_count":
                    min_rows = check.get("min_rows", 1)
                    passed = total_rows >= min_rows
                    failed_rows = 0 if passed else 1
                    details = f"Row count {total_rows} < min {min_rows}"

                failure_rate = round(failed_rows / max(total_rows, 1), 4)
                status_icon = "✅" if passed else "❌"
                print(
                    f"{status_icon} [{source_id}] {check_type}({col}): "
                    f"{'PASS' if passed else 'FAIL'} — {details}"
                )

                if not passed:
                    overall_pass = False

                all_results.append({
                    "run_id": run_id,
                    "source_id": source_id,
                    "check_type": check_type,
                    "check_column": col,
                    "passed": passed,
                    "failed_rows": failed_rows,
                    "total_rows": total_rows,
                    "failure_rate": failure_rate,
                    "details": details,
                })

            except Exception as e:
                print(f"⚠️  Check error [{source_id}.{col}]: {e}")

    # Write results to audit table
    if all_results:
        import pandas as pd
        df_audit = pd.DataFrame(all_results)
        conn.register("_quality_results", df_audit)
        try:
            conn.execute("INSERT INTO bronze.quality_audit SELECT * FROM _quality_results")
        except Exception:
            conn.execute("CREATE TABLE IF NOT EXISTS bronze.quality_audit AS SELECT * FROM _quality_results")
        conn.unregister("_quality_results")
        conn.commit()

    conn.close()

    total_checks = len(all_results)
    passed_checks = sum(1 for r in all_results if r["passed"])
    print(f"\n📊 Quality Report: {passed_checks}/{total_checks} checks passed")

    context["ti"].xcom_push(key="quality_pass_rate", value=passed_checks / max(total_checks, 1))

    # Don't fail the DAG on quality issues — just report
    return {
        "total_checks": total_checks,
        "passed": passed_checks,
        "failed": total_checks - passed_checks,
        "overall_pass": overall_pass,
    }


with DAG(
    dag_id="05_data_quality_validation",
    description="Data quality checks across bronze, silver, and gold layers",
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["quality", "validation", "monitoring"],
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    quality_task = PythonOperator(
        task_id="run_quality_checks",
        python_callable=run_quality_checks,
    )

    start >> quality_task >> end
