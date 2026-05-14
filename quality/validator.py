"""
Data Quality Validator — standalone utility for running checks
against any DuckDB table using YAML-defined rules.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

import duckdb
import pandas as pd

from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb")


class DataQualityValidator:
    """Runs configurable quality checks on DuckDB tables."""

    def __init__(self, db_path: str = DUCKDB_PATH):
        self.db_path = db_path

    def run_checks(
        self,
        table: str,
        checks: list[dict[str, Any]],
        source_id: str = "",
    ) -> list[dict[str, Any]]:
        """Run a list of checks against a table. Returns check results."""
        conn = duckdb.connect(self.db_path)
        run_id = str(uuid.uuid4())
        results = []

        try:
            total_rows = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        except Exception as e:
            logger.warning(f"Table {table} not accessible: {e}")
            conn.close()
            return results

        for check in checks:
            check_type = check.get("check_type", check.get("type", ""))
            col = check.get("column", check.get("col", ""))
            passed = True
            failed_rows = 0
            details = ""

            try:
                if check_type == "not_null":
                    cols = check.get("columns", [col] if col else [])
                    for c in cols:
                        count = conn.execute(
                            f"SELECT COUNT(*) FROM {table} WHERE {c} IS NULL"
                        ).fetchone()[0]
                        if count > 0:
                            passed = False
                            failed_rows += count
                    details = f"{failed_rows} nulls in {cols}"

                elif check_type == "unique":
                    cols = check.get("columns", [col])
                    cols_str = ", ".join(cols)
                    dupes = conn.execute(
                        f"SELECT COUNT(*) - COUNT(DISTINCT ({cols_str})) FROM {table}"
                    ).fetchone()[0]
                    passed = dupes == 0
                    failed_rows = dupes
                    details = f"{dupes} duplicate rows on ({cols_str})"

                elif check_type == "value_range":
                    min_v = check.get("min")
                    max_v = check.get("max")
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
                    details = f"{failed_rows} values outside [{min_v}, {max_v}] in {col}"

                elif check_type == "accepted_values":
                    values = check.get("values", [])
                    vals_str = "', '".join(str(v) for v in values)
                    failed_rows = conn.execute(
                        f"SELECT COUNT(*) FROM {table} WHERE {col} NOT IN ('{vals_str}')"
                    ).fetchone()[0]
                    passed = failed_rows == 0
                    details = f"{failed_rows} unexpected values in {col}"

                elif check_type == "freshness":
                    max_age_hours = check.get("max_age_hours", 24)
                    count = conn.execute(f"""
                        SELECT COUNT(*) FROM {table}
                        WHERE CAST({col} AS TIMESTAMPTZ) >= NOW() - INTERVAL '{max_age_hours} hours'
                    """).fetchone()[0]
                    passed = count > 0
                    failed_rows = 0 if passed else 1
                    details = f"Freshness check: {'OK' if passed else f'No data in last {max_age_hours}h'}"

                elif check_type == "row_count":
                    min_rows = check.get("min_rows", 1)
                    passed = total_rows >= min_rows
                    failed_rows = 0 if passed else 1
                    details = f"Row count {total_rows} vs min {min_rows}"

                else:
                    logger.warning(f"Unknown check type: {check_type}")
                    continue

                failure_rate = round(failed_rows / max(total_rows, 1), 6)
                icon = "✅" if passed else "❌"
                logger.info(f"{icon} [{source_id}] {check_type}({col}): {'PASS' if passed else 'FAIL'} — {details}")

                results.append({
                    "run_id": run_id,
                    "source_id": source_id,
                    "check_type": check_type,
                    "check_column": col,
                    "passed": passed,
                    "failed_rows": failed_rows,
                    "total_rows": total_rows,
                    "failure_rate": failure_rate,
                    "details": details,
                    "checked_at": datetime.now(timezone.utc).isoformat(),
                })

            except Exception as e:
                logger.error(f"Check error [{check_type}:{col}]: {e}", exc_info=True)

        conn.close()
        return results

    def write_audit(self, results: list[dict[str, Any]]) -> None:
        """Write quality check results to the audit table."""
        if not results:
            return
        df = pd.DataFrame(results)
        conn = duckdb.connect(self.db_path)
        try:
            conn.register("_quality", df)
            try:
                conn.execute("INSERT INTO bronze.quality_audit SELECT * FROM _quality")
            except Exception:
                conn.execute("CREATE TABLE IF NOT EXISTS bronze.quality_audit AS SELECT * FROM _quality")
            conn.commit()
        finally:
            conn.unregister("_quality")
            conn.close()
