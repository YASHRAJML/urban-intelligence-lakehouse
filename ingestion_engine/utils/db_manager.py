"""
DuckDB Manager — Thread-safe connection pooling and table operations.
Provides a clean interface for reading/writing DataFrames to DuckDB.
"""

from __future__ import annotations

import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)


class DuckDBManager:
    """
    Thread-safe DuckDB connection manager for the lakehouse.

    Manages connections and provides high-level methods for:
    - Writing DataFrames to tables
    - Reading tables as DataFrames
    - Schema management (medallion layers)
    - Audit trail recording
    """

    _instances: dict[str, "DuckDBManager"] = {}
    _lock = threading.Lock()

    def __new__(cls, db_path: str) -> "DuckDBManager":
        """Singleton per db_path."""
        with cls._lock:
            if db_path not in cls._instances:
                instance = super().__new__(cls)
                instance._initialized = False
                cls._instances[db_path] = instance
        return cls._instances[db_path]

    def __init__(self, db_path: str):
        if self._initialized:
            return
        self.db_path = db_path
        self._connection_lock = threading.Lock()
        self._ensure_db_dir()
        self._init_schemas()
        self._initialized = True
        logger.info(f"DuckDBManager initialized: {db_path}")

    def _ensure_db_dir(self) -> None:
        """Create parent directory if it doesn't exist."""
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)

    def _get_connection(self) -> duckdb.DuckDBPyConnection:
        """Get a new DuckDB connection (DuckDB is file-based, safe to create per call)."""
        return duckdb.connect(self.db_path)

    def _init_schemas(self) -> None:
        """Initialize medallion layer schemas and audit tables."""
        with self._get_connection() as conn:
            # Medallion layer schemas
            for schema in ["bronze", "silver", "gold", "staging"]:
                conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")

            # Ingestion audit table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze.ingestion_audit (
                    run_id          VARCHAR,
                    source_id       VARCHAR,
                    source_type     VARCHAR,
                    status          VARCHAR,
                    rows_extracted  BIGINT,
                    rows_written    BIGINT,
                    duration_seconds DOUBLE,
                    started_at      TIMESTAMPTZ,
                    finished_at     TIMESTAMPTZ,
                    errors          VARCHAR,
                    created_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Data quality audit table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS bronze.quality_audit (
                    run_id          VARCHAR,
                    source_id       VARCHAR,
                    check_type      VARCHAR,
                    check_column    VARCHAR,
                    passed          BOOLEAN,
                    failed_rows     BIGINT,
                    total_rows      BIGINT,
                    failure_rate    DOUBLE,
                    details         VARCHAR,
                    checked_at      TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.commit()
        logger.info("DuckDB schemas initialized: bronze, silver, gold, staging")

    def write_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        mode: str = "append",
        schema: str | None = None,
    ) -> int:
        """
        Write a DataFrame to a DuckDB table.

        Args:
            df: DataFrame to write
            table_name: Target table name (can include schema prefix)
            mode: 'append' | 'overwrite' | 'replace'
            schema: Schema to use if table_name doesn't include one
        """
        if df.empty:
            logger.warning(f"Skipping write to {table_name} — empty DataFrame")
            return 0

        # Resolve full table reference
        if "." not in table_name and schema:
            full_table = f"{schema}.{table_name}"
        elif "." not in table_name:
            # Infer schema from prefix
            prefix = table_name.split("_")[0].lower()
            if prefix in ("bronze", "silver", "gold", "staging"):
                full_table = f"{prefix}.{table_name}"
            else:
                full_table = table_name
        else:
            full_table = table_name

        with self._connection_lock:
            with self._get_connection() as conn:
                # Register df as a view for SQL access
                conn.register("_write_df", df)
                try:
                    if mode in ("overwrite", "replace"):
                        conn.execute(
                            f"CREATE OR REPLACE TABLE {full_table} AS SELECT * FROM _write_df"
                        )
                    else:  # append (create if not exists)
                        try:
                            conn.execute(
                                f"INSERT INTO {full_table} SELECT * FROM _write_df"
                            )
                        except duckdb.CatalogException:
                            # Table doesn't exist — create it
                            conn.execute(
                                f"CREATE TABLE {full_table} AS SELECT * FROM _write_df"
                            )
                    conn.commit()
                    row_count = len(df)
                    logger.info(f"Wrote {row_count} rows to {full_table} (mode={mode})")
                    return row_count
                finally:
                    conn.unregister("_write_df")

    def read_table(
        self,
        table_name: str,
        query: str | None = None,
        limit: int | None = None,
    ) -> pd.DataFrame:
        """Read a table or run a SQL query and return as DataFrame."""
        with self._get_connection() as conn:
            if query:
                sql = query
            else:
                full_table = table_name if "." in table_name else table_name
                sql = f"SELECT * FROM {full_table}"
                if limit:
                    sql += f" LIMIT {limit}"
            return conn.execute(sql).df()

    def execute(self, sql: str, params: list | None = None) -> Any:
        """Execute arbitrary SQL and return the result."""
        with self._get_connection() as conn:
            if params:
                return conn.execute(sql, params)
            return conn.execute(sql)

    def table_exists(self, table_name: str, schema: str | None = None) -> bool:
        """Check if a table exists."""
        try:
            ref = f"{schema}.{table_name}" if schema else table_name
            with self._get_connection() as conn:
                conn.execute(f"SELECT 1 FROM {ref} LIMIT 1")
            return True
        except Exception:
            return False

    def get_table_stats(self, table_name: str) -> dict[str, Any]:
        """Return basic table statistics."""
        try:
            with self._get_connection() as conn:
                result = conn.execute(
                    f"SELECT COUNT(*) as rows FROM {table_name}"
                ).fetchone()
                return {"table": table_name, "rows": result[0] if result else 0}
        except Exception as e:
            return {"table": table_name, "rows": 0, "error": str(e)}

    def record_ingestion_run(self, result: dict[str, Any]) -> None:
        """Record an ingestion run in the audit table."""
        import uuid
        import json

        row = pd.DataFrame([{
            "run_id": str(uuid.uuid4()),
            "source_id": result.get("source_id", ""),
            "source_type": result.get("source_type", ""),
            "status": result.get("status", ""),
            "rows_extracted": result.get("rows_extracted", 0),
            "rows_written": result.get("rows_written", 0),
            "duration_seconds": result.get("duration_seconds", 0.0),
            "started_at": result.get("started_at"),
            "finished_at": result.get("finished_at"),
            "errors": json.dumps(result.get("errors", [])),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }])
        self.write_dataframe(row, "bronze.ingestion_audit", mode="append")

    def list_tables(self, schema: str | None = None) -> list[dict[str, Any]]:
        """List all tables in the database or a specific schema."""
        with self._get_connection() as conn:
            if schema:
                result = conn.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_schema = ?",
                    [schema],
                ).fetchall()
            else:
                result = conn.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables "
                    "WHERE table_schema NOT IN ('information_schema', 'pg_catalog')"
                ).fetchall()
            return [{"schema": r[0], "table": r[1]} for r in result]
