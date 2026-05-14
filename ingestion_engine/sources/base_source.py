"""
Base class for all ingestion source handlers.
All source handlers must inherit from this and implement `ingest()`.
"""

from __future__ import annotations

import abc
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ingestion_engine.utils.db_manager import DuckDBManager
from ingestion_engine.utils.logger import get_logger
from ingestion_engine.utils.schema_mapper import SchemaMapper

logger = get_logger(__name__)


class BaseIngestionSource(abc.ABC):
    """Abstract base class for ingestion source handlers."""

    def __init__(self, config: dict[str, Any], db_manager: DuckDBManager):
        self.config = config
        self.db_manager = db_manager
        self.source_id = config.get("source_id", "unknown")
        self.source_name = config.get("source_name", "Unknown Source")
        self.source_type = config.get("source_type", "unknown")
        self.format = config.get("format", "csv")
        self.target = config.get("target", {})
        self.schema_mapping = config.get("schema_mapping", {})
        self.quality_checks = config.get("quality_checks", [])
        self.schema_mapper = SchemaMapper(self.schema_mapping)

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract raw data from the source. Must be implemented by subclasses."""
        ...

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply schema mapping and standard transformations."""
        if df.empty:
            logger.warning(f"[{self.source_id}] Empty DataFrame — skipping transform")
            return df

        # Apply schema mapping (column rename + type cast)
        df = self.schema_mapper.apply(df)

        # Add standard audit columns
        now = datetime.now(timezone.utc)
        df["_ingestion_timestamp"] = now
        df["_ingestion_date"] = now.date()
        df["_source_id"] = self.source_id
        df["_source_type"] = self.source_type
        df["_batch_id"] = f"{self.source_id}_{int(now.timestamp())}"

        return df

    def load(self, df: pd.DataFrame) -> int:
        """Load transformed DataFrame into DuckDB target table."""
        if df.empty:
            logger.warning(f"[{self.source_id}] Nothing to load — empty DataFrame")
            return 0

        table = self.target.get("table", f"bronze_{self.source_id}")
        write_mode = self.target.get("write_mode", "append")

        rows = self.db_manager.write_dataframe(
            df=df,
            table_name=table,
            mode=write_mode,
        )
        logger.info(f"[{self.source_id}] Loaded {rows} rows into {table}")
        return rows

    def ingest(self) -> dict[str, Any]:
        """Full ETL pipeline: extract → transform → load."""
        start_time = time.monotonic()
        result: dict[str, Any] = {
            "source_id": self.source_id,
            "source_type": self.source_type,
            "status": "unknown",
            "rows_extracted": 0,
            "rows_written": 0,
            "errors": [],
            "started_at": datetime.now(timezone.utc).isoformat(),
        }

        try:
            logger.info(f"[{self.source_id}] ── EXTRACT ──")
            df_raw = self.extract()
            result["rows_extracted"] = len(df_raw)
            logger.info(f"[{self.source_id}] Extracted {len(df_raw)} rows")

            logger.info(f"[{self.source_id}] ── TRANSFORM ──")
            df_transformed = self.transform(df_raw)

            logger.info(f"[{self.source_id}] ── LOAD ──")
            rows_written = self.load(df_transformed)
            result["rows_written"] = rows_written

            result["status"] = "success"

        except Exception as e:
            logger.error(f"[{self.source_id}] Ingestion error: {e}", exc_info=True)
            result["status"] = "failed"
            result["errors"].append(str(e))
            raise

        finally:
            elapsed = time.monotonic() - start_time
            result["duration_seconds"] = round(elapsed, 2)
            result["finished_at"] = datetime.now(timezone.utc).isoformat()

            # Record run in audit table
            try:
                self.db_manager.record_ingestion_run(result)
            except Exception as audit_err:
                logger.warning(f"[{self.source_id}] Failed to write audit record: {audit_err}")

        return result
