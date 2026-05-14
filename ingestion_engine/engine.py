"""
Unified Real-Time Data Lakehouse for Urban Intelligence
=======================================================
Core Ingestion Engine - Main Dispatcher

Reads metadata YAML configs and routes to the appropriate ingestion handler.
Adding a new data source requires only adding a new YAML file - no code changes.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import yaml

from ingestion_engine.sources.file_source import FileIngestionSource
from ingestion_engine.sources.api_source import APIIngestionSource
from ingestion_engine.sources.kafka_source import KafkaIngestionSource
from ingestion_engine.sources.kaggle_source import KaggleIngestionSource
from ingestion_engine.utils.db_manager import DuckDBManager
from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)

# Registry maps source_type -> handler class
SOURCE_REGISTRY: dict[str, type] = {
    "file": FileIngestionSource,
    "api": APIIngestionSource,
    "kafka": KafkaIngestionSource,
    "kaggle": KaggleIngestionSource,
}


class MetadataLoader:
    """Discovers and loads all YAML source configs from the metadata directory."""

    def __init__(self, metadata_dir: str | None = None):
        self.metadata_dir = Path(
            metadata_dir or os.getenv("METADATA_PATH", "/opt/airflow/metadata")
        )

    def load_all(self) -> list[dict[str, Any]]:
        """Load all source metadata configs."""
        configs = []
        sources_dir = self.metadata_dir / "sources"

        if not sources_dir.exists():
            logger.warning(f"Metadata sources directory not found: {sources_dir}")
            return configs

        for yaml_file in sorted(sources_dir.glob("*.yaml")):
            try:
                config = self._load_yaml(yaml_file)
                if config.get("enabled", True):
                    configs.append(config)
                    logger.info(f"Loaded source config: {config.get('source_id')} from {yaml_file.name}")
                else:
                    logger.info(f"Skipping disabled source: {config.get('source_id')}")
            except Exception as e:
                logger.error(f"Failed to load config {yaml_file}: {e}", exc_info=True)

        logger.info(f"Loaded {len(configs)} enabled source configurations")
        return configs

    def load_by_id(self, source_id: str) -> dict[str, Any] | None:
        """Load a specific source config by ID."""
        for config in self.load_all():
            if config.get("source_id") == source_id:
                return config
        logger.warning(f"Source config not found for ID: {source_id}")
        return None

    def load_by_type(self, source_type: str) -> list[dict[str, Any]]:
        """Load all configs for a given source type."""
        return [c for c in self.load_all() if c.get("source_type") == source_type]

    @staticmethod
    def _load_yaml(path: Path) -> dict[str, Any]:
        with open(path, "r", encoding="utf-8") as f:
            return yaml.safe_load(f)


class IngestionEngine:
    """
    Metadata-driven ingestion dispatcher.

    Routes ingestion requests to the correct handler based on
    source_type defined in YAML metadata configs.
    """

    def __init__(
        self,
        metadata_dir: str | None = None,
        db_path: str | None = None,
    ):
        self.metadata_loader = MetadataLoader(metadata_dir)
        self.db_path = db_path or os.getenv(
            "DUCKDB_PATH", "/opt/airflow/data/duckdb/urban_intelligence.duckdb"
        )
        self._db_manager: DuckDBManager | None = None

    @property
    def db_manager(self) -> DuckDBManager:
        if self._db_manager is None:
            self._db_manager = DuckDBManager(self.db_path)
        return self._db_manager

    def run_source(self, source_id: str) -> dict[str, Any]:
        """Run ingestion for a single source by ID."""
        config = self.metadata_loader.load_by_id(source_id)
        if not config:
            raise ValueError(f"No config found for source_id: {source_id}")
        return self._execute_ingestion(config)

    def run_by_type(self, source_type: str) -> list[dict[str, Any]]:
        """Run all ingestion sources of a given type."""
        configs = self.metadata_loader.load_by_type(source_type)
        results = []
        for config in configs:
            try:
                result = self._execute_ingestion(config)
                results.append(result)
            except Exception as e:
                logger.error(
                    f"Ingestion failed for {config.get('source_id')}: {e}",
                    exc_info=True,
                )
                results.append({
                    "source_id": config.get("source_id"),
                    "status": "failed",
                    "error": str(e),
                })
        return results

    def run_all(self) -> list[dict[str, Any]]:
        """Run ingestion for all enabled sources."""
        configs = self.metadata_loader.load_all()
        results = []
        for config in configs:
            try:
                result = self._execute_ingestion(config)
                results.append(result)
            except Exception as e:
                logger.error(
                    f"Ingestion failed for {config.get('source_id')}: {e}",
                    exc_info=True,
                )
                results.append({
                    "source_id": config.get("source_id"),
                    "status": "failed",
                    "error": str(e),
                })
        return results

    def _execute_ingestion(self, config: dict[str, Any]) -> dict[str, Any]:
        """Instantiate and run the appropriate source handler."""
        source_type = config.get("source_type")
        source_id = config.get("source_id", "unknown")

        if source_type not in SOURCE_REGISTRY:
            raise ValueError(
                f"Unknown source_type '{source_type}' for source '{source_id}'. "
                f"Available types: {list(SOURCE_REGISTRY.keys())}"
            )

        handler_class = SOURCE_REGISTRY[source_type]
        logger.info(
            f"Starting ingestion | source_id={source_id} | type={source_type} | "
            f"handler={handler_class.__name__}"
        )

        handler = handler_class(config=config, db_manager=self.db_manager)
        result = handler.ingest()

        logger.info(
            f"Completed ingestion | source_id={source_id} | "
            f"status={result.get('status')} | rows={result.get('rows_written', 0)}"
        )
        return result

    def get_registry(self) -> dict[str, str]:
        """Return the source type registry (for introspection/debugging)."""
        return {k: v.__name__ for k, v in SOURCE_REGISTRY.items()}

    def register_source_type(self, source_type: str, handler_class: type) -> None:
        """Register a new source type handler at runtime (extensibility)."""
        SOURCE_REGISTRY[source_type] = handler_class
        logger.info(f"Registered new source type: {source_type} -> {handler_class.__name__}")


# ─── Convenience factory ───────────────────────────────────────────────────────

def get_engine(
    metadata_dir: str | None = None,
    db_path: str | None = None,
) -> IngestionEngine:
    """Create and return a configured IngestionEngine instance."""
    return IngestionEngine(metadata_dir=metadata_dir, db_path=db_path)
