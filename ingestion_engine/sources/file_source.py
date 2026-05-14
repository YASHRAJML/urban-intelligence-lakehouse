"""
File-based ingestion handler.
Reads CSV, JSON, and Parquet files from local paths defined in metadata.
"""

from __future__ import annotations

import glob
from pathlib import Path
from typing import Any

import pandas as pd

from ingestion_engine.sources.base_source import BaseIngestionSource
from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)

# Reader registry: format -> reader function
FORMAT_READERS = {
    "csv": lambda path, cfg: pd.read_csv(
        path,
        encoding=cfg.get("encoding", "utf-8"),
        sep=cfg.get("delimiter", ","),
        header=0 if cfg.get("has_header", True) else None,
        skiprows=cfg.get("skip_rows", 0),
        quotechar=cfg.get("quote_char", '"'),
        low_memory=False,
    ),
    "json": lambda path, cfg: pd.read_json(path, lines=cfg.get("json_lines", False)),
    "parquet": lambda path, cfg: pd.read_parquet(path),
    "jsonl": lambda path, cfg: pd.read_json(path, lines=True),
    "ndjson": lambda path, cfg: pd.read_json(path, lines=True),
}


class FileIngestionSource(BaseIngestionSource):
    """Ingests data from local file system (CSV, JSON, Parquet)."""

    def __init__(self, config: dict[str, Any], db_manager):
        super().__init__(config, db_manager)
        self.file_config = config.get("file", {})
        self.base_path = Path(
            self.file_config.get("base_path", "/opt/airflow/data/raw")
        )
        self.pattern = self.file_config.get("pattern", "*.csv")
        self.recursive = self.file_config.get("recursive", False)
        self.fmt = self.format.lower()

    def extract(self) -> pd.DataFrame:
        """Discover and read matching files from the configured path."""
        files = self._discover_files()

        if not files:
            logger.warning(
                f"[{self.source_id}] No files found matching pattern "
                f"'{self.pattern}' in '{self.base_path}' — generating synthetic data"
            )
            return self._generate_synthetic_demographics()

        dfs: list[pd.DataFrame] = []
        for file_path in files:
            try:
                df = self._read_file(file_path)
                df["_source_file"] = str(file_path.name)
                dfs.append(df)
                logger.info(
                    f"[{self.source_id}] Read {len(df)} rows from {file_path.name}"
                )
            except Exception as e:
                logger.error(
                    f"[{self.source_id}] Failed to read {file_path}: {e}",
                    exc_info=True,
                )

        if not dfs:
            raise RuntimeError(f"[{self.source_id}] All file reads failed")

        combined = pd.concat(dfs, ignore_index=True)
        logger.info(
            f"[{self.source_id}] Combined {len(dfs)} files → {len(combined)} total rows"
        )
        return combined

    def _discover_files(self) -> list[Path]:
        """Find all matching files."""
        if not self.base_path.exists():
            self.base_path.mkdir(parents=True, exist_ok=True)
            return []

        if self.recursive:
            files = list(self.base_path.rglob(self.pattern))
        else:
            files = list(self.base_path.glob(self.pattern))

        files = sorted(files)
        logger.info(f"[{self.source_id}] Discovered {len(files)} files in {self.base_path}")
        return files

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Read a file based on its format."""
        fmt = self.fmt
        # Auto-detect from extension if format is generic
        ext = path.suffix.lower().lstrip(".")
        if ext in FORMAT_READERS:
            fmt = ext

        reader = FORMAT_READERS.get(fmt)
        if reader is None:
            raise ValueError(
                f"[{self.source_id}] Unsupported format: {fmt}. "
                f"Supported: {list(FORMAT_READERS.keys())}"
            )
        return reader(path, self.file_config)

    def _generate_synthetic_demographics(self) -> pd.DataFrame:
        """Generate synthetic US city demographics data."""
        import numpy as np

        np.random.seed(42)
        cities = [
            ("New York City", "New York", "NY"),
            ("Los Angeles", "California", "CA"),
            ("Chicago", "Illinois", "IL"),
            ("Houston", "Texas", "TX"),
            ("Phoenix", "Arizona", "AZ"),
            ("Philadelphia", "Pennsylvania", "PA"),
            ("San Antonio", "Texas", "TX"),
            ("San Diego", "California", "CA"),
            ("Dallas", "Texas", "TX"),
            ("San Jose", "California", "CA"),
            ("Austin", "Texas", "TX"),
            ("Jacksonville", "Florida", "FL"),
            ("San Francisco", "California", "CA"),
            ("Columbus", "Ohio", "OH"),
            ("Charlotte", "North Carolina", "NC"),
        ]
        races = ["White", "Black or African-American", "Hispanic or Latino", "Asian", "Other"]

        rows = []
        for city, state, code in cities:
            total_pop = np.random.randint(200_000, 8_500_000)
            male_pop = int(total_pop * np.random.uniform(0.47, 0.52))
            female_pop = total_pop - male_pop
            for race in races:
                rows.append({
                    "City": city,
                    "State": state,
                    "Median Age": round(np.random.uniform(28, 45), 1),
                    "Male Population": male_pop,
                    "Female Population": female_pop,
                    "Total Population": total_pop,
                    "Number of Veterans": np.random.randint(5000, 200000),
                    "Foreign-born": np.random.randint(10000, 3000000),
                    "Average Household Size": round(np.random.uniform(2.1, 3.5), 2),
                    "State Code": code,
                    "Race": race,
                    "Count": np.random.randint(1000, 2000000),
                })
        # Write the generated data so it's cached
        self.base_path.mkdir(parents=True, exist_ok=True)
        df = pd.DataFrame(rows)
        df.to_csv(self.base_path / "city_demographics_synthetic.csv", index=False)
        logger.info(f"[{self.source_id}] Generated {len(df)} synthetic demographic rows")
        return df
