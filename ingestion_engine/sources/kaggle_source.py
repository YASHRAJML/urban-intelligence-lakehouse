"""
Kaggle dataset ingestion handler.
Downloads datasets from Kaggle using the Kaggle API or generates synthetic data as fallback.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

from ingestion_engine.sources.base_source import BaseIngestionSource
from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)


class KaggleIngestionSource(BaseIngestionSource):
    """Ingests datasets from Kaggle or uses synthetic fallback data."""

    def __init__(self, config: dict[str, Any], db_manager):
        super().__init__(config, db_manager)
        self.kaggle_config = config.get("kaggle", {})
        self.download_path = Path(
            self.kaggle_config.get("download_path", "/opt/airflow/data/raw/kaggle")
        )
        self.dataset = self.kaggle_config.get("dataset", "")
        self.filename = self.kaggle_config.get("filename", "")

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=30),
        retry=retry_if_exception_type(Exception),
    )
    def extract(self) -> pd.DataFrame:
        """Download from Kaggle if credentials exist, else generate synthetic data."""
        self.download_path.mkdir(parents=True, exist_ok=True)
        target_file = self.download_path / self.filename

        if target_file.exists():
            logger.info(f"[{self.source_id}] Using cached file: {target_file}")
            return self._read_file(target_file)

        if self._has_kaggle_credentials():
            logger.info(f"[{self.source_id}] Downloading from Kaggle: {self.dataset}")
            return self._download_kaggle(target_file)
        else:
            logger.warning(
                f"[{self.source_id}] No Kaggle credentials found — generating synthetic data"
            )
            return self._generate_synthetic_data()

    def _has_kaggle_credentials(self) -> bool:
        """Check if Kaggle API credentials are configured."""
        kaggle_json = Path.home() / ".kaggle" / "kaggle.json"
        has_env = bool(
            os.getenv("KAGGLE_USERNAME") and os.getenv("KAGGLE_KEY")
        )
        return kaggle_json.exists() or has_env

    def _download_kaggle(self, target_file: Path) -> pd.DataFrame:
        """Run Kaggle CLI to download dataset."""
        try:
            cmd = [
                "kaggle", "datasets", "download",
                "-d", self.dataset,
                "-p", str(self.download_path),
                "--unzip",
            ]
            result = subprocess.run(
                cmd, capture_output=True, text=True, timeout=300
            )
            if result.returncode != 0:
                raise RuntimeError(f"Kaggle CLI error: {result.stderr}")

            if target_file.exists():
                return self._read_file(target_file)
            else:
                # Try to find the downloaded file
                csv_files = list(self.download_path.glob("*.csv"))
                if csv_files:
                    return self._read_file(csv_files[0])
                raise FileNotFoundError(f"Downloaded file not found: {target_file}")

        except FileNotFoundError:
            logger.warning(f"[{self.source_id}] Kaggle CLI not found — falling back to synthetic data")
            return self._generate_synthetic_data()

    def _read_file(self, path: Path) -> pd.DataFrame:
        """Read CSV or Parquet file."""
        if path.suffix.lower() == ".parquet":
            return pd.read_parquet(path)
        return pd.read_csv(path, low_memory=False)

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate synthetic dataset matching the expected schema."""
        import numpy as np
        from faker import Faker

        fake = Faker()
        Faker.seed(42)
        np.random.seed(42)

        source_id = self.source_id
        logger.info(f"[{source_id}] Generating synthetic data for: {source_id}")

        if "traffic" in source_id:
            return self._synthetic_traffic_data(fake, np)
        elif "air_quality" in source_id:
            return self._synthetic_air_quality_data(fake, np)
        else:
            return self._synthetic_generic_data(fake, np)

    def _synthetic_traffic_data(self, fake, np) -> pd.DataFrame:
        """Generate synthetic metro traffic volume data."""
        n = 5000
        dates = pd.date_range("2023-01-01", periods=n, freq="h")
        weather_main = ["Clear", "Clouds", "Rain", "Snow", "Mist", "Drizzle"]
        weather_desc = {
            "Clear": ["sky is clear"],
            "Clouds": ["few clouds", "scattered clouds", "broken clouds", "overcast clouds"],
            "Rain": ["light rain", "moderate rain", "heavy intensity rain"],
            "Snow": ["light snow", "moderate snow"],
            "Mist": ["mist"],
            "Drizzle": ["light intensity drizzle"],
        }
        w_main = np.random.choice(weather_main, n)
        w_desc = [np.random.choice(weather_desc[w]) for w in w_main]
        base_traffic = 3000
        hour_factor = np.sin(np.pi * dates.hour / 12) * 1500 + base_traffic
        traffic = np.maximum(
            0, hour_factor + np.random.normal(0, 300, n)
        ).astype(int)

        return pd.DataFrame({
            "date_time": dates,
            "holiday": np.where(np.random.random(n) < 0.03, "Holiday", None),
            "temp": np.random.uniform(250, 310, n).round(2),
            "rain_1h": np.where(w_main == "Rain", np.random.exponential(2, n), 0).round(2),
            "snow_1h": np.where(w_main == "Snow", np.random.exponential(1, n), 0).round(2),
            "clouds_all": np.random.randint(0, 100, n),
            "weather_main": w_main,
            "weather_description": w_desc,
            "traffic_volume": traffic,
        })

    def _synthetic_air_quality_data(self, fake, np) -> pd.DataFrame:
        """Generate synthetic air quality data."""
        cities = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Kolkata", "Pune", "Ahmedabad"]
        n = 8000
        city_choices = np.random.choice(cities, n)
        dates = pd.date_range("2023-01-01", periods=n // len(cities), freq="h")
        dates = np.tile(dates, len(cities))[:n]

        aqi_values = np.random.exponential(100, n).clip(0, 999).round(2)
        aqi_buckets = pd.cut(
            aqi_values,
            bins=[0, 50, 100, 200, 300, 400, 1000],
            labels=["Good", "Satisfactory", "Moderate", "Poor", "Very Poor", "Severe"],
        )

        return pd.DataFrame({
            "City": city_choices,
            "Datetime": dates,
            "PM2.5": np.random.exponential(60, n).clip(0, 999).round(2),
            "PM10": np.random.exponential(80, n).clip(0, 999).round(2),
            "NO": np.random.exponential(5, n).clip(0, 200).round(2),
            "NO2": np.random.exponential(20, n).clip(0, 200).round(2),
            "NOx": np.random.exponential(25, n).clip(0, 200).round(2),
            "NH3": np.random.exponential(10, n).clip(0, 100).round(2),
            "CO": np.random.exponential(1, n).clip(0, 50).round(2),
            "SO2": np.random.exponential(15, n).clip(0, 200).round(2),
            "O3": np.random.exponential(30, n).clip(0, 200).round(2),
            "Benzene": np.random.exponential(3, n).clip(0, 100).round(2),
            "Toluene": np.random.exponential(5, n).clip(0, 100).round(2),
            "Xylene": np.random.exponential(2, n).clip(0, 100).round(2),
            "AQI": aqi_values,
            "AQI_Bucket": aqi_buckets.astype(str),
        })

    def _synthetic_generic_data(self, fake, np) -> pd.DataFrame:
        """Generic synthetic dataset for unknown source types."""
        n = 1000
        return pd.DataFrame({
            "id": range(n),
            "name": [fake.name() for _ in range(n)],
            "value": np.random.random(n).round(4),
            "timestamp": pd.date_range("2023-01-01", periods=n, freq="h"),
        })
