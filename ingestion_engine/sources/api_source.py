"""
REST API ingestion handler.
Fetches data from REST API endpoints defined in metadata YAML config.
Supports: GET/POST, pagination, API key auth, JSONPath response mapping.
Falls back to synthetic data if the API is unavailable.
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from ingestion_engine.sources.base_source import BaseIngestionSource
from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)


class APIIngestionSource(BaseIngestionSource):
    """Ingests data from REST API endpoints."""

    def __init__(self, config: dict[str, Any], db_manager):
        super().__init__(config, db_manager)
        self.api_config = config.get("api", {})
        self.base_url = self.api_config.get("base_url", "")
        self.endpoint = self.api_config.get("endpoint", "")
        self.method = self.api_config.get("method", "GET").upper()
        self.timeout = self.api_config.get("timeout_seconds", 30)
        self.pagination = self.api_config.get("pagination", False)
        self.page_size = self.api_config.get("page_size", 100)
        self.max_pages = self.api_config.get("max_pages", 10)
        self.rate_limit = self.api_config.get("rate_limit_per_minute", 60)
        self._request_interval = 60.0 / self.rate_limit if self.rate_limit else 0

    def extract(self) -> pd.DataFrame:
        """Extract data from the REST API, with fallback to synthetic data."""
        try:
            records = self._fetch_all_pages()
            if records:
                df = pd.json_normalize(records)
                logger.info(f"[{self.source_id}] Fetched {len(df)} records from API")
                return df
            else:
                logger.warning(f"[{self.source_id}] Empty API response — using synthetic data")
                return self._generate_synthetic_data()
        except Exception as e:
            logger.warning(
                f"[{self.source_id}] API request failed: {e} — falling back to synthetic data"
            )
            return self._generate_synthetic_data()

    def _fetch_all_pages(self) -> list[dict]:
        """Fetch all pages of API data."""
        all_records: list[dict] = []
        url = self.base_url + self.endpoint

        if self.pagination:
            page = 1
            while page <= self.max_pages:
                records = self._fetch_page(url, page)
                if not records:
                    break
                all_records.extend(records)
                logger.info(
                    f"[{self.source_id}] Fetched page {page}: {len(records)} records"
                )
                page += 1
                if self._request_interval:
                    time.sleep(self._request_interval)
        else:
            # Single request with city/entity iteration
            entity_list = (
                self.api_config.get("city_list")
                or self.api_config.get("operator_list")
                or [{}]
            )
            for entity in entity_list:
                records = self._fetch_single(url, entity)
                if records:
                    all_records.extend(records if isinstance(records, list) else [records])
                if self._request_interval:
                    time.sleep(self._request_interval)

        return all_records

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=15),
        retry=retry_if_exception_type((requests.Timeout, requests.ConnectionError)),
    )
    def _fetch_single(self, url: str, entity: dict) -> list[dict] | dict | None:
        """Fetch a single API request."""
        import os

        params = {}
        params_template = self.api_config.get("params_template", {})

        # Resolve environment variables in params
        for k, v in params_template.items():
            if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                env_var = v[2:-1]
                params[k] = os.getenv(env_var, "")
            else:
                params[k] = v

        # Add entity-specific params
        if "lat" in entity and "lon" in entity:
            params["lat"] = entity["lat"]
            params["lon"] = entity["lon"]

        headers = self._build_headers()

        response = requests.request(
            method=self.method,
            url=url,
            params=params,
            headers=headers,
            timeout=self.timeout,
        )
        response.raise_for_status()
        data = response.json()

        # Flatten response if it's a dict with a data key
        if isinstance(data, dict):
            for key in ["data", "results", "routes", "features", "items"]:
                if key in data and isinstance(data[key], list):
                    return data[key]
            return [data]
        return data

    def _fetch_page(self, url: str, page: int) -> list[dict]:
        """Fetch a paginated page."""
        import os

        params = {}
        params_template = self.api_config.get("params_template", {})
        for k, v in params_template.items():
            if isinstance(v, str) and v.startswith("${") and v.endswith("}"):
                env_var = v[2:-1]
                params[k] = os.getenv(env_var, "")
            else:
                params[k] = v
        params["page"] = page
        params["per_page"] = self.page_size

        headers = self._build_headers()
        try:
            response = requests.request(
                method=self.method,
                url=url,
                params=params,
                headers=headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            data = response.json()
            if isinstance(data, list):
                return data
            for key in ["data", "results", "routes", "features"]:
                if key in data:
                    return data[key]
            return [data]
        except Exception as e:
            logger.warning(f"[{self.source_id}] Page {page} failed: {e}")
            return []

    def _build_headers(self) -> dict[str, str]:
        """Build request headers based on auth config."""
        import os

        headers: dict[str, str] = {
            "Accept": "application/json",
            "User-Agent": "UrbanIntelligenceLakehouse/1.0",
        }
        auth_type = self.api_config.get("auth_type", "none")
        auth_header = self.api_config.get("auth_header")
        if auth_type == "api_key" and auth_header:
            env_var = f"{self.source_id.upper()}_API_KEY"
            api_key = os.getenv(env_var, os.getenv("API_KEY", ""))
            if api_key:
                headers[auth_header] = api_key
        return headers

    def _generate_synthetic_data(self) -> pd.DataFrame:
        """Generate realistic synthetic API response data."""
        import numpy as np
        from faker import Faker

        fake = Faker()
        Faker.seed(42)
        np.random.seed(42)

        source_id = self.source_id
        logger.info(f"[{source_id}] Generating synthetic API data")

        if "weather" in source_id:
            return self._synthetic_weather()
        elif "transit" in source_id:
            return self._synthetic_transit()
        else:
            n = 200
            return pd.DataFrame({
                "id": range(n),
                "name": [fake.name() for _ in range(n)],
                "value": np.random.random(n).round(4),
                "timestamp": pd.date_range("2024-01-01", periods=n, freq="h"),
                "status": np.random.choice(["active", "inactive", "pending"], n),
            })

    def _synthetic_weather(self) -> pd.DataFrame:
        """Synthetic weather API response data."""
        import numpy as np

        cities = [
            {"city": "New York", "lat": 40.71, "lon": -74.01},
            {"city": "London", "lat": 51.51, "lon": -0.13},
            {"city": "Tokyo", "lat": 35.68, "lon": 139.65},
            {"city": "Mumbai", "lat": 19.08, "lon": 72.88},
            {"city": "Berlin", "lat": 52.52, "lon": 13.41},
            {"city": "Sydney", "lat": -33.87, "lon": 151.21},
            {"city": "Dubai", "lat": 25.20, "lon": 55.27},
            {"city": "Singapore", "lat": 1.35, "lon": 103.82},
        ]
        conditions = ["Clear", "Clouds", "Rain", "Snow", "Mist"]
        rows = []
        now = int(datetime.now(timezone.utc).timestamp())
        for city in cities:
            rows.append({
                "name": city["city"],
                "coord.lat": city["lat"],
                "coord.lon": city["lon"],
                "main.temp": round(np.random.uniform(5, 40), 2),
                "main.feels_like": round(np.random.uniform(3, 38), 2),
                "main.humidity": np.random.randint(30, 95),
                "main.pressure": np.random.randint(990, 1030),
                "wind.speed": round(np.random.uniform(0.5, 15), 2),
                "wind.deg": np.random.randint(0, 360),
                "weather[0].main": np.random.choice(conditions),
                "weather[0].description": "synthetic weather data",
                "visibility": np.random.randint(5000, 10000),
                "dt": now,
            })
        return pd.DataFrame(rows)

    def _synthetic_transit(self) -> pd.DataFrame:
        """Synthetic transit routes data."""
        import numpy as np

        route_types = [0, 1, 2, 3]  # tram, metro, rail, bus
        operators = ["BART", "London Underground", "Tokyo Metro", "NYC MTA", "TfL"]
        n = 150
        np.random.seed(42)
        return pd.DataFrame({
            "id": [f"route_{i:04d}" for i in range(n)],
            "onestop_id": [f"r-abc{i}-route{i}" for i in range(n)],
            "name": [f"Route {i}" for i in range(n)],
            "route_long_name": [f"Urban Route {i} - City Center Express" for i in range(n)],
            "route_type": np.random.choice(route_types, n),
            "route_color": [f"#{np.random.randint(0,16777215):06X}" for _ in range(n)],
            "operated_by_name": np.random.choice(operators, n),
            "geometry.type": "LineString",
        })
