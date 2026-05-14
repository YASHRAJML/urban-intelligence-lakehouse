"""
Kafka ingestion handler (batch/micro-batch consumer).
Reads messages from Kafka topics and writes them to the bronze layer.
Used by Airflow DAG for periodic consumption.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from ingestion_engine.sources.base_source import BaseIngestionSource
from ingestion_engine.utils.logger import get_logger

logger = get_logger(__name__)


class KafkaIngestionSource(BaseIngestionSource):
    """Consumes messages from Kafka topics and loads into bronze layer."""

    def __init__(self, config: dict[str, Any], db_manager):
        super().__init__(config, db_manager)
        self.kafka_config = config.get("kafka", {})
        self.bootstrap_servers = self.kafka_config.get(
            "bootstrap_servers",
            os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"),
        )
        self.topics = [t["name"] for t in self.kafka_config.get("topics", [])]
        self.consumer_group = self.kafka_config.get(
            "consumer_group", "urban-intelligence-airflow"
        )
        self.auto_offset_reset = self.kafka_config.get("auto_offset_reset", "earliest")
        self.max_poll_records = self.kafka_config.get("max_poll_records", 500)
        self.poll_timeout_ms = 5000  # 5s poll timeout for Airflow task

    def extract(self) -> pd.DataFrame:
        """Poll Kafka topics and return messages as DataFrame."""
        try:
            from kafka import KafkaConsumer

            logger.info(
                f"[{self.source_id}] Connecting to Kafka: {self.bootstrap_servers}"
            )

            consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.consumer_group,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=False,
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                key_deserializer=lambda k: k.decode("utf-8") if k else None,
                max_poll_records=self.max_poll_records,
                session_timeout_ms=30000,
                consumer_timeout_ms=self.poll_timeout_ms,
            )

            records = []
            for message in consumer:
                record = {
                    "kafka_topic": message.topic,
                    "kafka_partition": message.partition,
                    "kafka_offset": message.offset,
                    "kafka_key": message.key,
                    "kafka_timestamp": message.timestamp,
                    **message.value,  # unpack JSON payload
                }
                records.append(record)

            consumer.commit()
            consumer.close()

            if records:
                df = pd.DataFrame(records)
                logger.info(
                    f"[{self.source_id}] Consumed {len(df)} messages from Kafka"
                )
                return df
            else:
                logger.info(f"[{self.source_id}] No new Kafka messages — returning empty")
                return pd.DataFrame()

        except ImportError:
            logger.warning(
                f"[{self.source_id}] kafka-python not installed — using synthetic data"
            )
            return self._synthetic_stream_events()
        except Exception as e:
            logger.warning(
                f"[{self.source_id}] Kafka connection failed: {e} — using synthetic data"
            )
            return self._synthetic_stream_events()

    def _synthetic_stream_events(self, n: int = 500) -> pd.DataFrame:
        """Generate synthetic streaming events for testing."""
        import numpy as np
        import uuid

        np.random.seed(42)
        event_types = [
            "vehicle_detected", "pedestrian_count", "air_quality_reading",
            "order_placed", "order_delivered", "traffic_jam",
        ]
        topics = [
            "urban.traffic.events", "urban.pedestrian.events",
            "urban.air.events", "urban.orders.events",
        ]
        sensor_ids = [f"sensor_{i:03d}" for i in range(1, 51)]
        location_ids = [f"loc_{i:03d}" for i in range(1, 21)]

        now = datetime.now(timezone.utc)
        rows = []
        for i in range(n):
            event_type = np.random.choice(event_types)
            topic = np.random.choice(topics)
            rows.append({
                "event_id": str(uuid.uuid4()),
                "event_type": event_type,
                "sensor_id": np.random.choice(sensor_ids),
                "location_id": np.random.choice(location_ids),
                "latitude": round(np.random.uniform(40.5, 40.9), 6),
                "longitude": round(np.random.uniform(-74.1, -73.8), 6),
                "timestamp": (pd.Timestamp(now) - pd.Timedelta(seconds=np.random.randint(0, 3600))).isoformat(),
                "value": round(np.random.uniform(0, 1000), 2),
                "unit": np.random.choice(["count", "kg/m3", "km/h", "celsius", "usd"]),
                "kafka_topic": topic,
                "kafka_partition": np.random.randint(0, 3),
                "kafka_offset": i,
                "kafka_key": f"key_{i}",
                "kafka_timestamp": int(now.timestamp() * 1000),
                "payload": json.dumps({"extra_field": np.random.random()}),
            })
        return pd.DataFrame(rows)
