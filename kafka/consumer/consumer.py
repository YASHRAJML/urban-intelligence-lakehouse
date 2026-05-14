"""
Urban Intelligence Kafka Consumer
===================================
Continuously consumes messages from all urban event topics
and writes them to the bronze DuckDB layer in micro-batches.
Runs as a standalone Docker container (separate from Airflow).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka-consumer")

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/data/duckdb/urban_intelligence.duckdb")
BRONZE_PATH = os.getenv("BRONZE_PATH", "/data/bronze")
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))
FLUSH_INTERVAL_S = int(os.getenv("FLUSH_INTERVAL_S", "30"))

TOPICS = [
    "urban.traffic.events",
    "urban.pedestrian.events",
    "urban.air.events",
    "urban.orders.events",
]


def install_deps():
    import subprocess
    for pkg in ["duckdb==0.10.3", "kafka-python==2.0.2", "pandas==2.2.2", "pyarrow==16.1.0"]:
        subprocess.run([sys.executable, "-m", "pip", "install", "-q", pkg], capture_output=True)


def init_duckdb(db_path: str):
    """Initialize DuckDB schemas and bronze table."""
    import duckdb
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    conn.execute("CREATE SCHEMA IF NOT EXISTS bronze")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS bronze.bronze_urban_stream_events (
            event_id            VARCHAR,
            event_type          VARCHAR,
            sensor_id           VARCHAR,
            location_id         VARCHAR,
            latitude            DOUBLE,
            longitude           DOUBLE,
            event_timestamp     TIMESTAMPTZ,
            metric_value        DOUBLE,
            metric_unit         VARCHAR,
            kafka_topic         VARCHAR,
            kafka_partition     INTEGER,
            kafka_offset        BIGINT,
            raw_payload         VARCHAR,
            _ingestion_timestamp TIMESTAMPTZ,
            _ingestion_date     DATE,
            _source_id          VARCHAR,
            _source_type        VARCHAR,
            _batch_id           VARCHAR
        )
    """)
    conn.commit()
    conn.close()
    logger.info(f"DuckDB initialized: {db_path}")


def flush_batch(batch: list[dict], db_path: str, batch_id: str) -> int:
    """Write a batch of Kafka messages to DuckDB bronze table."""
    import duckdb
    import pandas as pd

    if not batch:
        return 0

    now = datetime.now(timezone.utc)
    rows = []
    for msg in batch:
        rows.append({
            "event_id":            msg.get("event_id", str(uuid.uuid4())),
            "event_type":          msg.get("event_type", "unknown"),
            "sensor_id":           msg.get("sensor_id", ""),
            "location_id":         msg.get("location_id", ""),
            "latitude":            msg.get("latitude"),
            "longitude":           msg.get("longitude"),
            "event_timestamp":     msg.get("timestamp", now.isoformat()),
            "metric_value":        msg.get("value"),
            "metric_unit":         msg.get("unit", ""),
            "kafka_topic":         msg.get("_kafka_topic", ""),
            "kafka_partition":     msg.get("_kafka_partition", 0),
            "kafka_offset":        msg.get("_kafka_offset", 0),
            "raw_payload":         json.dumps(msg.get("payload", {})),
            "_ingestion_timestamp": now.isoformat(),
            "_ingestion_date":     now.date().isoformat(),
            "_source_id":          "urban_events_kafka",
            "_source_type":        "kafka",
            "_batch_id":           batch_id,
        })

    df = pd.DataFrame(rows)
    conn = duckdb.connect(db_path)
    try:
        conn.register("_batch_df", df)
        conn.execute("INSERT INTO bronze.bronze_urban_stream_events SELECT * FROM _batch_df")
        conn.commit()
        conn.unregister("_batch_df")
        count = len(df)
        logger.info(f"Flushed {count} events to DuckDB | batch_id={batch_id}")
        return count
    finally:
        conn.close()


def wait_for_kafka(max_retries: int = 30) -> bool:
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable
    for attempt in range(1, max_retries + 1):
        try:
            c = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS, consumer_timeout_ms=1000)
            c.close()
            logger.info("Kafka is ready")
            return True
        except Exception:
            logger.warning(f"Kafka not ready ({attempt}/{max_retries}) — retrying in 5s")
            time.sleep(5)
    return False


def run_consumer():
    from kafka import KafkaConsumer
    from kafka.errors import KafkaError

    install_deps()
    init_duckdb(DUCKDB_PATH)

    if not wait_for_kafka():
        logger.error("Cannot connect to Kafka — exiting")
        sys.exit(1)

    consumer = KafkaConsumer(
        *TOPICS,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        group_id="urban-intelligence-streaming-consumer",
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        max_poll_records=BATCH_SIZE,
        session_timeout_ms=30000,
        heartbeat_interval_ms=10000,
    )

    running = True
    def _shutdown(sig, frame):
        nonlocal running
        logger.info("Shutdown signal received")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    batch: list[dict] = []
    last_flush = time.monotonic()
    total_consumed = 0

    logger.info(f"Consumer started | topics={TOPICS}")

    try:
        while running:
            records = consumer.poll(timeout_ms=1000, max_records=BATCH_SIZE)
            for tp, messages in records.items():
                for msg in messages:
                    event = msg.value
                    event["_kafka_topic"]     = msg.topic
                    event["_kafka_partition"] = msg.partition
                    event["_kafka_offset"]    = msg.offset
                    batch.append(event)

            now = time.monotonic()
            should_flush = len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL_S
            if batch and should_flush:
                batch_id = str(uuid.uuid4())
                written = flush_batch(batch, DUCKDB_PATH, batch_id)
                total_consumed += written
                batch.clear()
                consumer.commit()
                last_flush = now

                if total_consumed % 1000 == 0:
                    logger.info(f"Total consumed: {total_consumed} events")

    except KafkaError as e:
        logger.error(f"Kafka error: {e}", exc_info=True)
    except Exception as e:
        logger.error(f"Consumer error: {e}", exc_info=True)
    finally:
        # Flush remaining
        if batch:
            flush_batch(batch, DUCKDB_PATH, str(uuid.uuid4()))
            consumer.commit()
        consumer.close()
        logger.info(f"Consumer stopped. Total events consumed: {total_consumed}")


if __name__ == "__main__":
    run_consumer()
