"""
Urban Intelligence Kafka Producer
===================================
Simulates real-time IoT events from urban sensors:
- Vehicle detection events
- Pedestrian counting sensors
- Air quality sensors
- Delivery/order events

Runs continuously in Docker, producing events at configurable intervals.
"""

from __future__ import annotations

import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("kafka-producer")

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
PRODUCE_INTERVAL_MS = int(os.getenv("PRODUCE_INTERVAL_MS", "500"))

TOPICS = {
    "traffic":    "urban.traffic.events",
    "pedestrian": "urban.pedestrian.events",
    "air":        "urban.air.events",
    "orders":     "urban.orders.events",
}

# ── City grid ──────────────────────────────────────────────────────────────────
LOCATIONS = [
    {"id": f"loc_{i:03d}", "lat": 40.7128 + random.uniform(-0.05, 0.05),
     "lon": -74.0060 + random.uniform(-0.05, 0.05)}
    for i in range(1, 51)
]
SENSORS = [f"sensor_{i:03d}" for i in range(1, 101)]
VEHICLE_TYPES = ["car", "truck", "motorcycle", "bus", "bicycle"]
ORDER_STATUSES = ["placed", "confirmed", "dispatched", "delivered", "cancelled"]


# ── Event generators ────────────────────────────────────────────────────────────

def make_traffic_event() -> dict:
    loc = random.choice(LOCATIONS)
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "vehicle_detected",
        "sensor_id":   random.choice(SENSORS),
        "location_id": loc["id"],
        "latitude":    round(loc["lat"], 6),
        "longitude":   round(loc["lon"], 6),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "value":       random.randint(0, 120),          # speed km/h
        "unit":        "km/h",
        "vehicle_type": random.choice(VEHICLE_TYPES),
        "direction":   random.choice(["N", "S", "E", "W", "NE", "NW", "SE", "SW"]),
        "lane":        random.randint(1, 4),
        "payload":     {"confidence": round(random.uniform(0.75, 0.99), 3)},
    }


def make_pedestrian_event() -> dict:
    loc = random.choice(LOCATIONS)
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "pedestrian_count",
        "sensor_id":   random.choice(SENSORS),
        "location_id": loc["id"],
        "latitude":    round(loc["lat"], 6),
        "longitude":   round(loc["lon"], 6),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "value":       random.randint(0, 200),          # count per minute
        "unit":        "count/min",
        "direction":   random.choice(["inbound", "outbound", "both"]),
        "payload":     {"zone": random.choice(["commercial", "residential", "transit"])},
    }


def make_air_quality_event() -> dict:
    loc = random.choice(LOCATIONS)
    pm25 = random.gauss(50, 30)
    aqi  = min(500, max(0, pm25 * 2))
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "air_quality_reading",
        "sensor_id":   random.choice(SENSORS),
        "location_id": loc["id"],
        "latitude":    round(loc["lat"], 6),
        "longitude":   round(loc["lon"], 6),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "value":       round(aqi, 2),
        "unit":        "AQI",
        "pm25":        round(max(0, pm25), 2),
        "pm10":        round(max(0, random.gauss(70, 35)), 2),
        "co":          round(max(0, random.gauss(1, 0.5)), 3),
        "no2":         round(max(0, random.gauss(25, 15)), 2),
        "payload":     {"battery_pct": random.randint(10, 100)},
    }


def make_order_event() -> dict:
    loc = random.choice(LOCATIONS)
    return {
        "event_id":    str(uuid.uuid4()),
        "event_type":  "order_" + random.choice(ORDER_STATUSES),
        "sensor_id":   f"api_gateway_{random.randint(1,5):02d}",
        "location_id": loc["id"],
        "latitude":    round(loc["lat"], 6),
        "longitude":   round(loc["lon"], 6),
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "value":       round(random.uniform(5, 500), 2),  # order value USD
        "unit":        "USD",
        "category":    random.choice(["food", "grocery", "pharmacy", "electronics", "clothing"]),
        "payload": {
            "customer_zone": random.choice(["zone_A", "zone_B", "zone_C", "zone_D"]),
            "delivery_minutes": random.randint(15, 120),
        },
    }


EVENT_GENERATORS = {
    "traffic":    (make_traffic_event,    TOPICS["traffic"],    0.40),
    "pedestrian": (make_pedestrian_event, TOPICS["pedestrian"], 0.25),
    "air":        (make_air_quality_event,TOPICS["air"],        0.20),
    "orders":     (make_order_event,      TOPICS["orders"],     0.15),
}


# ── Main producer loop ─────────────────────────────────────────────────────────

def wait_for_kafka(bootstrap_servers: str, max_retries: int = 30) -> bool:
    from kafka import KafkaProducer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, max_retries + 1):
        try:
            p = KafkaProducer(bootstrap_servers=bootstrap_servers)
            p.close()
            logger.info(f"Kafka is ready at {bootstrap_servers}")
            return True
        except NoBrokersAvailable:
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_retries}) — retrying in 5s")
            time.sleep(5)
    return False


def run_producer():
    from kafka import KafkaProducer
    from kafka.errors import KafkaError

    if not wait_for_kafka(KAFKA_BOOTSTRAP_SERVERS):
        logger.error("Could not connect to Kafka — exiting")
        sys.exit(1)

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        compression_type="gzip",
    )

    # Graceful shutdown
    running = True
    def _shutdown(sig, frame):
        nonlocal running
        logger.info("Shutting down producer...")
        running = False

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    produced = 0
    weights = [v[2] for v in EVENT_GENERATORS.values()]
    event_types = list(EVENT_GENERATORS.keys())
    interval_s = PRODUCE_INTERVAL_MS / 1000.0

    logger.info(
        f"Producer started | bootstrap={KAFKA_BOOTSTRAP_SERVERS} | interval={interval_s}s"
    )

    while running:
        try:
            event_type = random.choices(event_types, weights=weights, k=1)[0]
            generator, topic, _ = EVENT_GENERATORS[event_type]
            event = generator()

            future = producer.send(
                topic=topic,
                key=event["event_id"],
                value=event,
            )
            produced += 1

            if produced % 100 == 0:
                logger.info(f"Produced {produced} events | last_topic={topic}")

        except KafkaError as e:
            logger.error(f"Kafka send error: {e}")
            time.sleep(1)
        except Exception as e:
            logger.error(f"Unexpected producer error: {e}", exc_info=True)
            time.sleep(1)

        time.sleep(interval_s)

    producer.flush()
    producer.close()
    logger.info(f"Producer stopped. Total events produced: {produced}")


if __name__ == "__main__":
    run_producer()
