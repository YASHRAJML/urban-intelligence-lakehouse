#!/usr/bin/env python3
"""
Bootstrap script — generates all sample datasets for the lakehouse.
Run this before docker-compose up to pre-populate raw data.
"""

from __future__ import annotations

import csv
import math
import json
import os
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"


def _gauss(mu: float, sigma: float) -> float:
    return random.gauss(mu, sigma)


def _exponential(scale: float) -> float:
    """Exponential distribution via inverse CDF (mean = scale)."""
    return random.expovariate(1.0 / scale)


def make_dirs():
    for d in ["raw/traffic", "raw/air_quality", "raw/demographics", "bronze", "silver", "gold", "duckdb"]:
        (DATA_DIR / d).mkdir(parents=True, exist_ok=True)
    print("[OK] Data directories created")


def generate_traffic_csv():
    """Generate Metro Interstate Traffic Volume CSV."""
    path = DATA_DIR / "raw" / "traffic" / "Metro_Interstate_Traffic_Volume.csv"
    if path.exists():
        print(f"[SKIP] Traffic CSV already exists: {path}")
        return

    weather_conditions = ["Clear", "Clouds", "Rain", "Snow", "Mist", "Drizzle"]
    weather_desc = {
        "Clear": "sky is clear", "Clouds": "broken clouds",
        "Rain": "moderate rain", "Snow": "light snow",
        "Mist": "mist", "Drizzle": "light intensity drizzle",
    }
    start = datetime(2016, 10, 2, 9, 0, 0)
    rows = []
    for i in range(10000):
        dt = start + timedelta(hours=i)
        w = random.choice(weather_conditions)
        hour = dt.hour
        base = 3000
        traffic = max(0, int(base + math.sin(math.pi * hour / 12) * 1500 + _gauss(0, 300)))
        rows.append({
            "date_time": dt.strftime("%Y-%m-%d %H:%M:%S"),
            "holiday": "None" if random.random() > 0.03 else "Columbus Day",
            "temp": round(random.uniform(250, 310), 2),
            "rain_1h": round(_exponential(2) if w == "Rain" else 0, 2),
            "snow_1h": round(_exponential(1) if w == "Snow" else 0, 2),
            "clouds_all": random.randint(0, 100),
            "weather_main": w,
            "weather_description": weather_desc[w],
            "traffic_volume": traffic,
        })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Traffic CSV: {len(rows)} rows -> {path}")


def generate_air_quality_csv():
    """Generate city-hour air quality CSV."""
    path = DATA_DIR / "raw" / "air_quality" / "city_hour.csv"
    if path.exists():
        print(f"[SKIP] Air quality CSV already exists: {path}")
        return

    cities = ["Mumbai", "Delhi", "Bangalore", "Chennai", "Hyderabad", "Kolkata", "Pune", "Ahmedabad"]
    rows = []
    start = datetime(2023, 1, 1)
    for city in cities:
        for hour in range(8760):  # 1 year hourly
            dt = start + timedelta(hours=hour)
            pm25 = max(0, random.gauss(80, 40))
            aqi = min(999, pm25 * 2.1)
            if aqi <= 50:
                cat = "Good"
            elif aqi <= 100:
                cat = "Satisfactory"
            elif aqi <= 200:
                cat = "Moderate"
            elif aqi <= 300:
                cat = "Poor"
            elif aqi <= 400:
                cat = "Very Poor"
            else:
                cat = "Severe"
            rows.append({
                "City": city, "Datetime": dt.strftime("%Y-%m-%d %H:%M:%S"),
                "PM2.5": round(pm25, 2), "PM10": round(max(0, random.gauss(100, 50)), 2),
                "NO": round(max(0, random.gauss(5, 3)), 2),
                "NO2": round(max(0, random.gauss(20, 10)), 2),
                "NOx": round(max(0, random.gauss(25, 12)), 2),
                "NH3": round(max(0, random.gauss(8, 4)), 2),
                "CO": round(max(0, random.gauss(1.2, 0.6)), 3),
                "SO2": round(max(0, random.gauss(12, 8)), 2),
                "O3": round(max(0, random.gauss(35, 15)), 2),
                "Benzene": round(max(0, random.gauss(3, 1.5)), 2),
                "Toluene": round(max(0, random.gauss(5, 2)), 2),
                "Xylene": round(max(0, random.gauss(2, 1)), 2),
                "AQI": round(aqi, 2), "AQI_Bucket": cat,
            })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Air quality CSV: {len(rows)} rows -> {path}")


def generate_demographics_csv():
    """Generate US cities demographics CSV."""
    path = DATA_DIR / "raw" / "demographics" / "city_demographics_2023.csv"
    if path.exists():
        print(f"[SKIP] Demographics CSV already exists: {path}")
        return

    cities = [
        ("New York City", "New York", "NY", 8_336_817),
        ("Los Angeles",   "California",  "CA", 3_979_576),
        ("Chicago",        "Illinois",    "IL", 2_693_976),
        ("Houston",        "Texas",       "TX", 2_304_580),
        ("Phoenix",        "Arizona",     "AZ", 1_608_139),
        ("Philadelphia",   "Pennsylvania","PA", 1_584_064),
        ("San Antonio",    "Texas",       "TX", 1_434_625),
        ("San Diego",      "California",  "CA", 1_386_932),
        ("Dallas",         "Texas",       "TX", 1_304_379),
        ("San Jose",       "California",  "CA", 1_013_240),
    ]
    races = ["White", "Black or African-American", "Hispanic or Latino", "Asian", "Other"]
    rows = []
    for city, state, code, total_pop in cities:
        for race in races:
            rows.append({
                "City": city, "State": state,
                "Median Age": round(random.uniform(30, 42), 1),
                "Male Population": int(total_pop * 0.49),
                "Female Population": int(total_pop * 0.51),
                "Total Population": total_pop,
                "Number of Veterans": random.randint(5000, 200000),
                "Foreign-born": random.randint(50000, 3000000),
                "Average Household Size": round(random.uniform(2.2, 3.5), 2),
                "State Code": code,
                "Race": race,
                "Count": random.randint(10000, 2000000),
            })

    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    print(f"[OK] Demographics CSV: {len(rows)} rows -> {path}")


if __name__ == "__main__":
    random.seed(42)
    make_dirs()
    generate_traffic_csv()
    generate_air_quality_csv()
    generate_demographics_csv()
    print("\n[DONE] All sample datasets generated successfully!")
    print(f"[DIR] Data directory: {DATA_DIR}")
