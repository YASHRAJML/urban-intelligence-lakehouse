# 🏙️ Unified Real-Time Data Lakehouse for Urban Intelligence

A production-style **modern data platform** combining batch + streaming + metadata-driven ingestion with a full **Medallion Architecture** (Bronze → Silver → Gold), running entirely locally with a single `docker-compose up`.

---

## 🏗️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    DATA SOURCES                                              │
├──────────────┬──────────────┬──────────────────┬───────────────────────────┤
│  Kaggle CSV  │  REST APIs   │  Kafka Streaming  │  Local Files (CSV/JSON)   │
│  - Traffic   │  - Weather   │  - IoT Sensors    │  - Demographics           │
│  - AirQual   │  - Transit   │  - Orders         │  - (extensible)           │
└──────┬───────┴──────┬───────┴────────┬──────────┴────────────┬──────────────┘
       │              │                │                        │
       ▼              ▼                ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  METADATA-DRIVEN INGESTION ENGINE                            │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │  YAML Metadata Config → Engine → Source Registry → Handler Dispatch  │   │
│  │                                                                       │   │
│  │  kaggle_source.py  │  api_source.py  │  kafka_source.py  │ file_source│   │
│  └──────────────────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER (Raw)                DuckDB: bronze.*                      │
│  ┌────────────────┐ ┌──────────────┐ ┌────────────────┐ ┌────────────────┐  │
│  │ bronze_urban   │ │ bronze_air   │ │ bronze_weather │ │ bronze_stream  │  │
│  │ _traffic       │ │ _quality     │ │ _api           │ │ _events        │  │
│  └────────────────┘ └──────────────┘ └────────────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
       │  (Airflow DAG 03 / dbt staging models)
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER (Cleaned)            DuckDB: silver.*                      │
│  ┌────────────────┐ ┌──────────────┐ ┌────────────────┐ ┌────────────────┐  │
│  │ silver_traffic │ │ silver_air   │ │ silver_weather │ │ silver_stream  │  │
│  │                │ │ _quality     │ │                │ │ _events        │  │
│  └────────────────┘ └──────────────┘ └────────────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
       │  (Airflow DAG 04 / dbt mart models)
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER (Analytics-Ready)      DuckDB: gold.*                        │
│  ┌────────────────┐ ┌──────────────┐ ┌────────────────┐ ┌────────────────┐  │
│  │ fact_traffic   │ │ fact_air     │ │ fact_stream    │ │ dim_date       │  │
│  │ _hourly        │ │ _quality_    │ │ _events_hourly │ │                │  │
│  │                │ │ daily        │ │                │ │ urban_intelli  │  │
│  └────────────────┘ └──────────────┘ └────────────────┘ │ gence_daily    │  │
│                                                           └────────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    CONSUMPTION LAYER                                         │
│  ┌──────────────────────────┐    ┌──────────────────────────────────────┐   │
│  │  Metabase Dashboards     │    │  Direct SQL / dbt / Analytics        │   │
│  │  localhost:3000          │    │  DuckDB CLI / Python                 │   │
│  └──────────────────────────┘    └──────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 📦 Tech Stack

| Component | Technology | Version | Purpose |
|-----------|-----------|---------|---------|
| Orchestration | Apache Airflow | 2.9.1 | DAG scheduling & monitoring |
| Data Warehouse | DuckDB | 0.10.3 | Local OLAP database |
| Transformation | dbt-duckdb | 1.8.1 | SQL models & tests |
| Streaming | Apache Kafka | 7.6.1 | Real-time event streaming |
| Stream Coord. | Apache Zookeeper | 7.6.1 | Kafka coordination |
| Dashboard | Metabase | v0.49.6 | Business intelligence |
| Metadata Store | PostgreSQL | 15 | Airflow metadata DB |
| Processing | Python | 3.11 | Ingestion engine |
| Containerization | Docker Compose | v2 | Full local deployment |

---

## 📁 Project Structure

```
urban-intelligence-lakehouse/
├── docker-compose.yml               ← Full platform definition
├── .env                             ← Environment variables
│
├── metadata/                        ← 🎯 METADATA-DRIVEN CONFIG
│   └── sources/
│       ├── urban_traffic.yaml       ← Kaggle traffic dataset
│       ├── air_quality.yaml         ← Kaggle air quality dataset
│       ├── open_weather_api.yaml    ← REST API weather
│       ├── public_transit_api.yaml  ← REST API transit
│       ├── urban_events_kafka.yaml  ← Kafka streaming
│       └── city_demographics.yaml  ← Local CSV files
│
├── ingestion_engine/                ← 🔧 CORE INGESTION ENGINE
│   ├── engine.py                    ← Main dispatcher (reads metadata)
│   ├── sources/
│   │   ├── base_source.py           ← Abstract base class
│   │   ├── kaggle_source.py         ← Kaggle handler
│   │   ├── api_source.py            ← REST API handler
│   │   ├── kafka_source.py          ← Kafka consumer handler
│   │   └── file_source.py           ← File handler (CSV/JSON/Parquet)
│   └── utils/
│       ├── db_manager.py            ← DuckDB connection manager
│       ├── schema_mapper.py         ← YAML schema → DataFrame mapper
│       └── logger.py                ← Centralized logging
│
├── airflow/
│   └── dags/
│       ├── dag_01_batch_ingestion.py      ← Kaggle + File + API
│       ├── dag_02_kafka_ingestion.py      ← Kafka micro-batch
│       ├── dag_03_bronze_to_silver.py     ← Clean & standardize
│       ├── dag_04_silver_to_gold.py       ← Aggregate & model
│       └── dag_05_data_quality.py         ← Quality checks
│
├── kafka/
│   ├── producer/
│   │   ├── producer.py              ← Urban IoT event simulator
│   │   └── Dockerfile
│   └── consumer/
│       ├── consumer.py              ← Streaming → bronze writer
│       └── Dockerfile
│
├── dbt_project/                     ← dbt transformations
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   ├── stg_traffic.sql
│       │   ├── stg_air_quality.sql
│       │   └── stg_stream_events.sql
│       └── marts/
│           ├── schema.yml
│           ├── fact_traffic_hourly.sql
│           ├── fact_air_quality_daily.sql
│           └── dim_date.sql
│
├── data/                            ← Local data storage
│   ├── bronze/                      ← Raw Parquet dumps
│   ├── silver/                      ← Cleaned data
│   ├── gold/                        ← Analytics-ready
│   ├── raw/                         ← Source files
│   └── duckdb/                      ← DuckDB database file
│
├── quality/                         ← Quality check outputs
└── scripts/
    ├── generate_sample_data.py      ← Pre-populate sample data
    └── wait-for-it.sh
```

---

## 🚀 Quick Start (One Command)

### Prerequisites

- **Docker Desktop** (with WSL2 on Windows)
- **Docker Compose** v2+
- **8 GB RAM** minimum (16 GB recommended)
- **20 GB free disk space**

### Step 1: Clone / Navigate to the project

```bash
cd urban-intelligence-lakehouse
```

### Step 2: Generate sample datasets (optional but recommended)

```bash
# Install minimal dependencies
pip install numpy faker pandas

# Generate ~70,000 rows of sample data
python scripts/generate_sample_data.py
```

### Step 3: Configure environment

```bash
# Edit .env if you have API keys (optional — system works without them)
# The system uses synthetic data fallback for all sources
notepad .env   # Windows
```

### Step 4: Start everything

```bash
docker-compose up -d
```

Wait 2-3 minutes for all services to initialize, then:

```bash
docker-compose ps   # Check all services are healthy
```

### Step 5: Access the services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Metabase** | http://localhost:3000 | Setup on first visit |
| **Kafka UI** | http://localhost:8090 | No auth |

---

## ⚡ Running the Pipeline

### Option A: Run All DAGs from Airflow UI

1. Open http://localhost:8080
2. Go to **DAGs** page
3. Enable and trigger DAGs in order:
   - `01_metadata_driven_batch_ingestion`
   - `02_kafka_stream_ingestion`
   - `03_bronze_to_silver_transformation`
   - `04_silver_to_gold_transformation`
   - `05_data_quality_validation`

### Option B: Trigger via CLI

```bash
# Trigger batch ingestion
docker exec lakehouse_airflow_scheduler airflow dags trigger 01_metadata_driven_batch_ingestion

# Trigger transformation
docker exec lakehouse_airflow_scheduler airflow dags trigger 03_bronze_to_silver_transformation
docker exec lakehouse_airflow_scheduler airflow dags trigger 04_silver_to_gold_transformation
```

### Option C: Run dbt Directly

```bash
docker exec lakehouse_airflow_webserver bash -c "
  cd /opt/airflow/dbt_project &&
  pip install -q dbt-duckdb &&
  dbt run --profiles-dir . --project-dir .
"
```

---

## 🔌 Adding a New Data Source (Zero Code Changes)

The system is fully metadata-driven. To add a new source:

**1. Create `metadata/sources/my_new_source.yaml`:**

```yaml
source_id: my_new_source
source_name: "My New Data Source"
source_type: api          # kaggle | api | kafka | file
format: json
enabled: true

api:
  base_url: "https://api.example.com"
  endpoint: "/data"
  method: GET
  timeout_seconds: 30
  pagination: false

target:
  layer: bronze
  table: bronze_my_new_source
  database_path: "/opt/airflow/data/duckdb/urban_intelligence.duckdb"

schedule:
  type: cron
  expression: "0 */6 * * *"
  retries: 3

schema_mapping:
  id: {target: record_id, type: varchar, required: true}
  value: {target: metric_value, type: double, required: false}
  timestamp: {target: event_timestamp, type: timestamp, required: true}

quality_checks:
  - check_type: not_null
    columns: [record_id, event_timestamp]
```

**2. That's it!** The next DAG run will automatically discover and ingest from your new source.

---

## 📊 Metabase Dashboard Setup

After Metabase starts at http://localhost:3000:

1. Complete the initial setup wizard
2. Go to **Admin → Databases → Add Database**
3. Select **DuckDB** (or SQLite as proxy)
   - Database file: `/duckdb-data/urban_intelligence.duckdb`
4. Create dashboards using the Gold layer tables:

### Recommended Dashboard Cards

| Chart | Table | Description |
|-------|-------|-------------|
| Traffic Trend Line | `gold.fact_traffic_hourly` | Avg traffic volume over time |
| AQI by City | `gold.fact_air_quality_daily` | Heatmap of air quality |
| Stream Events | `gold.fact_stream_events_hourly` | Real-time IoT event volume |
| Urban Intelligence | `gold.urban_intelligence_daily` | Cross-source daily summary |
| Quality Pass Rate | `bronze.quality_audit` | Data quality scorecard |

---

## 📈 Data Quality Framework

Quality checks run on every ingestion cycle:

| Check Type | Description |
|------------|-------------|
| `not_null` | Primary keys and required fields must not be null |
| `duplicate` | No duplicate records on unique keys |
| `value_range` | Numeric values within expected bounds |
| `accepted_values` | Categorical columns have valid values |
| `row_count` | Tables meet minimum row count thresholds |
| `freshness` | Data is within maximum age threshold |

Results are stored in `bronze.quality_audit` for trending.

---

## 🔧 Useful Commands

```bash
# View all logs
docker-compose logs -f

# View Kafka topics
docker exec lakehouse_kafka kafka-topics --bootstrap-server localhost:9092 --list

# Query DuckDB directly
docker exec lakehouse_airflow_webserver python3 -c "
import duckdb
conn = duckdb.connect('/opt/airflow/data/duckdb/urban_intelligence.duckdb')
print(conn.execute('SHOW TABLES').df())
"

# Run data quality checks manually
docker exec lakehouse_airflow_scheduler airflow dags trigger 05_data_quality_validation

# Stop everything
docker-compose down

# Full reset (removes all data!)
docker-compose down -v
```

---

## 🛡️ Production Considerations

This system includes several production-quality patterns:

- **Retry logic** with exponential backoff (tenacity library)
- **Audit trail** — all ingestion runs logged to `bronze.ingestion_audit`
- **Graceful shutdown** — Kafka producer/consumer handle SIGTERM
- **Singleton DuckDB manager** — thread-safe connection management
- **Synthetic data fallback** — system works without any external API keys
- **Schema evolution** — dbt `on_schema_change: sync_all_columns`
- **Incremental models** — dbt only processes new data
- **Tagged DAGs** — organized by layer for easy filtering

---

## 🐛 Troubleshooting

| Issue | Solution |
|-------|----------|
| Airflow not starting | Run `docker-compose logs airflow-init` |
| Kafka connection refused | Wait 60s after startup; check `docker-compose logs kafka` |
| DuckDB locked | Ensure only one writer at a time; restart consumer |
| Metabase blank screen | Wait 2-3 min; check `docker-compose logs metabase` |
| DAG import errors | Check `docker-compose logs airflow-scheduler` |

---

## 📄 License

MIT — Free to use, modify, and distribute.

---

*Built with ❤️ as a demonstration of modern data engineering patterns.*
