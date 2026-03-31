# NYC Taxi Data Pipeline 🚖

**End-to-end batch data pipeline implementing the Medallion Architecture**  
Raw NYC TLC taxi data → Bronze → Silver → Gold → BI-ready Delta tables

![CI](https://github.com/[yourusername]/nyc-taxi-data-pipeline/actions/workflows/ci.yml/badge.svg)
![Python](https://img.shields.io/badge/Python-3.11-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.1-blue)
![License](https://img.shields.io/badge/License-MIT-green)

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    NYC TLC Open Data                                 │
│          https://d37ci6vzurychx.cloudfront.net/trip-data            │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Monthly Parquet files (~3M rows/month)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  BRONZE LAYER  (raw/append-only)                                     │
│  • Zero transformation — faithful copy of source                    │
│  • Audit columns: ingested_at, source_file                          │
│  • Partitioned by pipeline_year / pipeline_month                    │
│  • Format: Delta Lake                                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark (clean_taxi.py)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  SILVER LAYER  (cleaned/canonical)                                  │
│  • Filter: negative fares, bad datetimes, invalid passengers        │
│  • Deduplicate on business key (vendor_id + pickup_datetime)        │
│  • Cast types, rename columns to snake_case                         │
│  • Derived: fare_per_mile, trip_duration_minutes, pickup_hour       │
│  • Outlier flag: is_outlier (kept for audit, excluded from Gold)    │
│  • Format: Delta Lake, partitioned by pickup_year / pickup_month    │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  PySpark (aggregate.py)
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  GOLD LAYER  (analytics-ready)                                      │
│  • daily_revenue     — revenue & trips by day                       │
│  • hourly_patterns   — demand by hour of day + day of week          │
│  • payment_analysis  — breakdown by payment method                  │
│  • top_routes        — most popular pickup → dropoff pairs          │
│  • distance_bands    — trip distribution by distance                │
│  • Format: Delta Lake                                               │
└───────────────────────────┬─────────────────────────────────────────┘
                            │
                            ▼
                   Power BI / Looker Studio
```

---

## Tech Stack

| Component       | Technology                          |
|-----------------|-------------------------------------|
| Processing      | Apache Spark 3.5 (PySpark)          |
| Storage format  | Delta Lake 3.1                      |
| Orchestration   | Apache Airflow *(Project 3)*        |
| Transformation  | dbt *(Project 2)*                   |
| Containerisation| Docker + Docker Compose             |
| CI/CD           | GitHub Actions                      |
| BI              | Power BI / Google Looker Studio     |
| Language        | Python 3.11                         |

---

## Dataset

NYC TLC publishes monthly Parquet files of all yellow taxi trips — data is updated with a two-month lag and goes back to 2009.

- **Source**: [NYC Taxi & Limousine Commission](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)
- **Volume**: ~3 million rows per month (January 2024)
- **Schema**: 19 columns including pickup/dropoff location IDs, fare, tip, distance, duration
- **License**: Public domain

---

## Quick Start

### Option A — Local with Docker (recommended)

```bash
# 1. Clone the repo
git clone https://github.com/[yourusername]/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

# 2. Copy environment config
cp .env.example .env

# 3. Start Spark + Jupyter
docker compose up -d

# 4. Open Jupyter Lab
# → http://localhost:8888
# → Open notebooks/01_bronze_ingestion.ipynb

# 5. Or run the full pipeline from CLI
docker compose exec jupyter python run_pipeline.py --year 2024 --month 1
```

### Option B — Local Python (no Docker)

```bash
# Requires: Python 3.11+, Java 11+
git clone https://github.com/[yourusername]/nyc-taxi-data-pipeline.git
cd nyc-taxi-data-pipeline

python -m venv .venv
source .venv/bin/activate          # Windows: .venv\Scripts\activate
pip install -r requirements.txt

cp .env.example .env

# Run individual stages
python -m src.bronze.ingest_taxi --year 2024 --month 1
python -m src.silver.clean_taxi  --year 2024 --month 1
python -m src.gold.aggregate     --year 2024 --month 1

# Or run everything at once
python run_pipeline.py --year 2024 --month 1
```

### Option C — Databricks (cloud)

```python
# In a Databricks notebook:
# 1. Clone repo: git clone ... (via Repos)
# 2. Install requirements: %pip install -r requirements.txt
# 3. Set environment variables in cluster config
# 4. Run:
%run ./run_pipeline --year 2024 --month 1
```

---

## Project Structure

```
nyc-taxi-data-pipeline/
│
├── src/
│   ├── config.py              ← all constants and config
│   ├── spark_session.py       ← reusable SparkSession factory
│   ├── bronze/
│   │   └── ingest_taxi.py     ← download + write Bronze Delta
│   ├── silver/
│   │   └── clean_taxi.py      ← clean + validate → Silver Delta
│   └── gold/
│       └── aggregate.py       ← build 5 Gold aggregation tables
│
├── data_quality/
│   └── checks.py              ← automated DQ assertions per layer
│
├── tests/
│   └── test_pipeline.py       ← pytest unit tests (no real data needed)
│
├── notebooks/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_cleaning.ipynb
│   └── 03_gold_exploration.ipynb
│
├── docs/
│   └── architecture.png
│
├── .github/workflows/ci.yml   ← GitHub Actions CI
├── docker-compose.yml         ← Spark + Jupyter local env
├── run_pipeline.py            ← single-command full pipeline runner
├── requirements.txt
├── .env.example
└── .gitignore
```

---

## Data Quality

Automated checks run after each layer. The pipeline fails fast if any check fails — preventing bad data from propagating downstream.

| Layer  | Checks |
|--------|--------|
| Bronze | Min row count, null rate on critical columns, audit column presence |
| Silver | Zero negative fares, no null pickup datetimes, zero duplicates, passenger count range |
| Gold   | Revenue positivity, avg fare sanity range ($2–$200), table row counts |

Run checks independently:
```bash
python -m data_quality.checks --layer bronze
python -m data_quality.checks --layer silver
python -m data_quality.checks --layer gold
python -m data_quality.checks --layer all
```

---

## Running Tests

```bash
pytest tests/ -v

# With coverage
pytest tests/ -v --cov=src --cov-report=term-missing
```

Tests use small in-memory DataFrames — no data download needed. CI runs on every push via GitHub Actions.

---

## Sample Gold Output

**daily_revenue** (top 5 rows):

| pickup_date | total_trips | total_revenue | avg_fare | avg_distance_miles |
|---|---|---|---|---|
| 2024-01-01 | 72,419 | $1,243,881 | $17.18 | 3.21 |
| 2024-01-02 | 98,742 | $1,694,319 | $17.16 | 3.19 |
| 2024-01-03 | 101,554 | $1,741,646 | $17.15 | 3.17 |

**payment_analysis**:

| payment_type_label | trip_count | share_of_trips_pct | avg_tip |
|---|---|---|---|
| Credit card | 2,108,442 | 71.2% | $3.42 |
| Cash | 812,301 | 27.4% | $0.00 |

---

## What I'd Add With More Time

- **Apache Airflow orchestration** — DAG with retries, SLA alerts, and backfill capability *(built in Project 3)*
- **dbt transformation layer** — replace Gold PySpark with versioned, tested dbt models *(built in Project 2)*
- **Great Expectations** — richer data quality profiles with HTML reports
- **Streaming ingestion** — Kafka + Spark Structured Streaming for real-time taxi data
- **CI/CD to Databricks** — auto-deploy notebooks on merge to main via Databricks REST API
- **Dashboard** — Power BI report connected to Gold Delta tables

---

## Author

**Mehedi Hasan Chonchol** · Data Engineer  
[LinkedIn](https://linkedin.com/in/ch0nch0l) · [GitHub](https://github.com/ch0nch0l)
