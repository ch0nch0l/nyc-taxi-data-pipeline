"""
Central configuration for the NYC Taxi Data Pipeline.
All paths, constants, and settings live here.
Import this module in every src/ script.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# ── Data layer paths ──────────────────────────────────────────────────────────
# On Databricks: use DBFS paths like /mnt/nyc-taxi/bronze
# Locally:       use relative paths under ./data/
BRONZE_PATH = os.getenv("BRONZE_PATH", "./data/bronze")
SILVER_PATH = os.getenv("SILVER_PATH", "./data/silver")
GOLD_PATH   = os.getenv("GOLD_PATH",   "./data/gold")

# ── Source data ───────────────────────────────────────────────────────────────
# NYC TLC publishes monthly Parquet files at this URL pattern:
# https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
TLC_BASE_URL = os.getenv(
    "TLC_BASE_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data"
)

# Default months to ingest (used when running scripts directly)
DEFAULT_YEAR  = 2024
DEFAULT_MONTH = 1   # January 2024 — ~2.9M rows, good for development

# ── Data quality thresholds ───────────────────────────────────────────────────
MAX_NULL_RATE      = 0.01    # fail if >1% nulls on critical columns
MAX_NEGATIVE_FARES = 0       # zero tolerance for negative fares
MIN_TRIP_DISTANCE  = 0.0     # trips must have non-negative distance
MAX_FARE_AMOUNT    = 1000.0  # flag suspiciously high fares (outliers)
MIN_ROW_COUNT      = 100_000 # fail if monthly file has fewer rows than this

# ── Spark settings ────────────────────────────────────────────────────────────
SPARK_APP_NAME     = "nyc-taxi-pipeline"
SPARK_MASTER       = os.getenv("SPARK_MASTER", "local[*]")

# Delta Lake write options
DELTA_OVERWRITE    = "overwrite"
DELTA_APPEND       = "append"

# ── Schema: expected columns in raw TLC data ──────────────────────────────────
# These are the Yellow Taxi columns from 2022 onwards (Parquet format)
REQUIRED_COLUMNS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "RatecodeID",
    "store_and_fwd_flag",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "extra",
    "mta_tax",
    "tip_amount",
    "tolls_amount",
    "improvement_surcharge",
    "total_amount",
    "congestion_surcharge",
]

# Payment type mapping (from TLC data dictionary)
PAYMENT_TYPE_MAP = {
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}
