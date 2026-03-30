"""
BRONZE LAYER — Raw Ingestion
────────────────────────────────────────────────────────────────────
Purpose : Download NYC TLC Yellow Taxi Parquet files and write them
          to the Bronze Delta table with zero transformation.
          Bronze = raw data preserved exactly as received.

Key decisions:
  - Append mode: each month's file is a separate partition
  - No filtering, no type casting — Bronze is a faithful raw copy
  - Audit columns (ingested_at, source_file) added for traceability
  - Schema validation: fail fast if expected columns are missing

Usage:
  python -m src.bronze.ingest_taxi --year 2024 --month 1
  python -m src.bronze.ingest_taxi --year 2024 --month 1 --month 2 --month 3
"""
import argparse
import os
import requests
import tempfile
from datetime import datetime

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.config import (
    TLC_BASE_URL, BRONZE_PATH, REQUIRED_COLUMNS, DELTA_APPEND
)
from src.spark_session import get_spark, stop_spark


# ── Download ──────────────────────────────────────────────────────────────────

def build_url(year: int, month: int) -> str:
    """Build the TLC download URL for a given year/month."""
    return f"{TLC_BASE_URL}/yellow_tripdata_{year}-{month:02d}.parquet"


def download_parquet(url: str, dest_dir: str) -> str:
    """
    Download a Parquet file from the TLC CDN.
    Returns the local file path.
    Streams the download to handle large files without memory issues.
    """
    filename = url.split("/")[-1]
    dest_path = os.path.join(dest_dir, filename)

    if os.path.exists(dest_path):
        print(f"  [cache] {filename} already downloaded, skipping")
        return dest_path

    print(f"  [download] {url}")
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    total = int(response.headers.get("content-length", 0))
    downloaded = 0

    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total:
                pct = downloaded / total * 100
                print(f"\r  {pct:.1f}%", end="", flush=True)

    print(f"\r  [done] {filename} ({downloaded / 1_048_576:.1f} MB)")
    return dest_path


# ── Validation ────────────────────────────────────────────────────────────────

def validate_schema(df: DataFrame, source: str) -> None:
    """
    Fail fast if the downloaded file is missing expected columns.
    TLC occasionally changes their schema — catching this early
    prevents silent downstream data quality failures.
    """
    actual_cols = set(df.columns)
    missing = [c for c in REQUIRED_COLUMNS if c not in actual_cols]
    if missing:
        raise ValueError(
            f"Schema mismatch in {source}.\n"
            f"Missing columns: {missing}\n"
            f"Actual columns: {sorted(actual_cols)}"
        )
    print(f"  [schema] All {len(REQUIRED_COLUMNS)} expected columns present")


# ── Write to Bronze ───────────────────────────────────────────────────────────

def write_to_bronze(df: DataFrame, source_url: str, year: int, month: int) -> int:
    """
    Add audit columns and write to Bronze Delta table.
    Partitioned by year/month for efficient downstream reads.
    Returns row count written.
    """
    df_bronze = (
        df
        .withColumn("ingested_at",   F.current_timestamp())
        .withColumn("source_file",   F.lit(source_url))
        .withColumn("pipeline_year", F.lit(year))
        .withColumn("pipeline_month", F.lit(month))
    )

    row_count = df_bronze.count()
    print(f"  [bronze] Writing {row_count:,} rows → {BRONZE_PATH}")

    (
        df_bronze.write
        .format("delta")
        .mode(DELTA_APPEND)
        .partitionBy("pipeline_year", "pipeline_month")
        .save(BRONZE_PATH)
    )

    print(f"  [bronze] Write complete — partition year={year}, month={month:02d}")
    return row_count


# ── Main ──────────────────────────────────────────────────────────────────────

def ingest_month(year: int, month: int) -> dict:
    """
    Full ingestion for one month: download → validate → write to Bronze.
    Returns a summary dict for reporting.
    """
    spark = get_spark()
    url   = build_url(year, month)
    print(f"\n{'='*60}")
    print(f"  Ingesting: {year}-{month:02d}")
    print(f"  Source   : {url}")
    print(f"{'='*60}")

    # Download to a local temp directory
    os.makedirs("./data/raw", exist_ok=True)
    local_path = download_parquet(url, "./data/raw")

    # Read Parquet
    df = spark.read.parquet(local_path)
    print(f"  [read] {df.count():,} rows, {len(df.columns)} columns")

    # Validate schema
    validate_schema(df, url)

    # Write to Bronze
    row_count = write_to_bronze(df, url, year, month)

    return {
        "year": year,
        "month": month,
        "source_url": url,
        "rows_ingested": row_count,
        "ingested_at": datetime.utcnow().isoformat(),
        "status": "success",
    }


def main():
    parser = argparse.ArgumentParser(
        description="Ingest NYC TLC Yellow Taxi data to Bronze Delta table"
    )
    parser.add_argument(
        "--year", type=int, default=2024,
        help="Year to ingest (default: 2024)"
    )
    parser.add_argument(
        "--month", type=int, nargs="+", default=[1],
        help="Month(s) to ingest, space-separated (default: 1)"
    )
    args = parser.parse_args()

    results = []
    for month in args.month:
        result = ingest_month(args.year, month)
        results.append(result)

    print(f"\n{'='*60}")
    print("  INGESTION SUMMARY")
    print(f"{'='*60}")
    total_rows = 0
    for r in results:
        status_icon = "✓" if r["status"] == "success" else "✗"
        print(f"  {status_icon} {r['year']}-{r['month']:02d}  →  {r['rows_ingested']:>10,} rows")
        total_rows += r["rows_ingested"]
    print(f"  {'─'*40}")
    print(f"  Total: {total_rows:,} rows across {len(results)} month(s)")
    print(f"  Bronze path: {BRONZE_PATH}")


if __name__ == "__main__":
    main()
