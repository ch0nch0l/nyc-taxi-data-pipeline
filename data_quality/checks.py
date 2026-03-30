"""
DATA QUALITY CHECKS
────────────────────────────────────────────────────────────────────
Purpose : Automated data quality assertions run after each pipeline
          stage. Failures raise exceptions that Airflow / CI can catch.

Philosophy:
  - Bronze checks: basic schema and volume (did we get data?)
  - Silver checks: business rule compliance (is data valid?)
  - Gold checks:   aggregation sanity (do numbers make sense?)

All checks produce a structured report — useful for dashboards
and audit trails. This is exactly the kind of DQ approach that
Finnish interviewers (TietoEVRY, Solita, Konecranes) look for.

Usage:
  python -m data_quality.checks --layer bronze
  python -m data_quality.checks --layer silver
  python -m data_quality.checks --layer gold
  python -m data_quality.checks --layer all
"""
import os
import sys
import argparse
from datetime import datetime
from dataclasses import dataclass, field
from typing import List

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from src.config import (
    BRONZE_PATH, SILVER_PATH, GOLD_PATH,
    MAX_NULL_RATE, MAX_NEGATIVE_FARES, MIN_ROW_COUNT
)
from src.spark_session import get_spark


# ── Result dataclass ──────────────────────────────────────────────────────────

@dataclass
class CheckResult:
    check_name:   str
    layer:        str
    status:       str          # "PASS" or "FAIL"
    metric:       float
    threshold:    float
    message:      str
    checked_at:   str = field(default_factory=lambda: datetime.utcnow().isoformat())


def _check(name, layer, metric, threshold, comparison, message) -> CheckResult:
    """Run a single check and return a CheckResult."""
    if comparison == "lt":
        passed = metric < threshold
    elif comparison == "lte":
        passed = metric <= threshold
    elif comparison == "gt":
        passed = metric > threshold
    elif comparison == "gte":
        passed = metric >= threshold
    elif comparison == "eq":
        passed = metric == threshold
    else:
        raise ValueError(f"Unknown comparison: {comparison}")

    status = "PASS" if passed else "FAIL"
    icon   = "✓" if passed else "✗"
    print(f"  {icon} [{layer}] {name}: {metric} (threshold: {comparison} {threshold}) → {status}")
    return CheckResult(name, layer, status, metric, threshold, message)


# ── Bronze checks ─────────────────────────────────────────────────────────────

def check_bronze(spark) -> List[CheckResult]:
    """
    Bronze data quality checks.
    These are volume and schema checks — Bronze doesn't need to be clean,
    just complete and structurally sound.
    """
    print("\n  Bronze Checks:")
    df = spark.read.format("delta").load(BRONZE_PATH)
    total = df.count()
    results = []

    # 1. Minimum row count — did we actually ingest data?
    results.append(_check(
        "minimum_row_count", "bronze",
        metric=total, threshold=MIN_ROW_COUNT, comparison="gte",
        message=f"Bronze must have at least {MIN_ROW_COUNT:,} rows"
    ))

    # 2. fare_amount column exists and is non-null for >99% of rows
    fare_nulls = df.filter(F.col("fare_amount").isNull()).count()
    null_rate = fare_nulls / total if total > 0 else 1.0
    results.append(_check(
        "fare_amount_null_rate", "bronze",
        metric=round(null_rate, 4), threshold=MAX_NULL_RATE, comparison="lt",
        message=f"fare_amount null rate must be < {MAX_NULL_RATE*100}%"
    ))

    # 3. tpep_pickup_datetime is populated
    dt_nulls = df.filter(F.col("tpep_pickup_datetime").isNull()).count()
    dt_null_rate = dt_nulls / total if total > 0 else 1.0
    results.append(_check(
        "pickup_datetime_null_rate", "bronze",
        metric=round(dt_null_rate, 4), threshold=MAX_NULL_RATE, comparison="lt",
        message="pickup datetime null rate must be < 1%"
    ))

    # 4. Audit columns are present (added by ingestion script)
    has_audit = "ingested_at" in df.columns and "source_file" in df.columns
    results.append(_check(
        "audit_columns_present", "bronze",
        metric=int(has_audit), threshold=1, comparison="eq",
        message="Bronze must have ingested_at and source_file audit columns"
    ))

    return results


# ── Silver checks ─────────────────────────────────────────────────────────────

def check_silver(spark) -> List[CheckResult]:
    """
    Silver data quality checks.
    Silver is the trusted canonical layer — stricter rules apply.
    """
    print("\n  Silver Checks:")
    df = spark.read.format("delta").load(SILVER_PATH)
    total = df.count()
    results = []

    # 1. No negative fares allowed in Silver
    negative_fares = df.filter(F.col("fare_amount") <= 0).count()
    results.append(_check(
        "no_negative_fares", "silver",
        metric=negative_fares, threshold=MAX_NEGATIVE_FARES, comparison="eq",
        message="Silver must have zero negative fares"
    ))

    # 2. No null pickup datetimes
    null_pickups = df.filter(F.col("tpep_pickup_datetime").isNull()).count()
    results.append(_check(
        "no_null_pickup_datetimes", "silver",
        metric=null_pickups, threshold=0, comparison="eq",
        message="Silver must have no null pickup datetimes"
    ))

    # 3. Trip duration is always positive
    bad_duration = df.filter(
        F.col("trip_duration_minutes").isNull() |
        (F.col("trip_duration_minutes") <= 0)
    ).count()
    bad_duration_rate = bad_duration / total if total > 0 else 1.0
    results.append(_check(
        "trip_duration_positive_rate", "silver",
        metric=round(bad_duration_rate, 4), threshold=0.001, comparison="lt",
        message="< 0.1% of trips can have non-positive duration"
    ))

    # 4. Derived column fare_per_mile is present
    has_fpm = "fare_per_mile" in df.columns
    results.append(_check(
        "fare_per_mile_column_exists", "silver",
        metric=int(has_fpm), threshold=1, comparison="eq",
        message="Silver must have fare_per_mile derived column"
    ))

    # 5. No duplicates on business key
    dedup_count = df.dropDuplicates(["vendor_id", "tpep_pickup_datetime"]).count()
    dupe_rate = (total - dedup_count) / total if total > 0 else 0
    results.append(_check(
        "duplicate_rate", "silver",
        metric=round(dupe_rate, 6), threshold=0.0, comparison="eq",
        message="Silver must have zero duplicate trips"
    ))

    # 6. Passenger count is within valid range
    invalid_pax = df.filter(~F.col("passenger_count").between(1, 8)).count()
    results.append(_check(
        "passenger_count_in_range", "silver",
        metric=invalid_pax, threshold=0, comparison="eq",
        message="All trips must have 1–8 passengers"
    ))

    return results


# ── Gold checks ───────────────────────────────────────────────────────────────

def check_gold(spark) -> List[CheckResult]:
    """
    Gold data quality checks.
    These are sanity checks — do the aggregations make business sense?
    """
    print("\n  Gold Checks:")
    results = []

    # Check daily_revenue table
    daily_path = os.path.join(GOLD_PATH, "daily_revenue")
    if os.path.exists(daily_path):
        df_daily = spark.read.format("delta").load(daily_path)
        daily_count = df_daily.count()

        # 1. At least some daily rows exist
        results.append(_check(
            "daily_revenue_has_rows", "gold",
            metric=daily_count, threshold=1, comparison="gte",
            message="daily_revenue Gold table must have at least 1 row"
        ))

        # 2. Total revenue is positive
        total_rev = df_daily.agg(F.sum("total_revenue")).collect()[0][0] or 0
        results.append(_check(
            "total_revenue_positive", "gold",
            metric=float(total_rev), threshold=0, comparison="gt",
            message="Total revenue across all days must be positive"
        ))

        # 3. Average fare is in reasonable range ($2–$200)
        avg_fare = df_daily.agg(F.avg("avg_fare")).collect()[0][0] or 0
        results.append(_check(
            "avg_fare_reasonable_low", "gold",
            metric=round(float(avg_fare), 2), threshold=2.0, comparison="gt",
            message="Average fare must be > $2 (not suspiciously low)"
        ))
        results.append(_check(
            "avg_fare_reasonable_high", "gold",
            metric=round(float(avg_fare), 2), threshold=200.0, comparison="lt",
            message="Average fare must be < $200 (not suspiciously high)"
        ))

    # Check payment_analysis table
    payment_path = os.path.join(GOLD_PATH, "payment_analysis")
    if os.path.exists(payment_path):
        df_pay = spark.read.format("delta").load(payment_path)
        payment_types = df_pay.count()
        results.append(_check(
            "payment_analysis_has_types", "gold",
            metric=payment_types, threshold=1, comparison="gte",
            message="payment_analysis must have at least 1 payment type"
        ))

    return results


# ── Report ────────────────────────────────────────────────────────────────────

def print_report(results: List[CheckResult]) -> bool:
    """Print a summary report. Returns True if all checks passed."""
    passed = [r for r in results if r.status == "PASS"]
    failed = [r for r in results if r.status == "FAIL"]

    print(f"\n{'='*60}")
    print(f"  DATA QUALITY REPORT — {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")
    print(f"  Total checks : {len(results)}")
    print(f"  Passed       : {len(passed)}")
    print(f"  Failed       : {len(failed)}")

    if failed:
        print(f"\n  FAILURES:")
        for r in failed:
            print(f"  ✗ [{r.layer}] {r.check_name}")
            print(f"      {r.message}")
            print(f"      Got {r.metric}, expected {r.threshold}")

    all_passed = len(failed) == 0
    status = "ALL CHECKS PASSED ✓" if all_passed else f"{len(failed)} CHECK(S) FAILED ✗"
    print(f"\n  {status}")
    return all_passed


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Run data quality checks")
    parser.add_argument(
        "--layer", choices=["bronze", "silver", "gold", "all"], default="all"
    )
    args = parser.parse_args()

    spark = get_spark()
    results = []

    if args.layer in ("bronze", "all"):
        results += check_bronze(spark)
    if args.layer in ("silver", "all"):
        results += check_silver(spark)
    if args.layer in ("gold", "all"):
        results += check_gold(spark)

    all_passed = print_report(results)

    # Exit with non-zero code on failure so CI/CD and Airflow can detect it
    sys.exit(0 if all_passed else 1)


if __name__ == "__main__":
    main()
