"""
SILVER LAYER — Cleaning & Validation
────────────────────────────────────────────────────────────────────
Purpose : Read from Bronze, apply business rules, clean data,
          and write a trusted Silver Delta table.
          Silver = cleaned, validated, enriched — the canonical layer.

Transformations applied:
  1. Filter invalid rows (negative fares, zero distance, bad dates)
  2. Deduplicate on business key (VendorID + pickup_datetime)
  3. Cast columns to correct types
  4. Rename columns to snake_case
  5. Add derived columns (fare_per_mile, trip_duration_minutes)
  6. Map payment_type integer → human-readable label
  7. Add data quality flag column for borderline rows

Usage:
  python -m src.silver.clean_taxi
  python -m src.silver.clean_taxi --year 2024 --month 1
"""
import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import IntegerType, DoubleType, TimestampType, StringType

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.config import (
    BRONZE_PATH, SILVER_PATH,
    PAYMENT_TYPE_MAP, DELTA_OVERWRITE
)
from src.spark_session import get_spark


# ── Step 1: Read from Bronze ──────────────────────────────────────────────────

def read_bronze(spark, year: int = None, month: int = None) -> DataFrame:
    """
    Read Bronze Delta table.
    If year/month provided, read only that partition (efficient).
    Otherwise read all partitions.
    """
    df = spark.read.format("delta").load(BRONZE_PATH)

    if year and month:
        df = df.filter(
            (F.col("pipeline_year") == year) &
            (F.col("pipeline_month") == month)
        )
        print(f"  [bronze] Filtered to year={year}, month={month:02d}")

    count = df.count()
    print(f"  [bronze] Read {count:,} rows from Bronze")
    return df, count


# ── Step 2: Rename & cast columns ─────────────────────────────────────────────

def rename_and_cast(df: DataFrame) -> DataFrame:
    """
    Standardise column names to snake_case and cast to correct types.
    Raw TLC data uses mixed CamelCase/snake_case naming.
    """
    return (
        df
        # Rename to snake_case
        .withColumnRenamed("VendorID",             "vendor_id")
        .withColumnRenamed("RatecodeID",           "rate_code_id")
        .withColumnRenamed("PULocationID",         "pickup_location_id")
        .withColumnRenamed("DOLocationID",         "dropoff_location_id")
        # Cast to correct types
        .withColumn("vendor_id",          F.col("vendor_id").cast(IntegerType()))
        .withColumn("passenger_count",    F.col("passenger_count").cast(IntegerType()))
        .withColumn("trip_distance",      F.col("trip_distance").cast(DoubleType()))
        .withColumn("rate_code_id",       F.col("rate_code_id").cast(IntegerType()))
        .withColumn("payment_type",       F.col("payment_type").cast(IntegerType()))
        .withColumn("fare_amount",        F.col("fare_amount").cast(DoubleType()))
        .withColumn("extra",              F.col("extra").cast(DoubleType()))
        .withColumn("mta_tax",            F.col("mta_tax").cast(DoubleType()))
        .withColumn("tip_amount",         F.col("tip_amount").cast(DoubleType()))
        .withColumn("tolls_amount",       F.col("tolls_amount").cast(DoubleType()))
        .withColumn("improvement_surcharge", F.col("improvement_surcharge").cast(DoubleType()))
        .withColumn("total_amount",       F.col("total_amount").cast(DoubleType()))
        .withColumn("congestion_surcharge", F.col("congestion_surcharge").cast(DoubleType()))
        .withColumn("tpep_pickup_datetime",  F.col("tpep_pickup_datetime").cast(TimestampType()))
        .withColumn("tpep_dropoff_datetime", F.col("tpep_dropoff_datetime").cast(TimestampType()))
    )


# ── Step 3: Filter invalid rows ───────────────────────────────────────────────

def filter_invalid_rows(df: DataFrame) -> tuple[DataFrame, dict]:
    """
    Apply business rules to remove invalid rows.
    Returns cleaned DataFrame and a dict of removal counts for auditing.
    """
    original_count = df.count()

    # Rule 1: fare_amount must be positive
    df_r1 = df.filter(F.col("fare_amount") > 0)
    after_r1 = df_r1.count()

    # Rule 2: trip_distance must be non-negative
    df_r2 = df_r1.filter(F.col("trip_distance") >= 0)
    after_r2 = df_r2.count()

    # Rule 3: pickup datetime must be valid (not null, not future)
    df_r3 = df_r2.filter(
        F.col("tpep_pickup_datetime").isNotNull() &
        (F.col("tpep_pickup_datetime") <= F.current_timestamp())
    )
    after_r3 = df_r3.count()

    # Rule 4: dropoff must be after pickup
    df_r4 = df_r3.filter(
        F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime")
    )
    after_r4 = df_r4.count()

    # Rule 5: passenger count must be 1–8
    df_r5 = df_r4.filter(
        F.col("passenger_count").between(1, 8)
    )
    after_r5 = df_r5.count()

    removals = {
        "negative_fares":       original_count - after_r1,
        "negative_distance":    after_r1 - after_r2,
        "invalid_pickup_time":  after_r2 - after_r3,
        "dropoff_before_pickup":after_r3 - after_r4,
        "invalid_passengers":   after_r4 - after_r5,
        "total_removed":        original_count - after_r5,
    }

    print(f"  [filter] Removed {removals['total_removed']:,} invalid rows:")
    for rule, count in removals.items():
        if rule != "total_removed" and count > 0:
            print(f"           └─ {rule}: {count:,}")

    return df_r5, removals


# ── Step 4: Deduplicate ───────────────────────────────────────────────────────

def deduplicate(df: DataFrame) -> tuple[DataFrame, int]:
    """
    Remove duplicate rows based on the business key:
    vendor_id + pickup_datetime uniquely identifies a trip.
    """
    before = df.count()
    df_dedup = df.dropDuplicates(["vendor_id", "tpep_pickup_datetime"])
    after = df_dedup.count()
    dupes = before - after
    print(f"  [dedup] Removed {dupes:,} duplicate rows")
    return df_dedup, dupes


# ── Step 5: Add derived columns ───────────────────────────────────────────────

def add_derived_columns(df: DataFrame) -> DataFrame:
    """
    Enrich the dataset with derived business metrics.
    These columns are commonly used in Gold aggregations.
    """
    # Map payment_type integer to human-readable label
    payment_mapping = F.create_map(
        *[val for pair in [(F.lit(k), F.lit(v)) for k, v in PAYMENT_TYPE_MAP.items()] for val in pair]
    )

    return (
        df
        # Trip duration in minutes
        .withColumn(
            "trip_duration_minutes",
            F.round(
                (F.unix_timestamp("tpep_dropoff_datetime") -
                 F.unix_timestamp("tpep_pickup_datetime")) / 60,
                2
            )
        )
        # Fare per mile (handle zero distance gracefully)
        .withColumn(
            "fare_per_mile",
            F.when(F.col("trip_distance") > 0,
                   F.round(F.col("fare_amount") / F.col("trip_distance"), 2))
             .otherwise(None)
        )
        # Pickup date parts for easy aggregation
        .withColumn("pickup_date",  F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour",  F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow",   F.dayofweek("tpep_pickup_datetime"))   # 1=Sun
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_year",  F.year("tpep_pickup_datetime"))
        # Payment type label
        .withColumn("payment_type_label", payment_mapping[F.col("payment_type")])
        # Flag outlier fares (kept in Silver, excluded in Gold)
        .withColumn(
            "is_outlier",
            F.col("fare_amount") > 500
        )
        # Silver audit column
        .withColumn("silver_processed_at", F.current_timestamp())
    )


# ── Step 6: Write to Silver ───────────────────────────────────────────────────

def write_to_silver(df: DataFrame, year: int = None, month: int = None) -> int:
    """
    Write cleaned data to Silver Delta table.
    Partitioned by pickup_year/pickup_month for efficient Gold reads.
    """
    row_count = df.count()
    print(f"  [silver] Writing {row_count:,} rows → {SILVER_PATH}")

    (
        df.write
        .format("delta")
        .mode(DELTA_OVERWRITE)
        .option("overwriteSchema", "true")
        .partitionBy("pickup_year", "pickup_month")
        .save(SILVER_PATH)
    )

    print(f"  [silver] Write complete")
    return row_count


# ── Main ──────────────────────────────────────────────────────────────────────

def clean_month(year: int = None, month: int = None) -> dict:
    """Run the full Bronze → Silver cleaning pipeline for one or all months."""
    spark = get_spark()

    print(f"\n{'='*60}")
    print(f"  Silver Cleaning: {'all partitions' if not year else f'{year}-{month:02d}'}")
    print(f"{'='*60}")

    # Step 1: Read
    df, bronze_count = read_bronze(spark, year, month)

    # Step 2: Rename & cast
    df = rename_and_cast(df)

    # Step 3: Filter invalid rows
    df, removals = filter_invalid_rows(df)

    # Step 4: Deduplicate
    df, dupes_removed = deduplicate(df)

    # Step 5: Add derived columns
    df = add_derived_columns(df)

    # Step 6: Write to Silver
    silver_count = write_to_silver(df, year, month)

    summary = {
        "bronze_rows":     bronze_count,
        "silver_rows":     silver_count,
        "rows_removed":    bronze_count - silver_count,
        "removal_rate_pct": round((bronze_count - silver_count) / bronze_count * 100, 2),
        "duplicates_removed": dupes_removed,
        **removals
    }

    print(f"\n  Silver Summary:")
    print(f"  Bronze input  : {summary['bronze_rows']:>10,}")
    print(f"  Silver output : {summary['silver_rows']:>10,}")
    print(f"  Rows removed  : {summary['rows_removed']:>10,}  ({summary['removal_rate_pct']}%)")

    return summary


def main():
    parser = argparse.ArgumentParser(description="Clean Bronze → Silver")
    parser.add_argument("--year",  type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    clean_month(args.year, args.month)


if __name__ == "__main__":
    main()
