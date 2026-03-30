"""
GOLD LAYER — Business Aggregations
────────────────────────────────────────────────────────────────────
Purpose : Read from Silver and produce business-ready aggregated
          tables optimised for BI tools and dashboards.
          Gold = analytics-ready, one table per business question.

Gold tables produced:
  1. daily_revenue        — revenue and trip counts by day
  2. hourly_patterns      — demand patterns by hour of day
  3. payment_analysis     — payment method breakdown
  4. top_routes           — most popular pickup/dropoff zone pairs
  5. trip_distance_bands  — distance distribution for pricing insight

Usage:
  python -m src.gold.aggregate
  python -m src.gold.aggregate --year 2024 --month 1
"""
import argparse
import os
import sys

from pyspark.sql import functions as F
from pyspark.sql import DataFrame

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from src.config import SILVER_PATH, GOLD_PATH, DELTA_OVERWRITE
from src.spark_session import get_spark


def read_silver(spark, year: int = None, month: int = None) -> DataFrame:
    """Read Silver Delta table, optionally filtered to a specific month."""
    df = spark.read.format("delta").load(SILVER_PATH)

    if year and month:
        df = df.filter(
            (F.col("pickup_year") == year) &
            (F.col("pickup_month") == month)
        )

    # Exclude outliers from Gold analytics
    df = df.filter(F.col("is_outlier") == False)

    count = df.count()
    print(f"  [silver] Read {count:,} rows (outliers excluded)")
    return df


# ── Gold Table 1: Daily Revenue ───────────────────────────────────────────────

def build_daily_revenue(df: DataFrame) -> DataFrame:
    """
    Daily revenue summary — the most commonly requested BI metric.
    Answers: "How much did we make today? How many trips?"
    """
    return (
        df
        .groupBy("pickup_date")
        .agg(
            F.count("*")                          .alias("total_trips"),
            F.round(F.sum("fare_amount"),    2)   .alias("total_fare_revenue"),
            F.round(F.sum("total_amount"),   2)   .alias("total_revenue"),
            F.round(F.sum("tip_amount"),     2)   .alias("total_tips"),
            F.round(F.avg("fare_amount"),    2)   .alias("avg_fare"),
            F.round(F.avg("trip_distance"),  2)   .alias("avg_distance_miles"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_minutes"),
            F.round(F.avg("passenger_count"), 2)  .alias("avg_passengers"),
        )
        .orderBy("pickup_date")
    )


# ── Gold Table 2: Hourly Patterns ─────────────────────────────────────────────

def build_hourly_patterns(df: DataFrame) -> DataFrame:
    """
    Trip demand by hour of day and day of week.
    Answers: "When is demand highest? When to surge price?"
    """
    dow_labels = F.create_map(
        F.lit(1), F.lit("Sunday"),
        F.lit(2), F.lit("Monday"),
        F.lit(3), F.lit("Tuesday"),
        F.lit(4), F.lit("Wednesday"),
        F.lit(5), F.lit("Thursday"),
        F.lit(6), F.lit("Friday"),
        F.lit(7), F.lit("Saturday"),
    )

    return (
        df
        .groupBy("pickup_hour", "pickup_dow")
        .agg(
            F.count("*")                        .alias("total_trips"),
            F.round(F.avg("fare_amount"),   2)  .alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2)  .alias("avg_distance"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration"),
        )
        .withColumn("day_of_week_label", dow_labels[F.col("pickup_dow")])
        .orderBy("pickup_dow", "pickup_hour")
    )


# ── Gold Table 3: Payment Analysis ────────────────────────────────────────────

def build_payment_analysis(df: DataFrame) -> DataFrame:
    """
    Revenue breakdown by payment method.
    Answers: "How much revenue comes from card vs cash?"
    """
    total_trips = df.count()

    return (
        df
        .groupBy("payment_type_label")
        .agg(
            F.count("*")                        .alias("trip_count"),
            F.round(F.sum("total_amount"),  2)  .alias("total_revenue"),
            F.round(F.avg("total_amount"),  2)  .alias("avg_fare"),
            F.round(F.avg("tip_amount"),    2)  .alias("avg_tip"),
            F.round(F.sum("tip_amount") / F.sum("fare_amount") * 100, 2)
                                                .alias("tip_rate_pct"),
        )
        .withColumn(
            "share_of_trips_pct",
            F.round(F.col("trip_count") / total_trips * 100, 2)
        )
        .orderBy(F.col("trip_count").desc())
    )


# ── Gold Table 4: Top Routes ───────────────────────────────────────────────────

def build_top_routes(df: DataFrame, top_n: int = 50) -> DataFrame:
    """
    Most popular pickup→dropoff zone pairs.
    Answers: "What routes should we optimise driver positioning for?"
    """
    return (
        df
        .groupBy("pickup_location_id", "dropoff_location_id")
        .agg(
            F.count("*")                        .alias("trip_count"),
            F.round(F.avg("fare_amount"),   2)  .alias("avg_fare"),
            F.round(F.avg("trip_distance"), 2)  .alias("avg_distance"),
            F.round(F.avg("tip_amount"),    2)  .alias("avg_tip"),
        )
        .orderBy(F.col("trip_count").desc())
        .limit(top_n)
    )


# ── Gold Table 5: Distance Bands ──────────────────────────────────────────────

def build_distance_bands(df: DataFrame) -> DataFrame:
    """
    Trip volume and revenue by distance band.
    Answers: "What proportion of trips are short hops vs long journeys?"
    """
    return (
        df
        .withColumn(
            "distance_band",
            F.when(F.col("trip_distance") < 1,  "< 1 mile")
             .when(F.col("trip_distance") < 3,  "1–3 miles")
             .when(F.col("trip_distance") < 7,  "3–7 miles")
             .when(F.col("trip_distance") < 15, "7–15 miles")
             .otherwise("15+ miles")
        )
        .groupBy("distance_band")
        .agg(
            F.count("*")                        .alias("trip_count"),
            F.round(F.avg("fare_amount"),   2)  .alias("avg_fare"),
            F.round(F.avg("tip_amount"),    2)  .alias("avg_tip"),
            F.round(F.avg("trip_duration_minutes"), 2).alias("avg_duration_min"),
        )
        .orderBy("distance_band")
    )


# ── Write helpers ─────────────────────────────────────────────────────────────

def write_gold_table(df: DataFrame, table_name: str) -> int:
    """Write a Gold Delta table and return row count."""
    path = os.path.join(GOLD_PATH, table_name)
    count = df.count()
    print(f"  [gold/{table_name}] {count:,} rows → {path}")
    (
        df.write
        .format("delta")
        .mode(DELTA_OVERWRITE)
        .option("overwriteSchema", "true")
        .save(path)
    )
    return count


# ── Main ──────────────────────────────────────────────────────────────────────

def build_gold(year: int = None, month: int = None) -> dict:
    """Build all Gold tables from Silver."""
    spark = get_spark()

    print(f"\n{'='*60}")
    print(f"  Gold Aggregation: {'all partitions' if not year else f'{year}-{month:02d}'}")
    print(f"{'='*60}")

    df = read_silver(spark, year, month)

    tables = {
        "daily_revenue":    build_daily_revenue(df),
        "hourly_patterns":  build_hourly_patterns(df),
        "payment_analysis": build_payment_analysis(df),
        "top_routes":       build_top_routes(df),
        "distance_bands":   build_distance_bands(df),
    }

    results = {}
    for name, gold_df in tables.items():
        results[name] = write_gold_table(gold_df, name)

    print(f"\n  Gold Summary:")
    for name, rows in results.items():
        print(f"  ✓ {name:<25} {rows:>6,} rows")
    print(f"  Gold path: {GOLD_PATH}")

    return results


def main():
    parser = argparse.ArgumentParser(description="Build Gold aggregations from Silver")
    parser.add_argument("--year",  type=int, default=None)
    parser.add_argument("--month", type=int, default=None)
    args = parser.parse_args()
    build_gold(args.year, args.month)


if __name__ == "__main__":
    main()
