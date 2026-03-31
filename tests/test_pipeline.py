"""
Unit tests for the NYC Taxi Data Pipeline.
Run with: pytest tests/ -v

Tests use small in-memory DataFrames — no real data download needed.
This makes tests fast and CI-friendly.
"""
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, IntegerType, DoubleType,
    TimestampType, StringType
)
from datetime import datetime

from src.silver.clean_taxi import (
    rename_and_cast, filter_invalid_rows,
    deduplicate, add_derived_columns
)
from src.gold.aggregate import (
    build_daily_revenue, build_payment_analysis, build_distance_bands
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Shared SparkSession for all tests. Session-scoped = created once.

    Note: Delta Lake extensions are intentionally excluded here.
    Tests use only in-memory DataFrames — no Delta reads/writes.
    Delta config requires the JAR on the classpath which is not
    guaranteed in all CI environments.
    """
    spark = (
        SparkSession.builder
        .appName("nyc-taxi-tests")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()


@pytest.fixture
def raw_schema():
    """Raw schema matching TLC Parquet format."""
    return StructType([
        StructField("VendorID",               IntegerType(), True),
        StructField("tpep_pickup_datetime",   TimestampType(), True),
        StructField("tpep_dropoff_datetime",  TimestampType(), True),
        StructField("passenger_count",        IntegerType(), True),
        StructField("trip_distance",          DoubleType(), True),
        StructField("RatecodeID",             IntegerType(), True),
        StructField("store_and_fwd_flag",     StringType(), True),
        StructField("PULocationID",           IntegerType(), True),
        StructField("DOLocationID",           IntegerType(), True),
        StructField("payment_type",           IntegerType(), True),
        StructField("fare_amount",            DoubleType(), True),
        StructField("extra",                  DoubleType(), True),
        StructField("mta_tax",                DoubleType(), True),
        StructField("tip_amount",             DoubleType(), True),
        StructField("tolls_amount",           DoubleType(), True),
        StructField("improvement_surcharge",  DoubleType(), True),
        StructField("total_amount",           DoubleType(), True),
        StructField("congestion_surcharge",   DoubleType(), True),
    ])


@pytest.fixture
def sample_raw_df(spark, raw_schema):
    """
    Small in-memory DataFrame with a mix of valid and invalid rows.
    Designed to test all cleaning rules.
    """
    t1 = datetime(2024, 1, 15, 9, 30)
    t2 = datetime(2024, 1, 15, 9, 45)
    t3 = datetime(2024, 1, 15, 10, 0)

    data = [
        # (VendorID, pickup, dropoff, pax, dist, rate, s&f, PU, DO, pay,  fare, extra, mta, tip, toll, imp, total, cong)
        (1, t1, t2, 2, 3.5, 1, "N", 100, 200, 1,  12.50, 0.5, 0.5, 2.5, 0.0, 0.3, 16.3,  2.5),  # valid
        (2, t1, t3, 1, 5.2, 1, "N", 150, 250, 2,  18.00, 0.5, 0.5, 0.0, 0.0, 0.3, 19.3,  2.5),  # valid
        (1, t1, t2, 2, 3.5, 1, "N", 100, 200, 1,  12.50, 0.5, 0.5, 2.5, 0.0, 0.3, 16.3,  2.5),  # DUPLICATE of row 1
        (1, t2, t3, 1, 0.8, 1, "N",  50, 100, 1,  -5.00, 0.0, 0.5, 0.0, 0.0, 0.3,  0.3,  0.0),  # INVALID: negative fare
        (2, t2, t3, 0, 2.1, 1, "N",  75, 175, 2,   8.00, 0.5, 0.5, 1.0, 0.0, 0.3,  10.3, 0.0),  # INVALID: 0 passengers
        (1, t3, t2, 1, 4.0, 1, "N", 200, 300, 1,  15.00, 0.5, 0.5, 3.0, 0.0, 0.3,  19.3, 2.5),  # INVALID: dropoff < pickup
        (2, t1, t3, 3, 7.5, 1, "N", 300, 400, 1,  25.00, 0.5, 0.5, 5.0, 0.0, 0.3,  31.3, 2.5),  # valid
    ]
    return spark.createDataFrame(data, schema=raw_schema)


# ── Silver tests ──────────────────────────────────────────────────────────────

class TestSilverCleaning:

    def test_filter_removes_negative_fares(self, sample_raw_df):
        df_renamed = rename_and_cast(sample_raw_df)
        df_filtered, removals = filter_invalid_rows(df_renamed)
        assert removals["negative_fares"] == 1

    def test_filter_removes_invalid_passengers(self, sample_raw_df):
        df_renamed = rename_and_cast(sample_raw_df)
        df_filtered, removals = filter_invalid_rows(df_renamed)
        assert removals["invalid_passengers"] == 1

    def test_filter_removes_reversed_datetimes(self, sample_raw_df):
        df_renamed = rename_and_cast(sample_raw_df)
        df_filtered, removals = filter_invalid_rows(df_renamed)
        assert removals["dropoff_before_pickup"] == 1

    def test_dedup_removes_exact_duplicates(self, sample_raw_df):
        # Dedup must run AFTER filter (matching pipeline order) so that
        # only the true business-key duplicate (row 3 == row 1) is counted.
        df_renamed = rename_and_cast(sample_raw_df)
        df_filtered, _ = filter_invalid_rows(df_renamed)
        df_dedup, dupes = deduplicate(df_filtered)
        assert dupes == 1

    def test_valid_rows_survive_cleaning(self, sample_raw_df):
        """After cleaning, exactly 3 valid rows should remain (rows 1, 2, 7)."""
        df = rename_and_cast(sample_raw_df)
        df, _ = filter_invalid_rows(df)
        df, _ = deduplicate(df)
        assert df.count() == 3

    def test_derived_columns_added(self, sample_raw_df):
        df = rename_and_cast(sample_raw_df)
        df, _ = filter_invalid_rows(df)
        df, _ = deduplicate(df)
        df = add_derived_columns(df)

        expected_cols = [
            "trip_duration_minutes",
            "fare_per_mile",
            "pickup_date",
            "pickup_hour",
            "pickup_dow",
            "payment_type_label",
            "is_outlier",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing derived column: {col}"

    def test_trip_duration_is_positive(self, sample_raw_df):
        df = rename_and_cast(sample_raw_df)
        df, _ = filter_invalid_rows(df)
        df, _ = deduplicate(df)
        df = add_derived_columns(df)

        bad = df.filter(F.col("trip_duration_minutes") <= 0).count()
        assert bad == 0, f"Found {bad} trips with non-positive duration"

    def test_fare_per_mile_non_negative(self, sample_raw_df):
        df = rename_and_cast(sample_raw_df)
        df, _ = filter_invalid_rows(df)
        df, _ = deduplicate(df)
        df = add_derived_columns(df)

        bad = df.filter(
            F.col("fare_per_mile").isNotNull() &
            (F.col("fare_per_mile") < 0)
        ).count()
        assert bad == 0


# ── Gold tests ────────────────────────────────────────────────────────────────

class TestGoldAggregations:

    @pytest.fixture
    def silver_df(self, spark, sample_raw_df):
        """Silver-cleaned version of the sample data for Gold tests."""
        df = rename_and_cast(sample_raw_df)
        df, _ = filter_invalid_rows(df)
        df, _ = deduplicate(df)
        df = add_derived_columns(df)
        df = df.withColumn("is_outlier", F.lit(False))
        return df

    def test_daily_revenue_has_rows(self, silver_df):
        result = build_daily_revenue(silver_df)
        assert result.count() > 0

    def test_daily_revenue_columns(self, silver_df):
        result = build_daily_revenue(silver_df)
        assert "total_trips"     in result.columns
        assert "total_revenue"   in result.columns
        assert "avg_fare"        in result.columns

    def test_payment_analysis_has_rows(self, silver_df):
        result = build_payment_analysis(silver_df)
        assert result.count() > 0

    def test_distance_bands_correct_labels(self, silver_df):
        result = build_distance_bands(silver_df)
        bands = [r["distance_band"] for r in result.collect()]
        # Note: uses en-dash (–) not hyphen (-) — must match aggregate.py exactly
        valid_bands = {"< 1 mile", "1\u20133 miles", "3\u20137 miles", "7\u201315 miles", "15+ miles"}
        for band in bands:
            assert band in valid_bands, f"Unexpected distance band: {band}"

    def test_total_trips_matches_input(self, silver_df):
        """Sum of trips across all daily rows must equal total Silver rows."""
        input_count = silver_df.filter(F.col("is_outlier") == False).count()
        result = build_daily_revenue(silver_df)
        total_from_gold = result.agg(F.sum("total_trips")).collect()[0][0]
        assert total_from_gold == input_count
