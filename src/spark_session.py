"""
Spark session factory.
Handles both local development and Databricks environments.
Import get_spark() in every PySpark script instead of
creating sessions inline.
"""
from pyspark.sql import SparkSession
from src.config import SPARK_APP_NAME, SPARK_MASTER


def get_spark(app_name: str = SPARK_APP_NAME) -> SparkSession:
    """
    Create or retrieve a SparkSession with Delta Lake support.

    On Databricks the session already exists — SparkSession.builder
    returns the running session automatically.
    Locally it creates a new session with Delta Lake configured.
    """
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # Delta Lake configuration
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        # Performance: use the modern Spark UI and adaptive execution
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Reduce default shuffle partitions for local runs
        .config("spark.sql.shuffle.partitions", "8")
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")   # suppress INFO noise
    return spark


def stop_spark(spark: SparkSession) -> None:
    """Gracefully stop a local Spark session. No-op on Databricks."""
    try:
        spark.stop()
    except Exception:
        pass
