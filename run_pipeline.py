"""
PIPELINE RUNNER
────────────────────────────────────────────────────────────────────
Run the full pipeline end-to-end with a single command.
Useful for local development and testing.

Usage:
  python run_pipeline.py                        # Jan 2024
  python run_pipeline.py --year 2024 --month 1
  python run_pipeline.py --year 2024 --month 1 2 3  # 3 months
  python run_pipeline.py --skip-dq              # skip quality checks
"""
import argparse
import sys
import time
from datetime import datetime

# Add project root to path
import os
sys.path.insert(0, os.path.dirname(__file__))

from src.bronze.ingest_taxi import ingest_month
from src.silver.clean_taxi  import clean_month
from src.gold.aggregate     import build_gold
from data_quality.checks    import check_bronze, check_silver, check_gold, print_report
from src.spark_session      import get_spark


def run_pipeline(year: int, months: list, skip_dq: bool = False):
    start = time.time()

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║           NYC TAXI DATA PIPELINE — FULL RUN                  ║
║  Year: {year}  |  Months: {months}  |  DQ: {'disabled' if skip_dq else 'enabled'}
╚══════════════════════════════════════════════════════════════╝
    """)

    spark = get_spark()

    # ── Step 1: Bronze ────────────────────────────────────────────
    print("\n[1/4] BRONZE — Ingesting raw data...")
    t0 = time.time()
    bronze_results = []
    for month in months:
        result = ingest_month(year, month)
        bronze_results.append(result)
    print(f"      Done in {time.time()-t0:.1f}s")

    # ── Step 2: Bronze DQ ─────────────────────────────────────────
    if not skip_dq:
        print("\n[2a] BRONZE DATA QUALITY...")
        bronze_checks = check_bronze(spark)
        bronze_ok = all(r.status == "PASS" for r in bronze_checks)
        if not bronze_ok:
            print("\n  ✗ Bronze DQ failed — stopping pipeline")
            print_report(bronze_checks)
            sys.exit(1)

    # ── Step 3: Silver ────────────────────────────────────────────
    print("\n[2/4] SILVER — Cleaning and validating...")
    t0 = time.time()
    for month in months:
        clean_month(year, month)
    print(f"      Done in {time.time()-t0:.1f}s")

    # ── Step 4: Silver DQ ─────────────────────────────────────────
    if not skip_dq:
        print("\n[3a] SILVER DATA QUALITY...")
        silver_checks = check_silver(spark)
        silver_ok = all(r.status == "PASS" for r in silver_checks)
        if not silver_ok:
            print("\n  ✗ Silver DQ failed — stopping pipeline")
            print_report(silver_checks)
            sys.exit(1)

    # ── Step 5: Gold ──────────────────────────────────────────────
    print("\n[3/4] GOLD — Building aggregations...")
    t0 = time.time()
    for month in months:
        build_gold(year, month)
    print(f"      Done in {time.time()-t0:.1f}s")

    # ── Step 6: Gold DQ ───────────────────────────────────────────
    if not skip_dq:
        print("\n[4a] GOLD DATA QUALITY...")
        gold_checks = check_gold(spark)
        all_checks = bronze_checks + silver_checks + gold_checks
        print_report(all_checks)

    # ── Summary ───────────────────────────────────────────────────
    elapsed = time.time() - start
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║  PIPELINE COMPLETE ✓                                         ║
║  Year: {year}  |  Months: {months}                          
║  Total time: {elapsed:.1f}s                                  
║  Finished at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
╚══════════════════════════════════════════════════════════════╝
    """)


def main():
    parser = argparse.ArgumentParser(description="Run the full NYC Taxi pipeline")
    parser.add_argument("--year",    type=int, default=2024)
    parser.add_argument("--month",   type=int, nargs="+", default=[1])
    parser.add_argument("--skip-dq", action="store_true",
                        help="Skip data quality checks (faster for dev)")
    args = parser.parse_args()
    run_pipeline(args.year, args.month, args.skip_dq)


if __name__ == "__main__":
    main()
