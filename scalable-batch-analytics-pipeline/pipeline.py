"""
End-to-end batch analytics pipeline orchestrator.

Raw CSV -> Bronze (Parquet) -> Silver (Delta) -> Gold (Delta/Parquet)
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from logging_config import log_pipeline_event, setup_logger
from spark.bronze_ingest import ingest_new_batches
from spark.gold_aggregate import build_gold_layer, build_gold_tables, read_silver
from spark.silver_transform import build_silver_layer
from spark.validation import (
    merge_reports,
    validate_gold_layer,
    validate_raw_schema,
    validate_raw_values,
    validate_silver_layer,
)
from spark_session import create_spark_session

logger = setup_logger("pipeline", "pipeline.log")


def validate_raw_batches(batch_paths: list[Path]) -> None:
    """Run pre-ingest validation on pending CSV files."""
    spark = create_spark_session("RawValidation")
    try:
        reports = []
        for csv_path in batch_paths:
            df = spark.read.csv(str(csv_path), header=True, inferSchema=True)
            reports.append(validate_raw_schema(df))
            reports.append(validate_raw_values(df))

        report = merge_reports(*reports)
        if not report.success:
            failed = [r.check_name for r in report.results if not r.passed]
            raise ValueError(f"Raw validation failed: {failed}")
        log_pipeline_event("validate_raw", "success", {"files": len(batch_paths)})
    finally:
        spark.stop()


def run_spark_pipeline(force_all: bool = False) -> dict:
    """Execute Bronze -> Silver -> Gold with validation gates."""
    spark = create_spark_session()

    try:
        logger.info("Step 1: Bronze — incremental ingest into partitioned Parquet")
        bronze_metrics = ingest_new_batches(spark, force_all=force_all)
        log_pipeline_event("bronze", "success", bronze_metrics)

        logger.info("Step 2: Silver — clean, validate, merge into partitioned Delta")
        silver_metrics = build_silver_layer(spark)
        silver_df = read_silver(spark)
        silver_report = validate_silver_layer(silver_df)
        if not silver_report.success:
            failed = [r.check_name for r in silver_report.results if not r.passed]
            raise ValueError(f"Silver validation failed: {failed}")
        log_pipeline_event("silver", "success", silver_metrics)

        logger.info("Step 3: Gold — build partitioned analytics marts")
        gold_counts = build_gold_layer(spark)
        gold_tables = build_gold_tables(silver_df)
        gold_report = validate_gold_layer(gold_tables)
        if not gold_report.success:
            failed = [r.check_name for r in gold_report.results if not r.passed]
            raise ValueError(f"Gold validation failed: {failed}")
        log_pipeline_event("gold", "success", gold_counts)

        logger.info("Spark pipeline complete")
        return {
            "bronze": bronze_metrics,
            "silver": silver_metrics,
            "gold": gold_counts,
        }
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the batch analytics Spark pipeline")
    parser.add_argument(
        "--force-all",
        action="store_true",
        help="Re-ingest all batch files regardless of manifest state",
    )
    parser.add_argument(
        "--skip-raw-validation",
        action="store_true",
        help="Skip pre-ingest raw CSV validation",
    )
    args = parser.parse_args()

    from config import DATA_DIR

    batch_paths = sorted(DATA_DIR.glob("orders_batch*.csv"))
    if not batch_paths:
        raise FileNotFoundError(f"No batch files found in {DATA_DIR}")

    if not args.skip_raw_validation:
        validate_raw_batches(batch_paths)

    metrics = run_spark_pipeline(force_all=args.force_all)
    log_pipeline_event("pipeline", "success", metrics)
    print("Pipeline complete. See logs/ for structured run history.")


if __name__ == "__main__":
    main()
