"""Standalone validation runner for CI and Airflow quality gates."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import DATA_DIR, GOLD_DIR, SILVER_ORDERS
from spark.gold_aggregate import build_gold_tables
from spark.validation import (
    merge_reports,
    validate_gold_layer,
    validate_raw_schema,
    validate_raw_values,
    validate_silver_layer,
)
from spark_session import create_spark_session


def validate_raw() -> bool:
    spark = create_spark_session("ValidateRaw")
    try:
        reports = []
        for csv_path in sorted(DATA_DIR.glob("orders_batch*.csv")):
            df = spark.read.csv(str(csv_path), header=True, inferSchema=True)
            reports.append(validate_raw_schema(df))
            reports.append(validate_raw_values(df))
        return merge_reports(*reports).success
    finally:
        spark.stop()


def validate_silver() -> bool:
    if not SILVER_ORDERS.exists():
        print("Silver layer not found — run pipeline.py first")
        return False

    spark = create_spark_session("ValidateSilver")
    try:
        silver_df = spark.read.format("delta").load(str(SILVER_ORDERS))
        return validate_silver_layer(silver_df).success
    finally:
        spark.stop()


def validate_gold() -> bool:
    if not GOLD_DIR.exists():
        print("Gold layer not found — run pipeline.py first")
        return False

    spark = create_spark_session("ValidateGold")
    try:
        silver_df = spark.read.format("delta").load(str(SILVER_ORDERS))
        tables = build_gold_tables(silver_df)
        return validate_gold_layer(tables).success
    finally:
        spark.stop()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run data validation checks")
    parser.add_argument(
        "--layer",
        choices=["raw", "silver", "gold", "all"],
        default="all",
        help="Which layer to validate",
    )
    args = parser.parse_args()

    checks = {
        "raw": validate_raw,
        "silver": validate_silver,
        "gold": validate_gold,
    }

    if args.layer == "all":
        success = all(fn() for fn in checks.values())
    else:
        success = checks[args.layer]()

    print(f"Validation success: {success}")
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
