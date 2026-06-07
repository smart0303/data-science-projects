"""Data quality validation checks for raw and curated layers."""

from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from config import REQUIRED_RAW_COLUMNS
from logging_config import setup_logger

logger = setup_logger("spark.validation", "validation.log")


@dataclass
class ValidationResult:
    check_name: str
    passed: bool
    detail: str


@dataclass
class ValidationReport:
    layer: str
    results: list[ValidationResult] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return all(result.passed for result in self.results)

    def add(self, check_name: str, passed: bool, detail: str) -> None:
        status = "PASS" if passed else "FAIL"
        logger.info("[%s] %s — %s", status, check_name, detail)
        self.results.append(ValidationResult(check_name, passed, detail))


def validate_raw_schema(df: DataFrame) -> ValidationReport:
    """Ensure required source columns exist before Bronze ingest."""
    report = ValidationReport(layer="raw")
    columns = set(df.columns)
    missing = REQUIRED_RAW_COLUMNS - columns
    report.add(
        "required_columns_present",
        not missing,
        f"missing={sorted(missing)}" if missing else "all required columns present",
    )
    return report


def validate_raw_values(df: DataFrame) -> ValidationReport:
    """Basic value checks on incoming CSV data."""
    report = ValidationReport(layer="raw")
    row_count = df.count()
    report.add("non_empty_dataset", row_count > 0, f"row_count={row_count}")

    null_order_ids = df.filter(F.col("order_id").isNull()).count()
    report.add("order_id_not_null", null_order_ids == 0, f"null_order_ids={null_order_ids}")

    invalid_qty = df.filter((F.col("quantity").isNull()) | (F.col("quantity") <= 0)).count()
    report.add("positive_quantity", invalid_qty == 0, f"invalid_quantity_rows={invalid_qty}")

    invalid_price = df.filter((F.col("unit_price").isNull()) | (F.col("unit_price") <= 0)).count()
    report.add("positive_unit_price", invalid_price == 0, f"invalid_price_rows={invalid_price}")

    return report


def validate_silver_layer(df: DataFrame) -> ValidationReport:
    """Post-transform checks on the Silver Delta table."""
    report = ValidationReport(layer="silver")
    row_count = df.count()
    report.add("silver_not_empty", row_count > 0, f"row_count={row_count}")

    duplicate_ids = (
        df.groupBy("order_id")
        .agg(F.count("*").alias("cnt"))
        .filter(F.col("cnt") > 1)
        .count()
    )
    report.add("order_id_unique", duplicate_ids == 0, f"duplicate_order_ids={duplicate_ids}")

    invalid_totals = df.filter(F.col("line_total") <= 0).count()
    report.add("positive_line_total", invalid_totals == 0, f"invalid_line_total_rows={invalid_totals}")

    null_regions = df.filter(F.col("region").isNull()).count()
    report.add("region_not_null", null_regions == 0, f"null_region_rows={null_regions}")

    return report


def validate_gold_layer(tables: dict[str, DataFrame]) -> ValidationReport:
    """Ensure Gold marts are populated."""
    report = ValidationReport(layer="gold")
    for name, df in tables.items():
        count = df.count()
        report.add(f"{name}_not_empty", count > 0, f"row_count={count}")
    return report


def merge_reports(*reports: ValidationReport) -> ValidationReport:
    """Combine multiple validation reports into one."""
    merged = ValidationReport(layer="combined")
    for report in reports:
        merged.results.extend(report.results)
    return merged
