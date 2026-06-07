"""Step 1: Incrementally ingest raw CSV batches into partitioned Bronze Parquet."""

from __future__ import annotations

import json
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import (
    BATCH_MANIFEST,
    BRONZE_FORMAT,
    BRONZE_ORDERS,
    BRONZE_PARTITION_COL,
    DATA_DIR,
    STATE_DIR,
)
from logging_config import setup_logger

logger = setup_logger("spark.bronze", "bronze_ingest.log")


def _load_manifest() -> set[str]:
    if not BATCH_MANIFEST.exists():
        return set()
    payload = json.loads(BATCH_MANIFEST.read_text(encoding="utf-8"))
    return set(payload.get("processed_files", []))


def _save_manifest(processed_files: set[str]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    payload = {"processed_files": sorted(processed_files)}
    BATCH_MANIFEST.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def _pending_batches(batch_paths: list[Path] | None = None) -> list[Path]:
    if batch_paths is None:
        batch_paths = sorted(DATA_DIR.glob("orders_batch*.csv"))

    processed = _load_manifest()
    pending = [path for path in batch_paths if path.name not in processed]
    return pending


def ingest_csv_batch(spark: SparkSession, csv_path: Path) -> int:
    """Append one CSV file to Bronze with ingestion metadata and date partition."""
    source_name = csv_path.name

    raw_df = (
        spark.read.csv(str(csv_path), header=True, inferSchema=True)
        .withColumn("_source_file", F.lit(source_name))
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn(BRONZE_PARTITION_COL, F.to_date("_ingested_at"))
    )

    BRONZE_ORDERS.parent.mkdir(parents=True, exist_ok=True)
    (
        raw_df.write.format(BRONZE_FORMAT)
        .mode("append")
        .partitionBy(BRONZE_PARTITION_COL)
        .save(str(BRONZE_ORDERS))
    )

    row_count = raw_df.count()
    logger.info("Appended %s rows from %s to Bronze", row_count, source_name)
    return row_count


def ingest_new_batches(
    spark: SparkSession,
    batch_paths: list[Path] | None = None,
    force_all: bool = False,
) -> dict[str, int]:
    """
    Ingest only batches not yet recorded in the manifest.

    Set force_all=True to reprocess every batch file (useful after lake reset).
    """
    if batch_paths is None:
        batch_paths = sorted(DATA_DIR.glob("orders_batch*.csv"))

    if not batch_paths:
        raise FileNotFoundError(f"No order batch files found under {DATA_DIR}")

    processed = set() if force_all else _load_manifest()
    metrics: dict[str, int] = {"files_processed": 0, "rows_ingested": 0, "files_skipped": 0}

    for csv_path in batch_paths:
        if csv_path.name in processed:
            metrics["files_skipped"] += 1
            logger.info("Skipping already processed batch: %s", csv_path.name)
            continue

        rows = ingest_csv_batch(spark, csv_path)
        processed.add(csv_path.name)
        metrics["files_processed"] += 1
        metrics["rows_ingested"] += rows

    _save_manifest(processed)
    logger.info(
        "Bronze ingest complete: %s files, %s rows, %s skipped",
        metrics["files_processed"],
        metrics["rows_ingested"],
        metrics["files_skipped"],
    )
    return metrics
