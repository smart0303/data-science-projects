"""Step 1: Ingest raw CSV batches into the Bronze Parquet layer."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import BRONZE_FORMAT, BRONZE_ORDERS, DATA_DIR


def ingest_csv_batch(spark: SparkSession, csv_path: Path) -> int:
    """Append one CSV file to Bronze with ingestion metadata columns."""
    source_name = csv_path.name

    raw_df = (
        spark.read.csv(str(csv_path), header=True, inferSchema=True)
        .withColumn("_source_file", F.lit(source_name))
        .withColumn("_ingested_at", F.current_timestamp())
    )

    BRONZE_ORDERS.parent.mkdir(parents=True, exist_ok=True)
    (
        raw_df.write.format(BRONZE_FORMAT)
        .mode("append")
        .save(str(BRONZE_ORDERS))
    )

    return raw_df.count()


def ingest_all_batches(spark: SparkSession, batch_paths: list[Path] | None = None) -> int:
    """Ingest every CSV in data/ (or the provided list) into Bronze."""
    if batch_paths is None:
        batch_paths = sorted(DATA_DIR.glob("orders_batch*.csv"))

    if not batch_paths:
        raise FileNotFoundError(f"No order batch files found under {DATA_DIR}")

    total_rows = 0
    for csv_path in batch_paths:
        rows = ingest_csv_batch(spark, csv_path)
        total_rows += rows
        print(f"  Bronze: appended {rows} rows from {csv_path.name}")

    return total_rows
