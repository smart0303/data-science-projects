"""Step 3: Build partitioned Gold analytics tables from Silver."""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import GOLD_DIR, GOLD_FORMAT, SILVER_FORMAT, SILVER_ORDERS, SILVER_PARTITION_COL
from logging_config import setup_logger

logger = setup_logger("spark.gold", "gold_aggregate.log")


def read_silver(spark: SparkSession):
    """Load the curated Silver Delta table."""
    return spark.read.format(SILVER_FORMAT).load(str(SILVER_ORDERS))


def build_gold_tables(silver_df) -> dict[str, object]:
    """Aggregate Silver orders into business-facing mart tables."""
    by_region = silver_df.groupBy("region").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("line_total"), 2).alias("avg_line_total"),
    )

    by_category = silver_df.groupBy("product_category").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("order_count"),
    )

    by_region_category = silver_df.groupBy("region", "product_category").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
    )

    monthly_summary = silver_df.groupBy(SILVER_PARTITION_COL).agg(
        F.sum("line_total").alias("total_sales"),
        F.countDistinct("order_id").alias("order_count"),
        F.sum("quantity").alias("total_units"),
    ).withColumnRenamed(SILVER_PARTITION_COL, "order_month")

    order_detail = silver_df.select(
        "order_id",
        "order_date",
        "region",
        "product_category",
        "product",
        "quantity",
        "unit_price",
        "line_total",
        "order_month",
        "ingested_at",
    )

    return {
        "sales_by_region": by_region,
        "sales_by_category": by_category,
        "sales_by_region_category": by_region_category,
        "sales_monthly_summary": monthly_summary,
        "order_detail": order_detail,
    }


def write_gold_tables(tables: dict[str, object]) -> None:
    """Persist Gold marts as Delta and Parquet for downstream consumers."""
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    for name, df in tables.items():
        delta_path = GOLD_DIR / name
        parquet_path = GOLD_DIR / f"{name}_parquet"

        writer = df.write.format(GOLD_FORMAT).mode("overwrite")
        if name == "order_detail":
            writer = writer.partitionBy("order_month")
        writer.save(str(delta_path))

        parquet_writer = df.write.format("parquet").mode("overwrite")
        if name == "order_detail":
            parquet_writer = parquet_writer.partitionBy("order_month")
        parquet_writer.save(str(parquet_path))

        logger.info("Wrote Gold table %s (delta + parquet)", name)


def build_gold_layer(spark: SparkSession) -> dict[str, int]:
    """Read Silver and materialize Gold analytics tables."""
    silver_df = read_silver(spark)
    tables = build_gold_tables(silver_df)
    write_gold_tables(tables)
    return {name: df.count() for name, df in tables.items()}
