"""PySpark sales aggregation pipeline: load CSV, transform, aggregate, save."""

from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "sales.csv"
OUTPUT_DIR = PROJECT_ROOT / "output"


def create_spark_session() -> SparkSession:
    """Step 1: Setup Spark session."""
    return (
        SparkSession.builder.appName("SalesAggregationPipeline")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_sales(spark: SparkSession, csv_path: Path):
    """Step 2: Load CSV dataset."""
    return (
        spark.read.csv(str(csv_path), header=True, inferSchema=True)
        .filter(F.col("order_id").isNotNull())
    )


def transform_sales(raw_df):
    """Step 3: Perform transformations."""
    return (
        raw_df.withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
        .withColumn("order_month", F.date_format("order_date", "yyyy-MM"))
        .filter(F.col("quantity") > 0)
        .filter(F.col("unit_price") > 0)
    )


def aggregate_sales(sales_df):
    """Step 4: Aggregate sales metrics."""
    by_region = sales_df.groupBy("region").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.avg("line_total"), 2).alias("avg_line_total"),
    )

    by_category = sales_df.groupBy("product_category").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
        F.countDistinct("order_id").alias("order_count"),
    )

    by_region_category = sales_df.groupBy("region", "product_category").agg(
        F.sum("line_total").alias("total_sales"),
        F.sum("quantity").alias("total_units"),
    )

    monthly_summary = sales_df.groupBy("order_month").agg(
        F.sum("line_total").alias("total_sales"),
        F.countDistinct("order_id").alias("order_count"),
    )

    return {
        "sales_by_region": by_region,
        "sales_by_category": by_category,
        "sales_by_region_category": by_region_category,
        "sales_monthly_summary": monthly_summary,
    }


def save_tables(aggregates: dict[str, object], output_dir: Path) -> None:
    """Step 5: Save output tables."""
    output_dir.mkdir(parents=True, exist_ok=True)

    for name, df in aggregates.items():
        target = output_dir / name
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(target))
        )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = load_sales(spark, DATA_PATH)
        sales_df = transform_sales(raw_df)
        aggregates = aggregate_sales(sales_df)
        save_tables(aggregates, OUTPUT_DIR)

        print("Pipeline complete. Output tables:")
        for name in aggregates:
            print(f"  - {OUTPUT_DIR / name}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
