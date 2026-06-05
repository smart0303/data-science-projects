"""Step 2: Clean Bronze records and persist the Silver Delta layer."""

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from config import BRONZE_FORMAT, BRONZE_ORDERS, SILVER_FORMAT, SILVER_ORDERS


def read_bronze(spark: SparkSession):
    """Load all Bronze Parquet records."""
    return spark.read.format(BRONZE_FORMAT).load(str(BRONZE_ORDERS))


def clean_orders(bronze_df):
    """Type-cast, validate, deduplicate, and enrich order records."""
    typed = (
        bronze_df.withColumn("order_id", F.col("order_id").cast("long"))
        .withColumn("order_date", F.to_date("order_date", "yyyy-MM-dd"))
        .withColumn("quantity", F.col("quantity").cast("int"))
        .withColumn("unit_price", F.col("unit_price").cast("double"))
        .withColumn(
            "ingested_at",
            F.coalesce(
                F.to_timestamp("ingested_at"),
                F.col("_ingested_at"),
            ),
        )
    )

    valid = typed.filter(
        F.col("order_id").isNotNull()
        & F.col("order_date").isNotNull()
        & (F.col("quantity") > 0)
        & (F.col("unit_price") > 0)
        & F.col("region").isNotNull()
        & F.col("product_category").isNotNull()
    )

    enriched = valid.withColumn(
        "line_total",
        F.round(F.col("quantity") * F.col("unit_price"), 2),
    ).withColumn("order_month", F.date_format("order_date", "yyyy-MM"))

    window = Window.partitionBy("order_id").orderBy(
        F.col("ingested_at").desc(),
        F.col("_ingested_at").desc(),
    )

    return (
        enriched.withColumn("_row_rank", F.row_number().over(window))
        .filter(F.col("_row_rank") == 1)
        .drop("_row_rank", "_source_file", "_ingested_at")
    )


def write_silver(clean_df, spark: SparkSession) -> int:
    """Merge cleaned records into the Silver Delta table by order_id."""
    SILVER_ORDERS.parent.mkdir(parents=True, exist_ok=True)

    if SILVER_ORDERS.exists() and (SILVER_ORDERS / "_delta_log").exists():
        (
            DeltaTable.forPath(spark, str(SILVER_ORDERS))
            .alias("target")
            .merge(
                clean_df.alias("source"),
                "target.order_id = source.order_id",
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
    else:
        (
            clean_df.write.format(SILVER_FORMAT)
            .mode("overwrite")
            .save(str(SILVER_ORDERS))
        )

    return clean_df.count()


def build_silver_layer(spark: SparkSession) -> int:
    """Read Bronze, clean, and write Silver."""
    bronze_df = read_bronze(spark)
    clean_df = clean_orders(bronze_df)
    row_count = write_silver(clean_df, spark)
    print(f"  Silver: merged {row_count} cleaned order records")
    return row_count
