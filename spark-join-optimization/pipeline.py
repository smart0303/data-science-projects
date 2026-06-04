"""Spark join optimization: datasets, joins, repartition vs coalesce, cache, execution analysis."""

import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
OUTPUT_DIR = PROJECT_ROOT / "output"

# Large fact table size for partition-heavy shuffles
ORDER_ROW_COUNT = 80_000


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SparkJoinOptimization")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def partition_count(df) -> int:
    return df.rdd.getNumPartitions()


def timed_action(label: str, action) -> float:
    start = time.perf_counter()
    action()
    elapsed = time.perf_counter() - start
    print(f"  {label}: {elapsed:.3f}s")
    return elapsed


def step1_create_datasets(spark: SparkSession):
    """Step 1: Load dimension tables and generate a large orders fact table."""
    customers = spark.read.csv(
        str(DATA_DIR / "customers.csv"), header=True, inferSchema=True
    )
    products = spark.read.csv(
        str(DATA_DIR / "products.csv"), header=True, inferSchema=True
    )

    customer_ids = [row.customer_id for row in customers.select("customer_id").collect()]
    product_ids = [row.product_id for row in products.select("product_id").collect()]

    orders = (
        spark.range(0, ORDER_ROW_COUNT)
        .withColumn("order_id", F.col("id") + 1000)
        .withColumn(
            "customer_id",
            F.element_at(F.array(*[F.lit(i) for i in customer_ids]), (F.col("id") % 20) + 1),
        )
        .withColumn(
            "product_id",
            F.element_at(F.array(*[F.lit(i) for i in product_ids]), (F.col("id") % 20) + 1),
        )
        .withColumn("quantity", (F.col("id") % 5) + 1)
        .withColumn("order_date", F.date_add(F.lit("2026-01-01"), (F.col("id") % 90).cast("int")))
        .drop("id")
    )

    print("Step 1 — Datasets created")
    print(f"  customers: {customers.count():,} rows, {partition_count(customers)} partitions")
    print(f"  products:  {products.count():,} rows, {partition_count(products)} partitions")
    print(f"  orders:    {orders.count():,} rows, {partition_count(orders)} partitions")

    return customers, products, orders


def step2_perform_joins(customers, products, orders):
    """Step 2: Multi-table joins and enrichment."""
    orders_enriched = (
        orders.join(customers, on="customer_id", how="inner")
        .join(products, on="product_id", how="inner")
        .withColumn("line_total", F.col("quantity") * F.col("unit_price"))
    )

    customer_summary = orders_enriched.groupBy(
        "customer_id", "customer_name", "region", "segment"
    ).agg(
        F.count("order_id").alias("order_count"),
        F.sum("line_total").alias("total_revenue"),
        F.sum("quantity").alias("total_units"),
    )

    category_summary = orders_enriched.groupBy("category").agg(
        F.sum("line_total").alias("category_revenue"),
        F.countDistinct("order_id").alias("order_count"),
    )

    print("\nStep 2 — Joins complete")
    print(f"  orders_enriched: {orders_enriched.count():,} rows")
    print(f"  customer_summary: {customer_summary.count():,} rows")
    print(f"  category_summary: {category_summary.count():,} rows")

    return orders_enriched, customer_summary, category_summary


def step3_compare_repartition_coalesce(orders_enriched):
    """Step 3: Compare repartition (full shuffle) vs coalesce (narrow merge)."""
    base_partitions = partition_count(orders_enriched)
    print(f"\nStep 3 — Repartition vs coalesce (base: {base_partitions} partitions)")

    repartitioned = orders_enriched.repartition(16, "region")
    coalesced = orders_enriched.coalesce(2)

    print(f"  repartition(16, 'region'): {partition_count(repartitioned)} partitions")
    print(f"  coalesce(2):               {partition_count(coalesced)} partitions")

    print("\n  Timing count() after shuffle-heavy groupBy:")
    grouped = orders_enriched.groupBy("region", "category").agg(
        F.sum("line_total").alias("revenue")
    )

    timed_action("groupBy baseline (no reshape)", lambda: grouped.count())

    rep_grouped = grouped.repartition(16)
    timed_action("after repartition(16)", lambda: rep_grouped.count())

    coa_grouped = grouped.coalesce(2)
    timed_action("after coalesce(2)", lambda: coa_grouped.count())

    return repartitioned, coalesced, grouped


def step4_cache_transformed(spark, orders_enriched):
    """Step 4: Cache joined data and measure reuse."""
    cached = orders_enriched.cache()
    print("\nStep 4 — Cache transformed data")

    print("  First action (materializes cache):")
    timed_action("count (cold cache)", lambda: cached.count())

    print("  Repeated actions on cached DataFrame:")
    timed_action("sum line_total", lambda: cached.agg(F.sum("line_total")).collect())
    timed_action("filter + count", lambda: cached.filter(F.col("quantity") >= 3).count())

    print("\n  Cached partitions in memory:")
    for item in spark.sparkContext._jsc.sc().getRDDStorageInfo():
        print(f"    {item.name()}: {item.numPartitions()} partitions, {item.memSize()} bytes")

    cached.unpersist()
    return cached


def step5_analyze_execution(spark, orders_enriched, grouped):
    """Step 5: Explain plans and Spark UI metrics."""
    print("\nStep 5 — Execution behavior")

    print("\n  Physical plan (orders_enriched join lineage):")
    orders_enriched.explain(mode="formatted")

    print("\n  Physical plan (post-shuffle aggregation):")
    grouped.explain(mode="formatted")

    print("\n  Spark configuration relevant to joins:")
    conf = spark.sparkContext.getConf()
    for key in ("spark.sql.shuffle.partitions", "spark.default.parallelism"):
        print(f"    {key} = {conf.get(key, 'n/a')}")

def save_results(customer_summary, category_summary, output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    for name, df in (
        ("customer_summary", customer_summary),
        ("category_summary", category_summary),
    ):
        (
            df.coalesce(1)
            .write.mode("overwrite")
            .option("header", True)
            .csv(str(output_dir / name))
        )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        customers, products, orders = step1_create_datasets(spark)
        orders_enriched, customer_summary, category_summary = step2_perform_joins(
            customers, products, orders
        )
        _, _, grouped = step3_compare_repartition_coalesce(orders_enriched)
        step4_cache_transformed(spark, orders_enriched)
        step5_analyze_execution(spark, orders_enriched, grouped)
        save_results(customer_summary, category_summary, OUTPUT_DIR)

        print("\nPipeline complete. Output tables:")
        print(f"  - {OUTPUT_DIR / 'customer_summary'}")
        print(f"  - {OUTPUT_DIR / 'category_summary'}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
