"""
Lakehouse Medallion Architecture pipeline.

Bronze (Parquet) -> Silver (Delta) -> Gold (Delta + Parquet)
"""

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipelines.bronze_ingest import ingest_all_batches
from pipelines.gold_aggregate import build_gold_layer
from pipelines.silver_transform import build_silver_layer
from spark_session import create_spark_session


def main() -> None:
    spark = create_spark_session()

    try:
        print("Step 1: Bronze — ingest raw CSV into Parquet")
        bronze_rows = ingest_all_batches(spark)
        print(f"  Total Bronze rows ingested: {bronze_rows}\n")

        print("Step 2: Silver — clean and merge into Delta")
        silver_rows = build_silver_layer(spark)
        print(f"  Silver records processed: {silver_rows}\n")

        print("Step 3: Gold — build analytics-ready marts")
        gold_tables = build_gold_layer(spark)
        print(f"  Gold tables created: {len(gold_tables)}\n")

        print("Pipeline complete. Lake layout:")
        print("  lake/bronze/orders/          (Parquet, append)")
        print("  lake/silver/orders/          (Delta, merge on order_id)")
        print("  lake/gold/<mart>/            (Delta)")
        print("  lake/gold/<mart>_parquet/    (Parquet)")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
