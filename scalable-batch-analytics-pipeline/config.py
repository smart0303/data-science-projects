"""Lake paths, partition columns, and layer constants."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
LAKE_ROOT = PROJECT_ROOT / "lake"
LOGS_DIR = PROJECT_ROOT / "logs"
STATE_DIR = PROJECT_ROOT / "state"
DBT_DIR = PROJECT_ROOT / "dbt"

BRONZE_ORDERS = LAKE_ROOT / "bronze" / "orders"
SILVER_ORDERS = LAKE_ROOT / "silver" / "orders"
SILVER_ORDERS_PARQUET = LAKE_ROOT / "silver" / "orders_parquet"
GOLD_DIR = LAKE_ROOT / "gold"

BATCH_MANIFEST = STATE_DIR / "batch_manifest.json"
PIPELINE_RUNS_LOG = LOGS_DIR / "pipeline_runs.jsonl"

BRONZE_FORMAT = "parquet"
SILVER_FORMAT = "delta"
GOLD_FORMAT = "delta"

BRONZE_PARTITION_COL = "_ingest_date"
SILVER_PARTITION_COL = "order_month"

REQUIRED_RAW_COLUMNS = {
    "order_id",
    "order_date",
    "region",
    "product_category",
    "product",
    "quantity",
    "unit_price",
}
