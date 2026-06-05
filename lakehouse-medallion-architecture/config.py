"""Lake paths and layer constants for the medallion pipeline."""

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_DIR = PROJECT_ROOT / "data"
LAKE_ROOT = PROJECT_ROOT / "lake"

BRONZE_ORDERS = LAKE_ROOT / "bronze" / "orders"
SILVER_ORDERS = LAKE_ROOT / "silver" / "orders"
GOLD_DIR = LAKE_ROOT / "gold"

BRONZE_FORMAT = "parquet"
SILVER_FORMAT = "delta"
GOLD_FORMAT = "delta"
