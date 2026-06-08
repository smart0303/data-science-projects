"""Configuration for data observability and validation checks."""

from __future__ import annotations

from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "sensor_events.csv"
GX_ROOT = PROJECT_ROOT / "gx"
REPORTS_DIR = PROJECT_ROOT / "reports"

DATASOURCE_NAME = "observability_datasource"
ASSET_NAME = "sensor_events"
BATCH_DEFINITION_NAME = "events_batch"
SUITE_NAME = "sensor_events_suite"
VALIDATION_NAME = "sensor_events_validation"
CHECKPOINT_NAME = "observability_checkpoint"

EXPECTED_COLUMNS = [
    "event_id",
    "sensor_id",
    "device_type",
    "metric_value",
    "event_timestamp",
    "loaded_at",
]

PRIMARY_KEY = "event_id"
COMPOSITE_KEY_COLUMNS = ["sensor_id", "event_timestamp"]

NOT_NULL_COLUMNS = EXPECTED_COLUMNS
NUMERIC_COLUMNS = ["metric_value"]

FRESHNESS_COLUMN = "loaded_at"
MAX_LOAD_AGE_HOURS = 24
MAX_EVENT_AGE_DAYS = 7

ANOMALY_COLUMN = "metric_value"
METRIC_VALUE_MIN = 0.0
METRIC_VALUE_MAX = 100.0
METRIC_MEAN_MIN = 30.0
METRIC_MEAN_MAX = 50.0
Z_SCORE_THRESHOLD = 3.0
