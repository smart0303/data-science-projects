"""Create the Great Expectations validation suite for ingestion orders."""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import great_expectations as gx
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "ingestion_orders.csv"
GX_ROOT = PROJECT_ROOT / "gx"

EXPECTED_COLUMNS = [
    "order_id",
    "product",
    "category",
    "quantity",
    "unit_price",
    "order_date",
    "ingested_at",
]

INTEGER_COLUMNS = ["order_id", "quantity"]
FLOAT_COLUMNS = ["unit_price"]
NOT_NULL_COLUMNS = EXPECTED_COLUMNS


def build_expectations(validator: gx.Validator) -> None:
    """Step 3 & 4: schema checks plus null/duplicate/freshness rules."""
    validator.expect_table_columns_to_match_ordered_list(column_list=EXPECTED_COLUMNS)

    for column in INTEGER_COLUMNS:
        validator.expect_column_values_to_be_in_type_list(
            column,
            type_list=["int64", "int32", "Integer"],
        )

    for column in FLOAT_COLUMNS:
        validator.expect_column_values_to_be_in_type_list(
            column,
            type_list=["float64", "float32", "Float64", "Float"],
        )

    for column in NOT_NULL_COLUMNS:
        validator.expect_column_values_to_not_be_null(column)

    validator.expect_column_values_to_be_unique("order_id")

    today = datetime.now().date()
    validator.expect_column_max_to_be_between(
        "ingested_at",
        min_value=(today - timedelta(days=7)).isoformat(),
        max_value=today.isoformat(),
    )
    validator.expect_column_max_to_be_between(
        "order_date",
        min_value=(today - timedelta(days=30)).isoformat(),
        max_value=today.isoformat(),
    )


def ensure_datasource(context: gx.FileDataContext):
    if "ingestion_datasource" in context.data_sources.all():
        datasource = context.data_sources.get("ingestion_datasource")
    else:
        datasource = context.data_sources.add_pandas("ingestion_datasource")

    try:
        asset = datasource.get_asset("orders")
    except (LookupError, KeyError, ValueError):
        asset = datasource.add_dataframe_asset("orders")

    batch_definitions = {definition.name: definition for definition in asset.batch_definitions}
    if "daily_batch" not in batch_definitions:
        batch_definition = asset.add_batch_definition_whole_dataframe("daily_batch")
    else:
        batch_definition = batch_definitions["daily_batch"]

    return asset, batch_definition


def ensure_checkpoint(
    context: gx.FileDataContext,
    batch_definition,
    suite: gx.ExpectationSuite,
) -> None:
    validation_definition_names = {
        definition.name for definition in context.validation_definitions.all()
    }
    if "ingestion_validation" in validation_definition_names:
        validation_definition = context.validation_definitions.get("ingestion_validation")
    else:
        validation_definition = gx.ValidationDefinition(
            name="ingestion_validation",
            data=batch_definition,
            suite=suite,
        )
        context.validation_definitions.add(validation_definition)

    if "ingestion_checkpoint" not in {checkpoint.name for checkpoint in context.checkpoints.all()}:
        checkpoint = gx.Checkpoint(
            name="ingestion_checkpoint",
            validation_definitions=[validation_definition],
        )
        context.checkpoints.add(checkpoint)


def create_validation_suite() -> gx.FileDataContext:
    """Step 2: create datasource, suite, and checkpoint."""
    context = gx.get_context(project_root_dir=str(PROJECT_ROOT))
    asset, batch_definition = ensure_datasource(context)

    suite_names = {suite.name for suite in context.suites.all()}
    if "ingestion_orders_suite" in suite_names:
        suite = context.suites.get("ingestion_orders_suite")
    else:
        suite = context.suites.add(gx.ExpectationSuite(name="ingestion_orders_suite"))

    dataframe = pd.read_csv(DATA_PATH)
    batch_request = asset.build_batch_request(options={"dataframe": dataframe})
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    build_expectations(validator)
    context.suites.add_or_update(validator.expectation_suite)
    ensure_checkpoint(context, batch_definition, validator.expectation_suite)

    print(f"Great Expectations project ready at {GX_ROOT}")
    return context


if __name__ == "__main__":
    create_validation_suite()
    print("Validation suite 'ingestion_orders_suite' created.")
