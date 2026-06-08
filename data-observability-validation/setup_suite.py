"""Step 1: Build the Great Expectations validation suite."""

from __future__ import annotations

from pathlib import Path

import great_expectations as gx
import pandas as pd

from checks.anomalies import add_anomaly_expectations
from checks.duplicates import add_duplicate_expectations
from checks.freshness import add_freshness_expectations
from config import (
    ASSET_NAME,
    BATCH_DEFINITION_NAME,
    CHECKPOINT_NAME,
    DATA_PATH,
    DATASOURCE_NAME,
    EXPECTED_COLUMNS,
    NOT_NULL_COLUMNS,
    PROJECT_ROOT,
    SUITE_NAME,
    VALIDATION_NAME,
)


def build_schema_expectations(validator: gx.Validator) -> None:
    """Baseline schema and nullability rules."""
    validator.expect_table_columns_to_match_ordered_list(column_list=EXPECTED_COLUMNS)
    for column in NOT_NULL_COLUMNS:
        validator.expect_column_values_to_not_be_null(column)


def build_expectations(validator: gx.Validator) -> None:
    """Register schema, duplicate, freshness, and anomaly expectations."""
    build_schema_expectations(validator)
    add_duplicate_expectations(validator)
    add_freshness_expectations(validator)
    add_anomaly_expectations(validator)


def ensure_datasource(context: gx.FileDataContext):
    if DATASOURCE_NAME in context.data_sources.all():
        datasource = context.data_sources.get(DATASOURCE_NAME)
    else:
        datasource = context.data_sources.add_pandas(DATASOURCE_NAME)

    try:
        asset = datasource.get_asset(ASSET_NAME)
    except (LookupError, KeyError, ValueError):
        asset = datasource.add_dataframe_asset(ASSET_NAME)

    batch_definitions = {definition.name: definition for definition in asset.batch_definitions}
    if BATCH_DEFINITION_NAME not in batch_definitions:
        batch_definition = asset.add_batch_definition_whole_dataframe(BATCH_DEFINITION_NAME)
    else:
        batch_definition = batch_definitions[BATCH_DEFINITION_NAME]

    return asset, batch_definition


def ensure_checkpoint(
    context: gx.FileDataContext,
    batch_definition,
    suite: gx.ExpectationSuite,
) -> None:
    validation_definition_names = {
        definition.name for definition in context.validation_definitions.all()
    }
    if VALIDATION_NAME in validation_definition_names:
        validation_definition = context.validation_definitions.get(VALIDATION_NAME)
    else:
        validation_definition = gx.ValidationDefinition(
            name=VALIDATION_NAME,
            data=batch_definition,
            suite=suite,
        )
        context.validation_definitions.add(validation_definition)

    if CHECKPOINT_NAME not in {checkpoint.name for checkpoint in context.checkpoints.all()}:
        checkpoint = gx.Checkpoint(
            name=CHECKPOINT_NAME,
            validation_definitions=[validation_definition],
        )
        context.checkpoints.add(checkpoint)


def create_validation_suite() -> gx.FileDataContext:
    """Create datasource, suite, and checkpoint for sensor events."""
    context = gx.get_context(project_root_dir=str(PROJECT_ROOT))
    asset, batch_definition = ensure_datasource(context)

    suite_names = {suite.name for suite in context.suites.all()}
    if SUITE_NAME in suite_names:
        suite = context.suites.get(SUITE_NAME)
    else:
        suite = context.suites.add(gx.ExpectationSuite(name=SUITE_NAME))

    dataframe = pd.read_csv(DATA_PATH)
    batch_request = asset.build_batch_request(options={"dataframe": dataframe})
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite,
    )

    build_expectations(validator)
    context.suites.add_or_update(validator.expectation_suite)
    ensure_checkpoint(context, batch_definition, validator.expectation_suite)

    print(f"Great Expectations project ready at {PROJECT_ROOT / 'gx'}")
    return context


if __name__ == "__main__":
    create_validation_suite()
    print(f"Validation suite '{SUITE_NAME}' created.")
