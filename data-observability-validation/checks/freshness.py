"""Freshness monitoring checks."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

import great_expectations as gx
import pandas as pd

from config import FRESHNESS_COLUMN, MAX_EVENT_AGE_DAYS, MAX_LOAD_AGE_HOURS


@dataclass
class FreshnessStatus:
    """Freshness assessment for a timestamp column."""

    column: str
    latest_timestamp: datetime | None
    age_hours: float | None
    max_age_hours: float
    passed: bool
    message: str


def add_freshness_expectations(validator: gx.Validator) -> None:
    """Register Great Expectations freshness rules on the validator."""
    now = datetime.now()
    validator.expect_column_values_to_not_be_null(FRESHNESS_COLUMN)
    validator.expect_column_max_to_be_between(
        FRESHNESS_COLUMN,
        min_value=(now - timedelta(hours=MAX_LOAD_AGE_HOURS)).isoformat(sep=" "),
        max_value=now.isoformat(sep=" "),
    )
    validator.expect_column_max_to_be_between(
        "event_timestamp",
        min_value=(now - timedelta(days=MAX_EVENT_AGE_DAYS)).isoformat(sep=" "),
        max_value=now.isoformat(sep=" "),
    )


def assess_freshness(
    dataframe: pd.DataFrame,
    column: str = FRESHNESS_COLUMN,
    max_age_hours: float = MAX_LOAD_AGE_HOURS,
) -> FreshnessStatus:
    """Evaluate how recently the dataset was loaded."""
    if column not in dataframe.columns or dataframe[column].isna().all():
        return FreshnessStatus(
            column=column,
            latest_timestamp=None,
            age_hours=None,
            max_age_hours=max_age_hours,
            passed=False,
            message=f"Column '{column}' is missing or entirely null.",
        )

    latest = pd.to_datetime(dataframe[column]).max()
    age_hours = (datetime.now() - latest.to_pydatetime()).total_seconds() / 3600
    passed = age_hours <= max_age_hours

    return FreshnessStatus(
        column=column,
        latest_timestamp=latest.to_pydatetime(),
        age_hours=round(age_hours, 2),
        max_age_hours=max_age_hours,
        passed=passed,
        message=(
            f"Latest {column} is {age_hours:.1f}h old (limit {max_age_hours}h)."
            if passed
            else f"Stale data: {column} is {age_hours:.1f}h old (limit {max_age_hours}h)."
        ),
    )
