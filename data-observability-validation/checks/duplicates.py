"""Duplicate detection checks."""

from __future__ import annotations

from dataclasses import dataclass

import great_expectations as gx
import pandas as pd

from config import COMPOSITE_KEY_COLUMNS, PRIMARY_KEY


@dataclass
class DuplicateCheckResult:
    """Result of a composite-key duplicate scan."""

    passed: bool
    duplicate_row_count: int
    duplicate_groups: int


def add_duplicate_expectations(validator: gx.Validator) -> None:
    """Register Great Expectations duplicate rules on the validator."""
    validator.expect_column_values_to_be_unique(PRIMARY_KEY)


def detect_composite_duplicates(dataframe: pd.DataFrame) -> DuplicateCheckResult:
    """Find rows duplicated on a composite business key."""
    duplicated_mask = dataframe.duplicated(subset=COMPOSITE_KEY_COLUMNS, keep=False)
    duplicate_rows = dataframe.loc[duplicated_mask]

    return DuplicateCheckResult(
        passed=duplicate_rows.empty,
        duplicate_row_count=len(duplicate_rows),
        duplicate_groups=duplicate_rows.groupby(COMPOSITE_KEY_COLUMNS).ngroups
        if not duplicate_rows.empty
        else 0,
    )
