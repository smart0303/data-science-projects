"""Anomaly detection and validation logic."""

from __future__ import annotations

from dataclasses import dataclass

import great_expectations as gx
import numpy as np
import pandas as pd

from config import (
    ANOMALY_COLUMN,
    METRIC_MEAN_MAX,
    METRIC_MEAN_MIN,
    METRIC_VALUE_MAX,
    METRIC_VALUE_MIN,
    Z_SCORE_THRESHOLD,
)


@dataclass
class AnomalyCheckResult:
    """Result of a z-score anomaly scan."""

    passed: bool
    outlier_count: int
    outlier_event_ids: list[int]
    z_score_threshold: float


def add_anomaly_expectations(validator: gx.Validator) -> None:
    """Register distribution and range expectations for anomaly validation."""
    validator.expect_column_values_to_not_be_null(ANOMALY_COLUMN)
    validator.expect_column_values_to_be_between(
        ANOMALY_COLUMN,
        min_value=METRIC_VALUE_MIN,
        max_value=METRIC_VALUE_MAX,
    )
    validator.expect_column_mean_to_be_between(
        ANOMALY_COLUMN,
        min_value=METRIC_MEAN_MIN,
        max_value=METRIC_MEAN_MAX,
    )


def detect_zscore_outliers(
    dataframe: pd.DataFrame,
    column: str = ANOMALY_COLUMN,
    z_threshold: float = Z_SCORE_THRESHOLD,
    id_column: str = "event_id",
) -> AnomalyCheckResult:
    """Flag rows whose metric values exceed a z-score threshold."""
    series = dataframe[column].astype(float)
    mean = series.mean()
    std = series.std(ddof=0)

    if std == 0 or np.isnan(std):
        return AnomalyCheckResult(
            passed=True,
            outlier_count=0,
            outlier_event_ids=[],
            z_score_threshold=z_threshold,
        )

    z_scores = (series - mean).abs() / std
    outlier_mask = z_scores > z_threshold
    outlier_ids = dataframe.loc[outlier_mask, id_column].astype(int).tolist()

    return AnomalyCheckResult(
        passed=len(outlier_ids) == 0,
        outlier_count=len(outlier_ids),
        outlier_event_ids=outlier_ids,
        z_score_threshold=z_threshold,
    )
