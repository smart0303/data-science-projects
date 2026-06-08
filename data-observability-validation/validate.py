"""Run observability validation and generate reports."""

from __future__ import annotations

import sys

import great_expectations as gx
import pandas as pd

from checks.anomalies import detect_zscore_outliers
from checks.duplicates import detect_composite_duplicates
from checks.freshness import assess_freshness
from config import CHECKPOINT_NAME, DATA_PATH, PROJECT_ROOT
from reporting import custom_check_dict, generate_validation_reports

GX_ROOT = PROJECT_ROOT / "gx"


def get_context() -> gx.AbstractDataContext:
    if not GX_ROOT.exists():
        from setup_suite import create_validation_suite

        return create_validation_suite()
    return gx.get_context(project_root_dir=str(PROJECT_ROOT))


def run_custom_checks(dataframe: pd.DataFrame) -> dict[str, dict]:
    """Run duplicate, freshness, and anomaly checks outside Great Expectations."""
    freshness = assess_freshness(dataframe)
    duplicates = detect_composite_duplicates(dataframe)
    anomalies = detect_zscore_outliers(dataframe)

    return {
        "freshness": custom_check_dict(freshness),
        "composite_duplicates": {
            **custom_check_dict(duplicates),
            "message": (
                "No composite-key duplicates found."
                if duplicates.passed
                else (
                    f"Found {duplicates.duplicate_row_count} rows across "
                    f"{duplicates.duplicate_groups} duplicate groups."
                )
            ),
        },
        "zscore_anomalies": {
            **custom_check_dict(anomalies),
            "message": (
                "No z-score outliers detected."
                if anomalies.passed
                else f"Outlier event_ids: {anomalies.outlier_event_ids}"
            ),
        },
    }


def run_validation() -> bool:
    """Validate sensor events and write JSON/HTML reports."""
    context = get_context()
    dataframe = pd.read_csv(DATA_PATH)

    checkpoint = context.checkpoints.get(CHECKPOINT_NAME)
    result = checkpoint.run(batch_parameters={"dataframe": dataframe})
    validation_results = list(result.run_results.values())
    stats = validation_results[0].statistics if validation_results else {}

    custom_checks = run_custom_checks(dataframe)
    custom_passed = all(check["passed"] for check in custom_checks.values())
    overall_success = result.success and custom_passed

    json_path, html_path = generate_validation_reports(
        checkpoint_success=result.success,
        validation_results=validation_results,
        custom_checks=custom_checks,
    )

    print(f"Validation success: {overall_success}")
    print(f"Great Expectations success: {result.success}")
    print(f"Evaluated expectations: {stats.get('evaluated_expectations', 0)}")
    print(f"Successful expectations: {stats.get('successful_expectations', 0)}")
    print(f"Failed expectations: {stats.get('unsuccessful_expectations', 0)}")

    for validation_result in validation_results:
        for expectation_result in validation_result.results:
            status = "PASS" if expectation_result.success else "FAIL"
            print(f"  [{status}] {expectation_result.expectation_config.type}")

    for name, check in custom_checks.items():
        status = "PASS" if check["passed"] else "FAIL"
        print(f"  [{status}] {name}: {check.get('message', '')}")

    print(f"JSON report: {json_path}")
    print(f"HTML report: {html_path}")

    return overall_success


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)
