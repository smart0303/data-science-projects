"""Run the ingestion data validation pipeline."""

from __future__ import annotations

import sys
from pathlib import Path

import great_expectations as gx
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_PATH = PROJECT_ROOT / "data" / "ingestion_orders.csv"
GX_ROOT = PROJECT_ROOT / "gx"


def get_context() -> gx.AbstractDataContext:
    if not GX_ROOT.exists():
        from setup_suite import create_validation_suite

        return create_validation_suite()
    return gx.get_context(project_root_dir=str(PROJECT_ROOT))


def run_validation() -> bool:
    """Step 5: validate ingestion data against the configured checkpoint."""
    context = get_context()
    dataframe = pd.read_csv(DATA_PATH)

    checkpoint = context.checkpoints.get("ingestion_checkpoint")
    result = checkpoint.run(batch_parameters={"dataframe": dataframe})

    validation_results = list(result.run_results.values())
    stats = validation_results[0].statistics if validation_results else {}

    print(f"Validation success: {result.success}")
    print(f"Evaluated expectations: {stats.get('evaluated_expectations', 0)}")
    print(f"Successful expectations: {stats.get('successful_expectations', 0)}")
    print(f"Failed expectations: {stats.get('unsuccessful_expectations', 0)}")

    for validation_result in validation_results:
        for expectation_result in validation_result.results:
            status = "PASS" if expectation_result.success else "FAIL"
            expectation_type = expectation_result.expectation_config.type
            print(f"  [{status}] {expectation_type}")

    return result.success


if __name__ == "__main__":
    success = run_validation()
    sys.exit(0 if success else 1)
