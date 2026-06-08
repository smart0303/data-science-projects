"""Generate validation reports from checkpoint results and custom checks."""

from __future__ import annotations

import json
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from config import REPORTS_DIR


def _expectation_rows(validation_results: list[Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for validation_result in validation_results:
        for expectation_result in validation_result.results:
            rows.append(
                {
                    "expectation_type": expectation_result.expectation_config.type,
                    "success": expectation_result.success,
                    "kwargs": expectation_result.expectation_config.kwargs,
                }
            )
    return rows


def build_report_payload(
    checkpoint_success: bool,
    validation_results: list[Any],
    custom_checks: dict[str, Any],
) -> dict[str, Any]:
    """Assemble a structured report payload."""
    stats = validation_results[0].statistics if validation_results else {}
    custom_passed = all(check.get("passed", False) for check in custom_checks.values())

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "overall_success": checkpoint_success and custom_passed,
        "great_expectations": {
            "success": checkpoint_success,
            "evaluated_expectations": stats.get("evaluated_expectations", 0),
            "successful_expectations": stats.get("successful_expectations", 0),
            "failed_expectations": stats.get("unsuccessful_expectations", 0),
            "expectations": _expectation_rows(validation_results),
        },
        "custom_checks": custom_checks,
    }


def write_json_report(payload: dict[str, Any], output_dir: Path = REPORTS_DIR) -> Path:
    """Write a JSON validation report."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = output_dir / f"validation_report_{timestamp}.json"
    report_path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    return report_path


def write_html_report(payload: dict[str, Any], output_dir: Path = REPORTS_DIR) -> Path:
    """Write a simple HTML validation summary."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    report_path = output_dir / f"validation_report_{timestamp}.html"

    gx = payload["great_expectations"]
    custom = payload["custom_checks"]
    status = "PASS" if payload["overall_success"] else "FAIL"
    status_color = "#1a7f37" if payload["overall_success"] else "#cf222e"

    expectation_rows = "".join(
        f"<tr><td>{row['expectation_type']}</td>"
        f"<td>{'PASS' if row['success'] else 'FAIL'}</td></tr>"
        for row in gx["expectations"]
    )

    custom_rows = "".join(
        f"<tr><td>{name}</td><td>{'PASS' if check['passed'] else 'FAIL'}</td>"
        f"<td>{check.get('message', '')}</td></tr>"
        for name, check in custom.items()
    )

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Validation Report</title>
  <style>
    body {{ font-family: system-ui, sans-serif; margin: 2rem; color: #1f2328; }}
    h1 {{ color: {status_color}; }}
    table {{ border-collapse: collapse; width: 100%; margin-bottom: 1.5rem; }}
    th, td {{ border: 1px solid #d0d7de; padding: 0.5rem 0.75rem; text-align: left; }}
    th {{ background: #f6f8fa; }}
  </style>
</head>
<body>
  <h1>Validation {status}</h1>
  <p>Generated at {payload['generated_at']}</p>
  <h2>Great Expectations</h2>
  <p>
    Evaluated: {gx['evaluated_expectations']} |
    Passed: {gx['successful_expectations']} |
    Failed: {gx['failed_expectations']}
  </p>
  <table>
    <thead><tr><th>Expectation</th><th>Status</th></tr></thead>
    <tbody>{expectation_rows}</tbody>
  </table>
  <h2>Custom Checks</h2>
  <table>
    <thead><tr><th>Check</th><th>Status</th><th>Details</th></tr></thead>
    <tbody>{custom_rows}</tbody>
  </table>
</body>
</html>
"""
    report_path.write_text(html, encoding="utf-8")
    return report_path


def generate_validation_reports(
    checkpoint_success: bool,
    validation_results: list[Any],
    custom_checks: dict[str, Any],
    output_dir: Path = REPORTS_DIR,
) -> tuple[Path, Path]:
    """Write JSON and HTML validation reports."""
    payload = build_report_payload(checkpoint_success, validation_results, custom_checks)
    json_path = write_json_report(payload, output_dir)
    html_path = write_html_report(payload, output_dir)
    return json_path, html_path


def custom_check_dict(check: Any, message: str | None = None) -> dict[str, Any]:
    """Convert a dataclass check result into a report-friendly dict."""
    data = asdict(check)
    if message is not None:
        data["message"] = message
    elif hasattr(check, "message"):
        data["message"] = check.message
    return data
