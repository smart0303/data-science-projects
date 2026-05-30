"""Simple ETL pipeline: extract CSV sales, transform aggregates, load to output."""

from __future__ import annotations

import csv
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.environ.get("DATA_DIR", BASE_DIR / "data"))
OUTPUT_DIR = Path(os.environ.get("OUTPUT_DIR", BASE_DIR / "output"))


def extract(input_path: Path) -> list[dict[str, str]]:
    """Read raw sales records from CSV."""
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    with input_path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def transform(rows: list[dict[str, str]]) -> tuple[list[dict], list[dict], list[dict]]:
    """Clean rows and build category-level and daily summaries."""
    cleaned: list[dict] = []
    by_category: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"order_count": 0, "units_sold": 0, "revenue": 0.0}
    )
    by_date: dict[str, dict[str, float | int]] = defaultdict(
        lambda: {"order_count": 0, "units_sold": 0, "revenue": 0.0}
    )

    for row in rows:
        quantity = int(row["quantity"])
        unit_price = float(row["unit_price"])
        revenue = round(quantity * unit_price, 2)
        category = row["category"].strip()
        order_date = row["order_date"].strip()

        record = {
            "order_id": int(row["order_id"]),
            "product": row["product"].strip(),
            "category": category,
            "quantity": quantity,
            "unit_price": unit_price,
            "revenue": revenue,
            "order_date": order_date,
        }
        cleaned.append(record)

        by_category[category]["order_count"] += 1
        by_category[category]["units_sold"] += quantity
        by_category[category]["revenue"] = round(
            by_category[category]["revenue"] + revenue, 2
        )

        by_date[order_date]["order_count"] += 1
        by_date[order_date]["units_sold"] += quantity
        by_date[order_date]["revenue"] = round(
            by_date[order_date]["revenue"] + revenue, 2
        )

    category_summary = [
        {"category": category, **metrics}
        for category, metrics in sorted(by_category.items())
    ]
    daily_summary = [
        {"order_date": order_date, **metrics}
        for order_date, metrics in sorted(by_date.items())
    ]

    return cleaned, category_summary, daily_summary


def load(
    cleaned: list[dict],
    category_summary: list[dict],
    daily_summary: list[dict],
    output_dir: Path,
) -> None:
    """Write transformed datasets to CSV files."""
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    datasets = {
        f"sales_clean_{timestamp}.csv": cleaned,
        f"sales_by_category_{timestamp}.csv": category_summary,
        f"sales_by_day_{timestamp}.csv": daily_summary,
    }

    for filename, records in datasets.items():
        path = output_dir / filename
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=records[0].keys())
            writer.writeheader()
            writer.writerows(records)
        print(f"Wrote {len(records)} rows to {path}")


def run(
    input_file: str = "sales_raw.csv",
    data_dir: Path = DATA_DIR,
    output_dir: Path = OUTPUT_DIR,
) -> None:
    """Run the full extract-transform-load pipeline."""
    input_path = data_dir / input_file
    print(f"Starting ETL from {input_path}")

    rows = extract(input_path)
    print(f"Extracted {len(rows)} raw records")

    cleaned, category_summary, daily_summary = transform(rows)
    print(
        f"Transformed into {len(cleaned)} cleaned rows, "
        f"{len(category_summary)} categories, {len(daily_summary)} days"
    )

    load(cleaned, category_summary, daily_summary, output_dir)
    print("ETL pipeline completed successfully")


if __name__ == "__main__":
    input_name = sys.argv[1] if len(sys.argv) > 1 else "sales_raw.csv"
    run(input_file=input_name)
