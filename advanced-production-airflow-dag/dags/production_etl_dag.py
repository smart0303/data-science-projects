"""Advanced production ETL: dynamic tasks, retries, SLA, task groups, and monitoring."""

from __future__ import annotations

import csv
import json
import logging
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

API_URL = "https://api.open-meteo.com/v1/forecast"
DATA_DIR = Path(__file__).resolve().parent.parent / "data"
METRICS_DIR = DATA_DIR / "metrics"

# Step 1: config-driven regions — tasks are generated dynamically per entry
REGIONS: list[dict[str, Any]] = [
    {"region_id": "los_angeles", "latitude": 34.05, "longitude": -118.24},
    {"region_id": "new_york", "latitude": 40.71, "longitude": -74.00},
    {"region_id": "chicago", "latitude": 41.88, "longitude": -87.63},
]

logger = logging.getLogger(__name__)


def _sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis) -> None:
    """Step 2: monitoring hook when a task misses its SLA."""
    missed = [ti.task_id for ti in task_list]
    logger.warning("SLA miss on tasks: %s (dag=%s)", missed, dag.dag_id)
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "event": "sla_miss",
        "dag_id": dag.dag_id,
        "missed_tasks": missed,
        "recorded_at": datetime.utcnow().isoformat() + "Z",
    }
    (METRICS_DIR / "sla_events.jsonl").open("a", encoding="utf-8").write(
        json.dumps(payload) + "\n"
    )


def _on_failure_callback(context: dict) -> None:
    """Step 2: monitoring hook on task failure."""
    ti = context["task_instance"]
    logger.error(
        "Task failed: %s (dag=%s, run=%s, try=%s)",
        ti.task_id,
        ti.dag_id,
        context.get("run_id"),
        ti.try_number,
    )
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "event": "task_failure",
        "dag_id": ti.dag_id,
        "task_id": ti.task_id,
        "try_number": ti.try_number,
        "recorded_at": datetime.utcnow().isoformat() + "Z",
    }
    (METRICS_DIR / "failure_events.jsonl").open("a", encoding="utf-8").write(
        json.dumps(payload) + "\n"
    )


# Step 2: production defaults — retries, backoff, SLA, monitoring callbacks
default_args = {
    "owner": "data-science",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "sla": timedelta(minutes=30),
    "sla_miss_callback": _sla_miss_callback,
    "on_failure_callback": _on_failure_callback,
}

with DAG(
    dag_id="production_weather_etl",
    default_args=default_args,
    description="Production ETL with dynamic regions, task groups, retries, and SLA",
    # Step 5: scheduled daily at 06:00 UTC
    schedule="0 6 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["production", "etl", "sla", "task-groups"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task(sla=timedelta(minutes=5))
    def pipeline_health_check() -> dict:
        """Verify API reachability and prepare output directories."""
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        METRICS_DIR.mkdir(parents=True, exist_ok=True)

        response = requests.get(API_URL, params={"latitude": 0, "longitude": 0}, timeout=15)
        response.raise_for_status()

        context = get_current_context()
        return {
            "status": "healthy",
            "run_date": context["ds"],
            "region_count": len(REGIONS),
        }

    health = pipeline_health_check()

    # Step 4: extract stage grouped per region (dynamic task generation)
    with TaskGroup(group_id="extract") as extract_group:
        extract_outputs: list[Any] = []

        for region in REGIONS:
            region_id = region["region_id"]

            @task(
                task_id=f"fetch_{region_id}",
                retries=4,
                sla=timedelta(minutes=15),
            )
            def fetch_region(health_result: dict, region_config: dict = region) -> dict:
                params = {
                    "latitude": region_config["latitude"],
                    "longitude": region_config["longitude"],
                    "hourly": "temperature_2m",
                }
                response = requests.get(API_URL, params=params, timeout=30)
                response.raise_for_status()
                payload = response.json()
                hourly = payload.get("hourly", {})

                return {
                    "region_id": region_config["region_id"],
                    "latitude": payload.get("latitude", region_config["latitude"]),
                    "longitude": payload.get("longitude", region_config["longitude"]),
                    "run_date": health_result["run_date"],
                    "hourly_time": hourly.get("time", []),
                    "temperature_2m": hourly.get("temperature_2m", []),
                }

            extract_outputs.append(fetch_region(health))

    # Step 4: transform stage
    with TaskGroup(group_id="transform") as transform_group:
        transformed_outputs: list[Any] = []

        for idx, region in enumerate(REGIONS):
            region_id = region["region_id"]

            @task(task_id=f"transform_{region_id}")
            def transform_region(raw: dict, region_config: dict = region) -> list[dict]:
                if raw["region_id"] != region_config["region_id"]:
                    raise ValueError(
                        f"Region mismatch: expected {region_config['region_id']}, "
                        f"got {raw['region_id']}"
                    )

                rows = []
                for time_str, temp in zip(
                    raw["hourly_time"], raw["temperature_2m"], strict=True
                ):
                    rows.append(
                        {
                            "region_id": raw["region_id"],
                            "time": time_str,
                            "temperature_2m": temp,
                            "latitude": raw["latitude"],
                            "longitude": raw["longitude"],
                            "run_date": raw["run_date"],
                        }
                    )
                return rows

            transformed_outputs.append(transform_region(extract_outputs[idx]))

    # Step 4: data-quality checks
    with TaskGroup(group_id="quality") as quality_group:
        quality_outputs: list[Any] = []

        for idx, region in enumerate(REGIONS):
            region_id = region["region_id"]

            @task(task_id=f"validate_{region_id}", sla=timedelta(minutes=10))
            def validate_region(rows: list[dict], region_config: dict = region) -> list[dict]:
                if not rows:
                    raise ValueError(f"No rows for region {region_config['region_id']}")

                temps = [r["temperature_2m"] for r in rows if r.get("temperature_2m") is not None]
                if len(temps) < 12:
                    raise ValueError(
                        f"Insufficient hourly readings for {region_config['region_id']}: "
                        f"{len(temps)}"
                    )

                if any(t < -60 or t > 60 for t in temps):
                    raise ValueError(
                        f"Temperature out of range for {region_config['region_id']}"
                    )

                return rows

            quality_outputs.append(validate_region(transformed_outputs[idx]))

    @task(sla=timedelta(minutes=5))
    def collect_pipeline_metrics(validated_batches: list[list[dict]]) -> dict:
        """Step 2: aggregate run metrics for monitoring dashboards."""
        context = get_current_context()
        run_date = context["ds"]

        per_region = {}
        total_rows = 0
        for batch in validated_batches:
            if not batch:
                continue
            region_id = batch[0]["region_id"]
            temps = [r["temperature_2m"] for r in batch if r.get("temperature_2m") is not None]
            per_region[region_id] = {
                "row_count": len(batch),
                "avg_temp_c": round(sum(temps) / len(temps), 2) if temps else None,
                "min_temp_c": min(temps) if temps else None,
                "max_temp_c": max(temps) if temps else None,
            }
            total_rows += len(batch)

        metrics = {
            "run_date": run_date,
            "regions_processed": len(per_region),
            "total_rows": total_rows,
            "per_region": per_region,
            "recorded_at": datetime.utcnow().isoformat() + "Z",
        }

        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        metrics_path = METRICS_DIR / f"run_metrics_{run_date}.json"
        metrics_path.write_text(json.dumps(metrics, indent=2), encoding="utf-8")

        print("=== Pipeline Metrics ===")
        print(json.dumps(metrics, indent=2))
        return metrics

    # Step 4: load stage
    with TaskGroup(group_id="load") as load_group:
        load_outputs: list[Any] = []

        for idx, region in enumerate(REGIONS):
            region_id = region["region_id"]

            @task(task_id=f"save_{region_id}")
            def save_region(rows: list[dict], region_config: dict = region) -> str:
                if not rows:
                    raise ValueError(f"No rows to save for {region_config['region_id']}")

                run_date = rows[0]["run_date"]
                DATA_DIR.mkdir(parents=True, exist_ok=True)
                filepath = DATA_DIR / f"weather_{region_config['region_id']}_{run_date}.csv"

                fieldnames = list(rows[0].keys())
                with filepath.open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)

                return str(filepath)

            load_outputs.append(save_region(quality_outputs[idx]))

    @task
    def pipeline_complete(metrics: dict, csv_paths: list[str]) -> None:
        """Final monitoring summary for the run."""
        print("=== Production ETL Complete ===")
        print(f"  run_date: {metrics['run_date']}")
        print(f"  regions: {metrics['regions_processed']}")
        print(f"  total_rows: {metrics['total_rows']}")
        print(f"  csv_files: {len(csv_paths)}")
        for path in csv_paths:
            print(f"    - {path}")

    end = EmptyOperator(task_id="end")

    # Step 3: explicit dependency chain across stages and task groups
    metrics = collect_pipeline_metrics(quality_outputs)
    completion = pipeline_complete(metrics, load_outputs)

    start >> health >> extract_group >> transform_group >> quality_group
    quality_group >> [metrics, load_group] >> completion >> end
