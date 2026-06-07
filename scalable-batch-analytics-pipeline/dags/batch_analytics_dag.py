"""Airflow orchestration for the scalable batch analytics pipeline."""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.utils.task_group import TaskGroup

PROJECT_ROOT = Path(__file__).resolve().parent.parent
PYTHON = sys.executable
METRICS_DIR = PROJECT_ROOT / "logs" / "airflow_metrics"
DBT_DIR = PROJECT_ROOT / "dbt"

logger = logging.getLogger(__name__)


def _record_metric(event: str, payload: dict) -> None:
    METRICS_DIR.mkdir(parents=True, exist_ok=True)
    record = {
        "event": event,
        "recorded_at": datetime.utcnow().isoformat() + "Z",
        **payload,
    }
    with (METRICS_DIR / "dag_runs.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record) + "\n")


def _on_failure_callback(context: dict) -> None:
    ti = context["task_instance"]
    logger.error("Task failed: %s (dag=%s)", ti.task_id, ti.dag_id)
    _record_metric(
        "task_failure",
        {"dag_id": ti.dag_id, "task_id": ti.task_id, "try_number": ti.try_number},
    )


def _run_python_script(script: str, *args: str) -> dict:
    cmd = [PYTHON, script, *args]
    result = subprocess.run(
        cmd,
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"Command failed ({result.returncode}): {' '.join(cmd)}\n"
            f"stdout: {result.stdout}\nstderr: {result.stderr}"
        )
    return {"stdout": result.stdout.strip()}


def _run_dbt(command: str) -> dict:
    env = {**os.environ, "DBT_PROFILES_DIR": str(DBT_DIR)}
    result = subprocess.run(
        ["dbt", command],
        cwd=str(DBT_DIR),
        capture_output=True,
        text=True,
        check=False,
        env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(f"dbt {command} failed:\n{result.stdout}\n{result.stderr}")
    return {"stdout": result.stdout.strip()}


default_args = {
    "owner": "data-science",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "on_failure_callback": _on_failure_callback,
}


with DAG(
    dag_id="batch_analytics_pipeline",
    default_args=default_args,
    description="Orchestrate Spark medallion pipeline, validation, and dbt modeling",
    schedule="0 7 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["batch", "spark", "dbt", "medallion"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def pipeline_health_check() -> dict:
        context = get_current_context()
        batch_files = list((PROJECT_ROOT / "data").glob("orders_batch*.csv"))
        if not batch_files:
            raise FileNotFoundError("No batch files found in data/")
        METRICS_DIR.mkdir(parents=True, exist_ok=True)
        return {"status": "healthy", "run_date": context["ds"], "batch_count": len(batch_files)}

    @task
    def validate_raw() -> dict:
        return _run_python_script("validate.py", "--layer", "raw")

    @task
    def run_spark_pipeline() -> dict:
        return _run_python_script("pipeline.py", "--skip-raw-validation")

    @task
    def validate_silver() -> dict:
        return _run_python_script("validate.py", "--layer", "silver")

    @task
    def validate_gold() -> dict:
        return _run_python_script("validate.py", "--layer", "gold")

    with TaskGroup(group_id="dbt") as dbt_group:
        @task(task_id="run_models")
        def dbt_run() -> dict:
            return _run_dbt("run")

        @task(task_id="run_tests")
        def dbt_test() -> dict:
            return _run_dbt("test")

        dbt_run() >> dbt_test()

    @task
    def collect_pipeline_metrics(health_info: dict, spark_result: dict) -> dict:
        context = get_current_context()
        payload = {
            "run_date": context["ds"],
            "health": health_info,
            "spark_stdout_lines": len(spark_result.get("stdout", "").splitlines()),
        }
        _record_metric("pipeline_complete", payload)
        return payload

    end = EmptyOperator(task_id="end")

    health = pipeline_health_check()
    raw_check = validate_raw()
    spark = run_spark_pipeline()
    silver_check = validate_silver()
    gold_check = validate_gold()
    metrics = collect_pipeline_metrics(health, spark)

    start >> health >> raw_check >> spark >> silver_check >> gold_check >> dbt_group >> metrics >> end
