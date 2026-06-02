"""
Production DE portfolio DAG: ETL → dbt → tests → data quality checks.
"""

from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path("/opt/airflow")
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
}


def run_etl() -> None:
    """Execute the Python ETL pipeline (extract → transform → load)."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    result = subprocess.run(
        ["python", "-m", "etl.pipeline"],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"ETL failed with exit code {result.returncode}")


def run_quality_checks() -> None:
    """Run SQL-based data quality validations."""
    env = os.environ.copy()
    env["PYTHONPATH"] = str(PROJECT_ROOT)
    result = subprocess.run(
        ["python", "/opt/airflow/scripts/run_quality_checks.py"],
        cwd=str(PROJECT_ROOT),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout)
    if result.stderr:
        print(result.stderr)
    if result.returncode != 0:
        raise RuntimeError(f"Quality checks failed with exit code {result.returncode}")


with DAG(
    dag_id="production_de_portfolio",
    default_args=default_args,
    description="JSONPlaceholder ETL, dbt transforms, tests, and analytics QA",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["portfolio", "etl", "dbt", "postgres"],
) as dag:
    start = EmptyOperator(task_id="start")

    extract_load = PythonOperator(
        task_id="run_python_etl",
        python_callable=run_etl,
        retries=5,
        retry_delay=timedelta(seconds=30),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --profiles-dir .",
        retries=2,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test --profiles-dir .",
        retries=2,
    )

    quality_checks = PythonOperator(
        task_id="run_data_quality_checks",
        python_callable=run_quality_checks,
        retries=2,
    )

    end = EmptyOperator(task_id="end")

    start >> extract_load >> dbt_run >> dbt_test >> quality_checks >> end
