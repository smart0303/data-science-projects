"""Retry-enabled weather ETL: fetch Open-Meteo forecast, wait on dependencies, save CSV."""

from __future__ import annotations

import csv
import os
from datetime import datetime, timedelta
from pathlib import Path

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.sensors.filesystem import FileSensor

API_URL = "https://api.open-meteo.com/v1/forecast"
API_PARAMS = {
    "latitude": 34.05,
    "longitude": -118.24,
    "hourly": "temperature_2m",
}

DATA_DIR = Path(__file__).resolve().parent.parent / "data"
READY_MARKER = DATA_DIR / ".pipeline_ready"

# Step 1: DAG-wide retry defaults (tasks may override)
default_args = {
    "owner": "data-science",
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_etl",
    default_args=default_args,
    description="Fetch LA hourly temperature forecast with retries and dependency wait",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "etl", "retry"],
) as dag:
    start = EmptyOperator(task_id="start")

    @task
    def validate_api_config() -> dict:
        """Validate API parameters and mark the pipeline ready for downstream work."""
        if not (-90 <= API_PARAMS["latitude"] <= 90):
            raise ValueError("latitude must be between -90 and 90")
        if not (-180 <= API_PARAMS["longitude"] <= 180):
            raise ValueError("longitude must be between -180 and 180")
        if not API_PARAMS.get("hourly"):
            raise ValueError("hourly parameter is required")

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        READY_MARKER.write_text(
            f"ready_at={datetime.utcnow().isoformat()}Z\n",
            encoding="utf-8",
        )
        return dict(API_PARAMS)

    # Step 3: wait until the readiness marker exists (depends on validate_api_config)
    wait_for_dependencies = FileSensor(
        task_id="wait_for_dependencies",
        filepath=str(READY_MARKER),
        poke_interval=5,
        timeout=120,
        mode="poke",
    )

    @task(
        retries=5,
        retry_delay=timedelta(seconds=10),
        retry_exponential_backoff=True,
    )
    def fetch_weather(config: dict) -> dict:
        """Fetch forecast from Open-Meteo; task-level retries recover transient failures."""
        context = get_current_context()
        attempt = context["ti"].try_number
        fail_until = int(os.environ.get("WEATHER_API_FAIL_ATTEMPTS", "0"))

        if attempt <= fail_until:
            raise requests.RequestException(
                f"Simulated API failure on attempt {attempt} "
                f"(WEATHER_API_FAIL_ATTEMPTS={fail_until})"
            )

        response = requests.get(API_URL, params=config, timeout=30)
        response.raise_for_status()
        payload = response.json()

        hourly = payload.get("hourly", {})
        times = hourly.get("time", [])
        temps = hourly.get("temperature_2m", [])
        if not times or not temps:
            raise ValueError("API response missing hourly temperature data")

        return {
            "latitude": payload.get("latitude", config["latitude"]),
            "longitude": payload.get("longitude", config["longitude"]),
            "hourly_time": times,
            "temperature_2m": temps,
        }

    @task
    def process_weather(raw: dict) -> list[dict]:
        """Flatten hourly API payload into row-oriented records."""
        rows = []
        for time_str, temp in zip(raw["hourly_time"], raw["temperature_2m"], strict=True):
            rows.append(
                {
                    "time": time_str,
                    "temperature_2m": temp,
                    "latitude": raw["latitude"],
                    "longitude": raw["longitude"],
                }
            )
        return rows

    @task
    def save_to_csv(rows: list[dict]) -> str:
        """Step 4: persist processed weather rows to a dated CSV."""
        context = get_current_context()
        run_date = context["ds"]

        DATA_DIR.mkdir(parents=True, exist_ok=True)
        filepath = DATA_DIR / f"weather_{run_date}.csv"

        if not rows:
            raise ValueError("No weather rows to save")

        fieldnames = list(rows[0].keys())
        with filepath.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

        return str(filepath)

    @task
    def print_summary(rows: list[dict], csv_path: str) -> None:
        temps = [r["temperature_2m"] for r in rows if r.get("temperature_2m") is not None]

        print("=== Weather ETL Summary (Los Angeles) ===")
        print(f"  rows_saved: {len(rows)}")
        print(f"  csv_path: {csv_path}")
        print(f"  latitude: {rows[0].get('latitude')}")
        print(f"  longitude: {rows[0].get('longitude')}")
        if temps:
            print(f"  avg_temp_c: {sum(temps) / len(temps):.1f}")
            print(f"  min_temp_c: {min(temps):.1f}")
            print(f"  max_temp_c: {max(temps):.1f}")
        print("=== First 5 hourly readings ===")
        for row in rows[:5]:
            print(f"  {row['time']}: {row['temperature_2m']} °C")

    end = EmptyOperator(task_id="end")

    # Step 2: explicit dependency chain
    config = validate_api_config()
    weather_raw = fetch_weather(config)
    weather_rows = process_weather(weather_raw)
    csv_path = save_to_csv(weather_rows)
    summary = print_summary(weather_rows, csv_path)

    start >> config >> wait_for_dependencies >> weather_raw >> weather_rows >> csv_path >> summary >> end
