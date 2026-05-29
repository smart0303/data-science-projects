# Retry-Enabled Weather ETL DAG

Apache Airflow pipeline that fetches hourly temperature forecasts for Los Angeles from [Open-Meteo](https://open-meteo.com/), uses automatic retries on failure, waits for dependency tasks via a file sensor, and saves results to CSV.

## DAG: `weather_etl`

| Step | Task | Description |
|------|------|-------------|
| 1 | `default_args` | DAG-wide `retries=3`, exponential backoff; `fetch_weather` overrides with `retries=5` |
| 2 | Chain | `start` → `validate_api_config` → `wait_for_dependencies` → `fetch_weather` → `process_weather` → `save_to_csv` → `print_summary` → `end` |
| 3 | `wait_for_dependencies` | Polls every 5s until `validate_api_config` writes `data/.pipeline_ready` |
| 4 | `save_to_csv` | Writes `data/weather_YYYY-MM-DD.csv` |
| 5 | `scripts/test_retry.ps1` | Sets `WEATHER_API_FAIL_ATTEMPTS=2` to simulate transient API errors |

### API

```python
url = "https://api.open-meteo.com/v1/forecast"
params = {
    "latitude": 34.05,
    "longitude": -118.24,
    "hourly": "temperature_2m",
}
```

Schedule: `@daily`

## Setup (Step 1)

```powershell
cd retry-enabled-weather-ETL-DAG
.\scripts\setup.ps1
```

## Run the DAG

```powershell
.\scripts\run_dag.ps1
```

## Test retry behavior (Step 5)

```powershell
.\scripts\test_retry.ps1
```

The `fetch_weather` task raises a simulated `requests.RequestException` on the first N attempts (`ti.try_number`), then calls the real API. With `WEATHER_API_FAIL_ATTEMPTS=2`, attempts 1–2 fail and attempt 3 succeeds.

## Optional: Web UI

```powershell
$env:AIRFLOW_HOME = (Get-Location).Path
$env:AIRFLOW__CORE__DAGS_FOLDER = "$env:AIRFLOW_HOME\dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
.\.venv\Scripts\Activate.ps1
airflow standalone
```

Open http://localhost:8080 and trigger `weather_etl`.
