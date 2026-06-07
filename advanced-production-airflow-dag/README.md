# Advanced Production Airflow DAG

Production-style Apache Airflow pipeline that fetches hourly temperature forecasts for multiple cities, with dynamic task generation, retries, SLA monitoring, task groups, and a cron schedule.

## DAG: `production_weather_etl`

| Step | Feature | Implementation |
|------|---------|----------------|
| 1 | Dynamic DAG | Tasks generated from `REGIONS` config (LA, NYC, Chicago) |
| 2 | Retries & SLA | `default_args` with `retries=3`, exponential backoff, per-task SLA, failure/SLA callbacks |
| 3 | Dependency chains | `start` → health → extract → transform → quality → metrics → load → complete → `end` |
| 4 | Task groups | `extract`, `transform`, `quality`, `load` groups with one task per region |
| 5 | Schedule | `0 6 * * *` (daily at 06:00 UTC) |

### Pipeline flow

```
start
  └─ pipeline_health_check
       └─ [extract] fetch_{region}
            └─ [transform] transform_{region}
                 └─ [quality] validate_{region}
                      └─ collect_pipeline_metrics
                           └─ [load] save_{region}
                                └─ pipeline_complete
                                     └─ end
```

### Monitoring

- `on_failure_callback` appends to `data/metrics/failure_events.jsonl`
- `sla_miss_callback` appends to `data/metrics/sla_events.jsonl`
- `collect_pipeline_metrics` writes `data/metrics/run_metrics_YYYY-MM-DD.json`

### Output

Per-region CSV files: `data/weather_{region_id}_YYYY-MM-DD.csv`

## Setup

```powershell
cd advanced-production-airflow-dag
.\scripts\setup.ps1
```

## Run the DAG (test mode)

```powershell
.\scripts\run_dag.ps1
```

## Optional: Web UI

```powershell
$env:AIRFLOW_HOME = (Get-Location).Path
$env:AIRFLOW__CORE__DAGS_FOLDER = "$env:AIRFLOW_HOME\dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
.\.venv\Scripts\Activate.ps1
airflow standalone
```

Open http://localhost:8080 and trigger `production_weather_etl`.
