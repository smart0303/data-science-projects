# Step 5: simulate API failures and verify Airflow task retries succeed
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$env:AIRFLOW_HOME = $ProjectRoot
$env:AIRFLOW__CORE__DAGS_FOLDER = Join-Path $ProjectRoot "dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW__CORE__EXECUTOR = "SequentialExecutor"
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:///$($ProjectRoot -replace '\\', '/')/airflow.db"

# Fail the first 2 attempts; fetch_weather has retries=5 so the run should still succeed
$env:WEATHER_API_FAIL_ATTEMPTS = "2"

& ".venv\Scripts\Activate.ps1"

$RunDate = (Get-Date).ToString("yyyy-MM-dd")
Write-Host "Testing retry behavior (WEATHER_API_FAIL_ATTEMPTS=2) for $RunDate ..."

airflow dags list-import-errors
airflow dags test weather_etl $RunDate

Write-Host ""
Write-Host "Retry test finished. Check task logs for simulated failures on attempts 1-2."
