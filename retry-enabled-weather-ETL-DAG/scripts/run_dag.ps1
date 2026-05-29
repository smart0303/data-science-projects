# Run the weather_etl DAG once (test mode)
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$env:AIRFLOW_HOME = $ProjectRoot
$env:AIRFLOW__CORE__DAGS_FOLDER = Join-Path $ProjectRoot "dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW__CORE__EXECUTOR = "SequentialExecutor"
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:///$($ProjectRoot -replace '\\', '/')/airflow.db"

if ($env:WEATHER_API_FAIL_ATTEMPTS) {
    Remove-Item Env:WEATHER_API_FAIL_ATTEMPTS
}

& ".venv\Scripts\Activate.ps1"

$RunDate = (Get-Date).ToString("yyyy-MM-dd")
Write-Host "Running DAG weather_etl for execution date $RunDate ..."

airflow dags list-import-errors
airflow dags test weather_etl $RunDate
