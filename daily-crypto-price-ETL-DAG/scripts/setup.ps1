# Setup Airflow for daily-crypto-price-ETL-DAG (Windows PowerShell)
$ErrorActionPreference = "Stop"

$ProjectRoot = Split-Path -Parent $PSScriptRoot
Set-Location $ProjectRoot

$env:AIRFLOW_HOME = $ProjectRoot
$env:AIRFLOW__CORE__DAGS_FOLDER = Join-Path $ProjectRoot "dags"
$env:AIRFLOW__CORE__LOAD_EXAMPLES = "False"
$env:AIRFLOW__CORE__EXECUTOR = "SequentialExecutor"
$env:AIRFLOW__DATABASE__SQL_ALCHEMY_CONN = "sqlite:///$($ProjectRoot -replace '\\', '/')/airflow.db"

Write-Host "AIRFLOW_HOME=$env:AIRFLOW_HOME"

if (-not (Test-Path ".venv")) {
    python -m venv .venv
}

& ".venv\Scripts\Activate.ps1"
pip install --upgrade pip
pip install -r requirements.txt

airflow db migrate
Write-Host "Airflow initialized. Run: .\scripts\run_dag.ps1"
