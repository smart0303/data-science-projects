$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

if (-not (Test-Path ".venv")) {
    Write-Error "Virtual environment not found. Run scripts/setup.ps1 first."
}

.\.venv\Scripts\Activate.ps1
$env:AIRFLOW_HOME = (Get-Location).Path

Write-Host "Testing Airflow DAG (batch_analytics_pipeline)..."
airflow dags test batch_analytics_pipeline
