$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

Write-Host "Creating virtual environment..."
py -3.12 -m venv .venv
.\.venv\Scripts\Activate.ps1

Write-Host "Installing dependencies..."
pip install -r requirements.txt

Write-Host "Initializing Airflow..."
$env:AIRFLOW_HOME = (Get-Location).Path
airflow db migrate

Write-Host ""
Write-Host "Setup complete."
Write-Host "  Activate:  .\.venv\Scripts\Activate.ps1"
Write-Host "  Pipeline:  python pipeline.py"
Write-Host "  dbt:       cd dbt; `$env:DBT_PROFILES_DIR = (Get-Location).Path; dbt run"
Write-Host "  Airflow:   `$env:AIRFLOW_HOME = (Get-Location).Path; airflow dags test batch_analytics_pipeline"
