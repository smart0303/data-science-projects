$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot\..

if (-not (Test-Path ".venv")) {
    Write-Error "Virtual environment not found. Run scripts/setup.ps1 first."
}

.\.venv\Scripts\Activate.ps1

Write-Host "Running Spark medallion pipeline..."
python pipeline.py

Write-Host ""
Write-Host "Running dbt models..."
Push-Location dbt
$env:DBT_PROFILES_DIR = (Get-Location).Path
dbt run
dbt test
Pop-Location

Write-Host ""
Write-Host "Full pipeline complete."
