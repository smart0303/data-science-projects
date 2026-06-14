# Topic 05: Run the Kafka consumer
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-05-simple-consumer
#
# Usage:
#   Terminal 1: .\run-consumer.ps1
#   Terminal 2: run producer from topic-04 (see consumer-notes.md)

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$ProjectDir = $PSScriptRoot

Write-Host "`n=== Topic 05: Simple Kafka Consumer ===" -ForegroundColor Cyan

Write-Host "`n--- Ensure Kafka is running ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
Pop-Location

Write-Host "`n--- Setup Python environment ---" -ForegroundColor Yellow
Push-Location $ProjectDir
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}
& .\.venv\Scripts\Activate.ps1
pip install -q -r requirements.txt

Write-Host "`n--- Start consumer (screenshot received messages here) ---" -ForegroundColor Cyan
Write-Host "In another terminal, run the Topic 04 producer to publish messages." -ForegroundColor Yellow
Write-Host "Press Ctrl+C to stop the consumer.`n" -ForegroundColor Yellow
python consumer.py

Pop-Location
