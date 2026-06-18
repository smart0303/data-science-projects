# Setup script — Kafka Producer & Consumer System
# Run from project root

$ErrorActionPreference = "Stop"
$ProjectDir = $PSScriptRoot | Split-Path -Parent

Write-Host "`n=== Kafka Project Setup ===" -ForegroundColor Cyan

Write-Host "`n--- 1. Check Docker ---" -ForegroundColor Yellow
docker --version
docker compose version

Write-Host "`n--- 2. Start Kafka ---" -ForegroundColor Yellow
Push-Location $ProjectDir
docker compose up -d

Write-Host "`n--- 3. Wait for healthy broker ---" -ForegroundColor Yellow
$maxAttempts = 12
for ($i = 1; $i -le $maxAttempts; $i++) {
    $status = docker inspect --format='{{.State.Health.Status}}' kafka 2>$null
    if ($status -eq "healthy") {
        Write-Host "Kafka is healthy." -ForegroundColor Green
        break
    }
    Write-Host "  Attempt $i/$maxAttempts - status: $status"
    Start-Sleep -Seconds 5
}

Write-Host "`n--- 4. Create topics ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n--- 5. Python environment ---" -ForegroundColor Yellow
if (-not (Test-Path "$ProjectDir\.venv")) {
    py -3.12 -m venv "$ProjectDir\.venv"
}
& "$ProjectDir\.venv\Scripts\pip.exe" install -q -r "$ProjectDir\requirements.txt"

Pop-Location

Write-Host "`nSetup complete!" -ForegroundColor Green
Write-Host "  Activate: .\.venv\Scripts\Activate.ps1" -ForegroundColor Cyan
Write-Host "  Consumer: python consumer.py" -ForegroundColor Cyan
Write-Host "  Producer: python producer.py" -ForegroundColor Cyan
