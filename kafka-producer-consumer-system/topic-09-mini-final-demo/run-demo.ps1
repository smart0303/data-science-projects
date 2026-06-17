# Topic 09: User Signup Event Pipeline — Final Demo
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-09-mini-final-demo

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$ProjectDir = $PSScriptRoot

Write-Host "`n========================================" -ForegroundColor Cyan
Write-Host "  TOPIC 09: USER SIGNUP EVENT PIPELINE" -ForegroundColor Cyan
Write-Host "========================================" -ForegroundColor Cyan

Write-Host "`n--- Step 1: Start Kafka ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
docker compose ps
Pop-Location

Write-Host "`n--- Step 2: Ensure user-events topic exists ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null

Write-Host "`n--- Step 3: Reset welcome-email consumer group ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group welcome-email-service --delete 2>$null
Write-Host "Consumer group reset." -ForegroundColor Green

Write-Host "`n--- Step 4: Setup Python environment ---" -ForegroundColor Yellow
Push-Location $ProjectDir
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}
& .\.venv\Scripts\pip.exe install -q -r requirements.txt
Pop-Location
Write-Host "Environment ready." -ForegroundColor Green

Write-Host @"

--- Step 5: RUN THE DEMO (two terminals) ---

TERMINAL 1 — Welcome Email Service (start FIRST):
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python signup_consumer.py

TERMINAL 2 — Signup Service (register 5 users):
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python signup_producer.py

Watch Terminal 1 for user info + welcome emails.
Screenshot both terminals for deliverables.

Screenshots folder: screenshots\
  01-consumer-waiting.png
  02-producer-signups.png
  03-consumer-emails.png
  04-pipeline-overview.png

Full guide: demo-guide.md

"@ -ForegroundColor Cyan
