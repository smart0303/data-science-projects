# Topic 07: JSON events demo
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-07-json-messages

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$ProjectDir = $PSScriptRoot

Write-Host "`n=== Topic 07: JSON Events ===" -ForegroundColor Cyan

Write-Host "`n--- Start Kafka ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
Pop-Location

Write-Host "`n--- Ensure topics exist ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n--- Reset JSON consumer group ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group json-events-consumer-group --delete 2>$null

Write-Host "`n--- Setup Python environment ---" -ForegroundColor Yellow
Push-Location $ProjectDir
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}
& .\.venv\Scripts\pip.exe install -q -r requirements.txt
Pop-Location

Write-Host @"

=== JSON DEMO — use two terminals ===

TERMINAL 1 (Consumer):
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python json_consumer.py

TERMINAL 2 (Producer):
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python json_producer.py

Events sent: user_signup, user_login (user-events), order_created (order-events).
Screenshot formatted consumer output -> screenshots\json-pipeline.png

"@ -ForegroundColor Cyan
