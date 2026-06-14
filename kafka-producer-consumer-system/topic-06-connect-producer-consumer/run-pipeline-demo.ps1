# Topic 06: End-to-end Kafka pipeline demo
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-06-connect-producer-consumer

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$Topic03Dir = "d:\Work\data-science-projects\kafka-producer-consumer-system\topic-03-create-kafka-topics"
$ProducerDir = "d:\Work\data-science-projects\kafka-producer-consumer-system\topic-04-simple-producer"
$ConsumerDir = "d:\Work\data-science-projects\kafka-producer-consumer-system\topic-05-simple-consumer"

Write-Host "`n=== Topic 06: Connect Producer and Consumer ===" -ForegroundColor Cyan

Write-Host "`n--- Step 1: Start Kafka ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
docker compose ps
Pop-Location

Write-Host "`n--- Wait for healthy broker ---" -ForegroundColor Yellow
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

Write-Host "`n--- Step 2: Ensure user-events topic exists ---" -ForegroundColor Yellow
Push-Location $Topic03Dir
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
Pop-Location

Write-Host "`n--- Step 3: Reset consumer group (fresh demo) ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group user-events-consumer-group --delete 2>$null
Write-Host "Consumer group reset (or did not exist yet)." -ForegroundColor Green

Write-Host "`n--- Step 4: Setup Python environments ---" -ForegroundColor Yellow
foreach ($dir in @($ProducerDir, $ConsumerDir)) {
    Push-Location $dir
    if (-not (Test-Path ".venv")) {
        py -3.12 -m venv .venv
    }
    & .\.venv\Scripts\pip.exe install -q -r requirements.txt
    Pop-Location
}
Write-Host "Producer and consumer environments ready." -ForegroundColor Green

Write-Host @"

=== PIPELINE DEMO — use two terminals ===

TERMINAL 1 (Consumer — start first):
  cd $ConsumerDir
  .\.venv\Scripts\Activate.ps1
  python consumer.py

TERMINAL 2 (Producer — send messages):
  cd $ProducerDir
  .\.venv\Scripts\Activate.ps1
  python producer.py

Run the producer multiple times to send more batches (11 messages each run).
Verify Terminal 1 shows 11 'Received:' lines per producer run.

Screenshot both terminals for the deliverable.
Save to: topic-06-connect-producer-consumer\screenshots\pipeline-demo.png

Press Ctrl+C in Terminal 1 to stop the consumer, then test stop/restart behavior.

Full docs: pipeline-demo.md | Architecture: pipeline-architecture.md

"@ -ForegroundColor Cyan
