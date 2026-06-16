# Topic 08: Error handling demo
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-08-error-handling

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$ProjectDir = $PSScriptRoot

Write-Host "`n=== Topic 08: Error Handling ===" -ForegroundColor Cyan

Write-Host "`n--- Start Kafka ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
Pop-Location

Write-Host "`n--- Ensure topics exist ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic user-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic order-events --partitions 3 --replication-factor 1 --if-not-exists 2>$null

Write-Host "`n--- Setup Python environment ---" -ForegroundColor Yellow
Push-Location $ProjectDir
if (-not (Test-Path ".venv")) {
    py -3.12 -m venv .venv
}
& .\.venv\Scripts\pip.exe install -q -r requirements.txt
Pop-Location

Write-Host @"

=== ERROR HANDLING EXERCISES ===

1. PRODUCER CONNECTION FAILURE
   docker compose stop     (in kafka-producer-consumer/)
   python producer.py      (observe retry logs)
   docker compose start
   python producer.py      (should succeed)

2. CONSUMER CONNECTION FAILURE
   docker compose stop
   python consumer.py      (observe retry logs)

3. KAFKA SHUTDOWN DURING CONSUME
   Terminal 1: python consumer.py
   Terminal 2: docker compose stop (then start to recover)

4. INVALID JSON
   docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic user-events
   (type: this is not json)
   python consumer.py      (observe WARNING log, valid messages still processed)

5. NORMAL OPERATION
   Terminal 1: python consumer.py
   Terminal 2: python producer.py

Screenshot error/recovery logs -> screenshots\error-handling.png

Full guide: error-handling-notes.md | Examples: error-examples.md

"@ -ForegroundColor Cyan
