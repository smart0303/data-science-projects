# Topic 03: Create and manage Kafka topics
# Run from: d:\Work\data-science-projects\kafka-producer-consumer-system\topic-03-create-kafka-topics
# Prerequisite: Kafka running (Topic 02 docker compose)

$ErrorActionPreference = "Stop"
$KafkaDir = "d:\Work\data-science-projects\kafka-producer-consumer"
$KafkaExec = "docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092"

function Invoke-KafkaTopics {
    param([string[]]$Args)
    $cmd = "$KafkaExec " + ($Args -join " ")
    Write-Host "> $cmd" -ForegroundColor DarkGray
    Invoke-Expression $cmd
}

Write-Host "`n=== Topic 03: Kafka Topic Management ===" -ForegroundColor Cyan

Write-Host "`n--- Ensure Kafka is running ---" -ForegroundColor Yellow
Push-Location $KafkaDir
docker compose up -d
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
    if ($i -eq $maxAttempts) {
        Write-Host "Kafka did not become healthy in time. Check: docker compose logs kafka" -ForegroundColor Red
        exit 1
    }
}

Write-Host "`n--- 1. List existing topics ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--list")

Write-Host "`n--- 2. Create user-events ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--create", "--topic", "user-events", "--partitions", "3", "--replication-factor", "1", "--if-not-exists")

Write-Host "`n--- 3. Create order-events ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--create", "--topic", "order-events", "--partitions", "3", "--replication-factor", "1", "--if-not-exists")

Write-Host "`n--- 4. List all topics ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--list")

Write-Host "`n--- 5. Describe user-events ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--describe", "--topic", "user-events")

Write-Host "`n--- 6. Delete and recreate test-topic ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--create", "--topic", "test-topic", "--partitions", "1", "--replication-factor", "1", "--if-not-exists")
Invoke-KafkaTopics @("--delete", "--topic", "test-topic")
Start-Sleep -Seconds 2
Invoke-KafkaTopics @("--create", "--topic", "test-topic", "--partitions", "2", "--replication-factor", "1")
Invoke-KafkaTopics @("--describe", "--topic", "test-topic")

Write-Host "`n--- 7. Final topic list (screenshot this) ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--list")

Write-Host "`n--- 8. Clean up test-topic ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--delete", "--topic", "test-topic")

Write-Host "`n--- Final topics ---" -ForegroundColor Cyan
Invoke-KafkaTopics @("--list")

Write-Host "`nDone. Screenshot the topic list showing user-events and order-events." -ForegroundColor Green
Write-Host "Save to: topic-03-create-kafka-topics\screenshots\topics-list.png" -ForegroundColor Green
