# Topic 02: Local Kafka Setup - verification script
# Run from: d:\Work\data-science-projects\kafka-producer-consumer

Write-Host "`n=== 1. Verify Docker ===" -ForegroundColor Cyan
docker --version
docker compose version

Write-Host "`n=== 2. Start Kafka ===" -ForegroundColor Cyan
docker compose up -d

Write-Host "`n=== 3. Verify containers are running ===" -ForegroundColor Cyan
docker compose ps

Write-Host "`n=== 4. Wait for Kafka health check ===" -ForegroundColor Cyan
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

Write-Host "`n=== 5. Test broker connectivity ===" -ForegroundColor Cyan
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

Write-Host "`n=== 6. Stop containers ===" -ForegroundColor Cyan
docker compose stop
docker compose ps

Write-Host "`n=== 7. Restart containers ===" -ForegroundColor Cyan
docker compose start
docker compose ps

Write-Host "`nDone. Take a screenshot of 'docker compose ps' showing kafka running." -ForegroundColor Green
