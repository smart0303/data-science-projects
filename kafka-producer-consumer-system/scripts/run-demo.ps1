# Run the portfolio demo
# Run from project root via: .\scripts\run-demo.ps1

$ErrorActionPreference = "Stop"
$ProjectDir = $PSScriptRoot | Split-Path -Parent

& "$ProjectDir\scripts\setup.ps1"

Write-Host "`n--- Reset consumer group ---" -ForegroundColor Yellow
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group welcome-email-service --delete 2>$null

Write-Host @"

=== PORTFOLIO DEMO ===

TERMINAL 1 (start first):
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python consumer.py

TERMINAL 2:
  cd $ProjectDir
  .\.venv\Scripts\Activate.ps1
  python producer.py

5 users sign up; 5 welcome emails are sent.
Screenshot both terminals for your portfolio.

"@ -ForegroundColor Cyan
