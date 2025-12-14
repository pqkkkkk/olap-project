# Script để chạy POS Simulator (Kafka Producer) trên Windows

Write-Host "🚀 Starting POS Simulator - Kafka Producer" -ForegroundColor Green
Write-Host "==========================================" -ForegroundColor Green

# Kiểm tra và install dependencies
Write-Host "📦 Checking dependencies..." -ForegroundColor Yellow
try {
    python -c "import kafka" 2>$null
} catch {
    Write-Host "Installing kafka-python..." -ForegroundColor Yellow
    pip install kafka-python
}

# Chạy POS simulator
Write-Host "`n▶️  Starting simulation..." -ForegroundColor Cyan
python pos_simulator.py `
    --broker localhost:9094 `
    --topic credit-card-transactions `
    --csv data/transactions.csv `
    --min-delay 1 `
    --max-delay 5 `
    --loop

# Options:
# --broker: Kafka broker address (default: localhost:9094)
# --topic: Kafka topic name (default: credit-card-transactions)
# --csv: Path to CSV file (default: data/transactions.csv)
# --min-delay: Minimum delay between transactions in seconds (default: 1)
# --max-delay: Maximum delay between transactions in seconds (default: 5)
# --loop: Loop through data continuously (optional flag)
