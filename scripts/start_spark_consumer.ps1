# Script để chạy Spark Streaming Consumer trong Docker (Windows)

Write-Host "🚀 Starting Spark Streaming Consumer" -ForegroundColor Green
Write-Host "=====================================" -ForegroundColor Green

$CONTAINER_NAME = "spark-master"
$SCRIPT_PATH = "/opt/spark-scripts/spark_streaming_consumer.py"

# Kiểm tra container có đang chạy không
$containerRunning = docker ps --format "{{.Names}}" | Select-String -Pattern $CONTAINER_NAME
if (-not $containerRunning) {
    Write-Host "❌ Container $CONTAINER_NAME is not running!" -ForegroundColor Red
    Write-Host "Please start Docker services first: docker-compose up -d" -ForegroundColor Yellow
    exit 1
}

Write-Host "`n📦 Installing dependencies in Spark container..." -ForegroundColor Yellow
docker exec $CONTAINER_NAME pip install kafka-python

Write-Host ""
Write-Host "▶️  Starting Spark Streaming Consumer..." -ForegroundColor Cyan
Write-Host "   Mode: Console output" -ForegroundColor Gray
Write-Host "   Press Ctrl+C to stop" -ForegroundColor Gray
Write-Host ""

# Submit Spark job
docker exec -it $CONTAINER_NAME `
    spark-submit `
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
    --master local[*] `
    $SCRIPT_PATH `
    --broker kafka-broker:9092 `
    --topic credit-card-transactions `
    --output console

# Options:
# --broker: Kafka broker address (default: kafka-broker:9092)
# --topic: Kafka topic (default: credit-card-transactions)
# --output: Output type - console, csv, hdfs, all (default: console)
