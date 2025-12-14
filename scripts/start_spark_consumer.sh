#!/bin/bash
# Script để chạy Spark Streaming Consumer trong Docker

echo "🚀 Starting Spark Streaming Consumer"
echo "====================================="

CONTAINER_NAME="spark-master"
SCRIPT_PATH="/opt/spark-scripts/spark_streaming_consumer.py"

# Kiểm tra container có đang chạy không
if ! docker ps | grep -q $CONTAINER_NAME; then
    echo "❌ Container $CONTAINER_NAME is not running!"
    echo "Please start Docker services first: docker-compose up -d"
    exit 1
fi

echo "📦 Installing dependencies in Spark container..."
docker exec $CONTAINER_NAME pip install kafka-python

echo ""
echo "▶️  Starting Spark Streaming Consumer..."
echo "   Mode: Console output"
echo "   Press Ctrl+C to stop"
echo ""

# Submit Spark job
docker exec -it $CONTAINER_NAME \
    spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --master local[*] \
    $SCRIPT_PATH \
    --broker kafka-broker:9092 \
    --topic credit-card-transactions \
    --output console

# Options:
# --broker: Kafka broker address (default: kafka-broker:9092)
# --topic: Kafka topic (default: credit-card-transactions)
# --output: Output type - console, csv, hdfs, all (default: console)
