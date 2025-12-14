#!/bin/bash
# Script để chạy POS Simulator (Kafka Producer)

echo "🚀 Starting POS Simulator - Kafka Producer"
echo "=========================================="

# Install dependencies nếu chưa có
if ! python -c "import kafka" 2>/dev/null; then
    echo "📦 Installing dependencies..."
    pip install kafka-python
fi

# Chạy POS simulator
python pos_simulator.py \
    --broker localhost:9094 \
    --topic credit-card-transactions \
    --csv data/transactions.csv \
    --min-delay 1 \
    --max-delay 5 \
    --loop

# Options:
# --broker: Kafka broker address (default: localhost:9094)
# --topic: Kafka topic name (default: credit-card-transactions)
# --csv: Path to CSV file (default: data/transactions.csv)
# --min-delay: Minimum delay between transactions in seconds (default: 1)
# --max-delay: Maximum delay between transactions in seconds (default: 5)
# --loop: Loop through data continuously (optional flag)
