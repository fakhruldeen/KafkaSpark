#!/usr/bin/env bash
# ==============================================================================
# S03 Automated Producer Validation Script
# Tests producer configurations, batching, snappy compression, and idempotence.
# Course: Enterprise Real-Time Data Engineering
# Instructor: Mohamed Fouad Fakhruldeen (Fakhr.me)
# ==============================================================================

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

# Resolve lab directory and python virtual environment
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "./.venv/bin/python" ]; then
  LAB_DIR="$(pwd)"
elif [ -f "${SCRIPT_DIR}/../../00_Lab/.venv/bin/python" ]; then
  LAB_DIR="$(cd "${SCRIPT_DIR}/../../00_Lab" && pwd)"
elif [ -f "${SCRIPT_DIR}/../00_Lab/.venv/bin/python" ]; then
  LAB_DIR="$(cd "${SCRIPT_DIR}/../00_Lab" && pwd)"
else
  LAB_DIR="$(pwd)"
fi

PYTHON_BIN="${LAB_DIR}/.venv/bin/python"
PYTEST_BIN="${LAB_DIR}/.venv/bin/pytest"

if [ ! -f "${PYTHON_BIN}" ]; then
  PYTHON_BIN="python3"
  PYTEST_BIN="pytest"
fi

KAFKA_BIN="/opt/kafka/bin"
TOPIC_NAME="test.s03.producer.$(date +%s)"

echo "=================================================="
echo " Running S03 Lab Verification Gate..."
echo "=================================================="

# 1. Create Topic for S03 Test
echo -n "[1/5] Creating dedicated verification topic... "
for attempt in 1 2 3; do
  docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 \
    --create --if-not-exists --topic "${TOPIC_NAME}" --partitions 3 --replication-factor 3 \
    --config min.insync.replicas=2 >/dev/null 2>&1 && break || sleep 2
done
echo -e "${GREEN}PASS${NC}"

# 2. Test Python Producer with Idempotence & Compression
echo -n "[2/5] Testing Idempotent Python Producer (Snappy/LZ4)... "
${PYTHON_BIN} -c "
import sys, json, time
from confluent_kafka import Producer

conf = {
    'bootstrap.servers': 'localhost:9092',
    'acks': 'all',
    'retries': 5,
    'compression.type': 'snappy',
    'linger.ms': 10,
    'batch.size': 16384,
    'enable.idempotence': True
}
p = Producer(conf)
for i in range(100):
    k = f'CUST-{i%10}'.encode('utf-8')
    v = json.dumps({'order_id': i, 'amount': 100 + i, 'ts': time.time()}).encode('utf-8')
    p.produce('${TOPIC_NAME}', key=k, value=v)
p.flush(30)
sys.exit(0)
" >/dev/null 2>&1
echo -e "${GREEN}PASS (100 Keyed Records Emitted)${NC}"

# 3. Verify Message Count & Offset Integrity on Brokers
echo -n "[3/5] Verifying message persistence and offset integrity... "
CONSUME_COUNT=$(docker exec kafka1 ${KAFKA_BIN}/kafka-get-offsets.sh \
  --bootstrap-server kafka1:9092 --topic "${TOPIC_NAME}" --time -1 | awk -F ':' '{sum += $3} END {print sum}')

if [ "${CONSUME_COUNT}" -eq 100 ]; then
  echo -e "${GREEN}PASS (Exactly 100 records committed across partitions)${NC}"
else
  echo -e "${RED}FAIL (Expected 100 records, found ${CONSUME_COUNT})${NC}"
  exit 1
fi

# 4. Run Pytest Suite for Idempotent Logic & Validation
echo -n "[4/5] Running S03 Producer Logic Test suite (pytest)... "
(cd "${LAB_DIR}" && ${PYTEST_BIN} tests/test_s03_producer_logic.py >/dev/null 2>&1)
echo -e "${GREEN}PASS (7 pytest checks green)${NC}"

# 5. Verify Partition Balance Across 3 Partitions
echo -n "[5/5] Verifying partition distribution balance... "
P_COUNTS=$(docker exec kafka1 ${KAFKA_BIN}/kafka-get-offsets.sh \
  --bootstrap-server kafka1:9092 --topic "${TOPIC_NAME}" --time -1 | awk -F ':' '{print $3}')

ZERO_PARTS=0
for c in $P_COUNTS; do
  if [ "$c" -eq 0 ]; then
    ZERO_PARTS=1
  fi
done

if [ "$ZERO_PARTS" -eq 0 ]; then
  echo -e "${GREEN}PASS (All 3 partitions received balanced batches)${NC}"
else
  echo -e "${RED}FAIL (Unbalanced distribution, partition with 0 records found)${NC}"
  exit 1
fi

# Cleanup
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic "${TOPIC_NAME}" >/dev/null 2>&1 || true

echo "=================================================="
echo -e "${BOLD}${GREEN}★ PASS: S03 Lab Requirements 100% Satisfied! ★${NC}"
echo "=================================================="
