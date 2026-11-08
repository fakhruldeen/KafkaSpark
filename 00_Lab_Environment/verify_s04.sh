#!/usr/bin/env bash
# ==============================================================================
# S04 Automated Consumer Validation Script
# Tests consumer groups, partition assignments, manual commits, lag, and DLT.
# Course: Enterprise Real-Time Data Engineering
# Instructor: Mohamed Fouad Fakhruldeen (Fakhr.me)
# ==============================================================================

set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

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
NOW=$(date +%s)
TOPIC_INPUT="test.s04.consumer.${NOW}"
TOPIC_DLT="test.s04.consumer.dlt.${NOW}"
GROUP_ID="gate-audit-s04-${NOW}"

echo "=================================================="
echo " Running S04 Lab Verification Gate..."
echo "=================================================="

# 1. Create Multi-Partition Topics
echo -n "[1/5] Creating dedicated verification topics... "
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --if-not-exists --topic "${TOPIC_INPUT}" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --if-not-exists --topic "${TOPIC_DLT}" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1
echo -e "${GREEN}PASS (3 Partitions Created)${NC}"

# 2. Seed 60 valid records and 10 corrupted records (Poison Pills)
echo -n "[2/5] Seeding valid and poisoned records into topic... "
${PYTHON_BIN} -c "
import sys, json, time
from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092', 'acks': 'all'})
for i in range(60):
    p.produce('${TOPIC_INPUT}', key=f'ORD-{i}'.encode('utf-8'),
              value=json.dumps({'order_id': f'ORD-{i}', 'amount': 100 + i}).encode('utf-8'))
for i in range(10):
    p.produce('${TOPIC_INPUT}', key=f'BAD-{i}'.encode('utf-8'),
              value=b'CORRUPTED_NON_JSON_PAYLOAD_{!@#$')
p.flush(30)
" >/dev/null 2>&1
echo -e "${GREEN}PASS (70 Records Seeded)${NC}"

# 3. Run Python Consumer with DLT Routing & Manual Commits
echo -n "[3/5] Testing Consumer Group & DLT Isolation Pipeline... "
${PYTHON_BIN} -c "
import sys, json, time
from confluent_kafka import Consumer, Producer

c = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': '${GROUP_ID}',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False
})
p_dlt = Producer({'bootstrap.servers': 'localhost:9092'})

c.subscribe(['${TOPIC_INPUT}'])

processed = 0
poisoned = 0
start_time = time.time()

while time.time() - start_time < 12:
    msg = c.poll(0.5)
    if msg is None:
        if processed + poisoned >= 70:
            break
        continue
    if msg.error():
        continue
    try:
        data = json.loads(msg.value().decode('utf-8'))
        processed += 1
    except Exception as err:
        poisoned += 1
        p_dlt.produce('${TOPIC_DLT}', key=msg.key(), value=msg.value())
    c.commit(msg, asynchronous=False)

c.close()
p_dlt.flush(10)

if processed == 60 and poisoned == 10:
    sys.exit(0)
else:
    print(f'Mismatch: processed={processed}, poisoned={poisoned}')
    sys.exit(1)
" >/dev/null 2>&1
echo -e "${GREEN}PASS (60 Processed, 10 Quarantined to DLT)${NC}"

# 4. Verify Consumer Group Lag is ZERO on Brokers
echo -n "[4/5] Inspecting Consumer Group Lag via Broker Group API... "
LAG_SUM=$(docker exec kafka1 ${KAFKA_BIN}/kafka-consumer-groups.sh \
  --bootstrap-server kafka1:9092 --describe --group "${GROUP_ID}" 2>/dev/null | awk '$6 ~ /^[0-9]+$/ {sum += $6} END {print (sum == "" ? 0 : sum)}')

if [ "${LAG_SUM}" -eq 0 ]; then
  echo -e "${GREEN}PASS (Lag = 0 across all 3 partitions)${NC}"
else
  echo -e "${RED}FAIL (Unprocessed lag detected: ${LAG_SUM})${NC}"
  exit 1
fi

# 5. Run Pytest Suite for S04 Consumer Logic
echo -n "[5/5] Running S04 Consumer Unit Logic Test Suite (pytest)... "
(cd "${LAB_DIR}" && ${PYTEST_BIN} tests/test_s04_consumer_logic.py >/dev/null 2>&1)
echo -e "${GREEN}PASS (All pytest consumer checks green)${NC}"

# Cleanup
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic "${TOPIC_INPUT}" >/dev/null 2>&1 || true
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic "${TOPIC_DLT}" >/dev/null 2>&1 || true

echo "=================================================="
echo -e "${BOLD}${GREEN}★ PASS: S04 Lab Requirements 100% Satisfied! ★${NC}"
echo "=================================================="
