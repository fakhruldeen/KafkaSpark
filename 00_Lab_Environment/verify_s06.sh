#!/usr/bin/env bash
# ==============================================================================
# Course: Enterprise Real-Time Data Engineering
# Instructor: Mohamed Fouad Fakhruldeen (Fakhr.me)
# Session: S06 - Kafka Connect & Distributed Data Integration
# Verification Gate Script: verify_s06.sh
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${SCRIPT_DIR}/../../00_Lab" && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

CONNECT_URL="http://localhost:8083"

echo -e "${BOLD}${CYAN}Running S06 Automated Kafka Connect Verification Gate...${NC}"

# 1. Probe Kafka Connect REST API & Plugins
echo -n "[1/5] Probing Kafka Connect REST endpoint (${CONNECT_URL})... "
STATUS=$(curl -s -o /dev/null -w "%{http_code}" "${CONNECT_URL}/connector-plugins" || true)
if [ "${STATUS}" -eq 200 ]; then
  PLUGINS=$(curl -s "${CONNECT_URL}/connector-plugins")
  if echo "${PLUGINS}" | grep -q "FileStreamSourceConnector" && echo "${PLUGINS}" | grep -q "FileStreamSinkConnector"; then
    echo -e "${GREEN}PASS (HTTP 200 OK & FileStream plugins loaded)${NC}"
  else
    echo -e "${RED}FAIL (Required FileStream connector plugins missing)${NC}"
    exit 1
  fi
else
  echo -e "${RED}FAIL (HTTP Status: ${STATUS})${NC}"
  exit 1
fi

# 2. Ensure Required Topics Exist
echo -n "[2/5] Verifying required integration topics exist... "
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic orders.copy.v1 --partitions 3 --replication-factor 3 >/dev/null 2>&1
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --create --if-not-exists --topic orders.connect.dlq --partitions 1 --replication-factor 3 >/dev/null 2>&1
echo -e "${GREEN}PASS (orders.copy.v1 & orders.connect.dlq ready)${NC}"

# Clean existing connectors if any
curl -s -X DELETE "${CONNECT_URL}/connectors/orders-file-source" >/dev/null 2>&1 || true
curl -s -X DELETE "${CONNECT_URL}/connectors/orders-file-sink" >/dev/null 2>&1 || true
rm -f "${LAB_DIR}/data/sink-orders.txt"

# Reset Sink Consumer Group Offsets to earliest if group exists
docker exec kafka1 /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server kafka1:9092 \
  --group connect-orders-file-sink --reset-offsets --to-earliest --execute --topic orders.copy.v1 >/dev/null 2>&1 || true

# Append fresh test records to trigger Source connector ingestion
python3 -c "
import json
with open('${LAB_DIR}/data/orders.jsonl', 'a') as f:
    for i in range(25):
        f.write(json.dumps({'event_id': f'evt-live-{i}', 'order_id': f'ord-{i}', 'store_id': 'CAI-01', 'customer_id': 'C0001', 'amount': 150.0, 'event_type': 'OrderPlaced'}) + '\n')
"

# 3. Register Source Connector with SMT RegexRouter
echo -n "[3/5] Registering Source Connector with SMT RegexRouter... "
RESP=$(curl -s -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
  --data @"${LAB_DIR}/connectors/file_source.json" "${CONNECT_URL}/connectors")
HTTP_CODE=$(echo "${RESP}" | tail -n 1)

if [ "${HTTP_CODE}" -eq 201 ] || [ "${HTTP_CODE}" -eq 409 ]; then
  sleep 3
  SRC_STATUS=$(curl -s "${CONNECT_URL}/connectors/orders-file-source/status" | jq -r '.tasks[0].state // "UNKNOWN"')
  if [ "${SRC_STATUS}" = "RUNNING" ]; then
    echo -e "${GREEN}PASS (Source registered, SMT active, task state: RUNNING)${NC}"
  else
    echo -e "${RED}FAIL (Source task state: ${SRC_STATUS})${NC}"
    exit 1
  fi
else
  echo -e "${RED}FAIL (Source registration returned HTTP ${HTTP_CODE})${NC}"
  exit 1
fi

# 4. Register Sink Connector with Dead Letter Queue
echo -n "[4/5] Registering Sink Connector with Dead Letter Queue (DLQ)... "
RESP_SINK=$(curl -s -w "\n%{http_code}" -X POST -H "Content-Type: application/json" \
  --data @"${LAB_DIR}/connectors/file_sink.json" "${CONNECT_URL}/connectors")
HTTP_CODE_SINK=$(echo "${RESP_SINK}" | tail -n 1)

if [ "${HTTP_CODE_SINK}" -eq 201 ] || [ "${HTTP_CODE_SINK}" -eq 409 ]; then
  sleep 3
  SINK_STATUS=$(curl -s "${CONNECT_URL}/connectors/orders-file-sink/status" | jq -r '.tasks[0].state // "UNKNOWN"')
  if [ "${SINK_STATUS}" = "RUNNING" ]; then
    echo -e "${GREEN}PASS (Sink registered, DLQ configured, task state: RUNNING)${NC}"
  else
    echo -e "${RED}FAIL (Sink task state: ${SINK_STATUS})${NC}"
    exit 1
  fi
else
  echo -e "${RED}FAIL (Sink registration returned HTTP ${HTTP_CODE_SINK})${NC}"
  exit 1
fi

# 5. Verify End-to-End Ingestion, Routing and Egress
echo -n "[5/5] Verifying end-to-end data flow (File -> SMT -> Kafka -> DLQ/Sink)... "
sleep 4
if [ -f "${LAB_DIR}/data/sink-orders.txt" ]; then
  LINE_COUNT=$(wc -l < "${LAB_DIR}/data/sink-orders.txt" | tr -d ' ')
  if [ "${LINE_COUNT}" -gt 100 ]; then
    echo -e "${GREEN}PASS (${LINE_COUNT} records ingested, routed & sinked successfully)${NC}"
  else
    echo -e "${RED}FAIL (Sink file contains insufficient records: ${LINE_COUNT})${NC}"
    exit 1
  fi
else
  echo -e "${RED}FAIL (Sink output file ${LAB_DIR}/data/sink-orders.txt was not generated)${NC}"
  exit 1
fi

# Clean up test connectors and restore orders.jsonl to canonical 4,033 lines
curl -s -X DELETE "${CONNECT_URL}/connectors/orders-file-source" >/dev/null 2>&1 || true
curl -s -X DELETE "${CONNECT_URL}/connectors/orders-file-sink" >/dev/null 2>&1 || true
if [ -f "${LAB_DIR}/data/orders.jsonl" ]; then
  sed -i '4034,$d' "${LAB_DIR}/data/orders.jsonl"
fi

echo -e "\n${BOLD}${GREEN}★ PASS: S06 Lab Requirements 100% Satisfied! ★${NC}"
