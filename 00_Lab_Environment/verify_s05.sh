#!/usr/bin/env bash
# ==============================================================================
# S05 Automated Schema Registry & Avro Validation Script
# Tests schema registration, Avro serialization, compatible evolution, and breaking change rejection.
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

if [ ! -f "${PYTHON_BIN}" ]; then
  PYTHON_BIN="python3"
fi

SCHEMA_REGISTRY_URL="http://localhost:8081"
SUBJECT_NAME="test.s05.orders-value"

echo "=================================================="
echo " Running S05 Lab Verification Gate..."
echo "=================================================="

# 1. Probe Schema Registry Health
echo -n "[1/5] Probing Confluent Schema Registry reachability... "
if curl -fsS "${SCHEMA_REGISTRY_URL}/subjects" >/dev/null 2>&1; then
  echo -e "${GREEN}PASS (HTTP 200 OK)${NC}"
else
  echo -e "${RED}FAIL (Schema Registry unreachable on port 8081)${NC}"
  exit 1
fi

# 2. Register Schema V1 via REST API
echo -n "[2/5] Registering Avro Schema V1 via REST API... "
PAYLOAD_V1=$(jq -c . "${LAB_DIR}/schemas/order_event_v1.avsc" | jq -R . | jq -c '{schema: .}')

SCHEMA_ID=""
for attempt in 1 2 3; do
  REGISTER_RESP=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
    --data "${PAYLOAD_V1}" \
    "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions" || true)
  SCHEMA_ID=$(echo "${REGISTER_RESP}" | jq -r '.id // empty')
  if [ -n "${SCHEMA_ID}" ] && [ "${SCHEMA_ID}" != "null" ]; then
    break
  fi
  sleep 2
done
if [ -n "${SCHEMA_ID}" ] && [ "${SCHEMA_ID}" != "null" ]; then
  echo -e "${GREEN}PASS (Registered with Schema ID: ${SCHEMA_ID})${NC}"
else
  echo -e "${RED}FAIL (Registration failed: ${REGISTER_RESP})${NC}"
  exit 1
fi

# 3. Test Compatible Evolution (Schema V2 with Default Value)
echo -n "[3/5] Testing compatible Schema evolution (V2 with default)... "
PAYLOAD_V2=$(jq -c . "${LAB_DIR}/schemas/order_event_v2_compatible.avsc" | jq -R . | jq -c '{schema: .}')
REGISTER_V2_RESP=$(curl -s -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${PAYLOAD_V2}" \
  "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions")

SCHEMA_V2_ID=$(echo "${REGISTER_V2_RESP}" | jq -r '.id // empty')
if [ -n "${SCHEMA_V2_ID}" ] && [ "${SCHEMA_V2_ID}" != "null" ]; then
  echo -e "${GREEN}PASS (Evolved to V2 with Schema ID: ${SCHEMA_V2_ID})${NC}"
else
  echo -e "${RED}FAIL (Compatible evolution rejected: ${REGISTER_V2_RESP})${NC}"
  exit 1
fi

# 4. Test Incompatible Breaking Change Rejection (HTTP 409 Conflict / 422 Unprocessable)
echo -n "[4/5] Testing breaking schema rejection (missing default)... "
PAYLOAD_V3=$(jq -c . "${LAB_DIR}/schemas/order_event_v3_breaking.avsc" | jq -R . | jq -c '{schema: .}')
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  --data "${PAYLOAD_V3}" \
  "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}/versions")

if [ "${HTTP_STATUS}" -eq 409 ] || [ "${HTTP_STATUS}" -eq 422 ]; then
  echo -e "${GREEN}PASS (Rejected with HTTP ${HTTP_STATUS} Incompatible Schema Conflict)${NC}"
else
  echo -e "${RED}FAIL (Expected HTTP 409/422, received: ${HTTP_STATUS})${NC}"
  exit 1
fi

# 5. Run Python Avro Producer Pipeline
echo -n "[5/5] Running Python Avro Producer against Schema Registry... "
OUTPUT=$(cd "${LAB_DIR}" && PYTHONPATH=. ${PYTHON_BIN} python/s05_avro.py produce --schema schemas/order_event_v1.avsc --count 5 2>&1 || true)
if echo "${OUTPUT}" | grep -q "produced .* Avro events"; then
  echo -e "${GREEN}PASS (Avro records produced and validated with Schema ID)${NC}"
else
  echo -e "${RED}FAIL (Avro producer failed: ${OUTPUT})${NC}"
  exit 1
fi

# Cleanup test subject
curl -s -X DELETE "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}" >/dev/null 2>&1 || true
curl -s -X DELETE "${SCHEMA_REGISTRY_URL}/subjects/${SUBJECT_NAME}?permanent=true" >/dev/null 2>&1 || true

echo "=================================================="
echo -e "${BOLD}${GREEN}★ PASS: S05 Lab Requirements 100% Satisfied! ★${NC}"
echo "=================================================="
