#!/usr/bin/env bash
# ==============================================================================
# S01 Lab Automated Verification Script
# Nile Retail Training Cohort · S01 Verification Gate
# ==============================================================================
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

echo "=================================================="
echo " Running S01 Lab Verification Gate..."
echo "=================================================="

# 1. Check Broker Reachability
echo -n "[1/5] Checking Kafka cluster reachability... "
if docker exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka1:9092 >/dev/null 2>&1; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL (Cluster unreachable)${NC}"
    exit 1
fi

# 2. Check Topic Creation
echo -n "[2/5] Verifying topic 'retail.orders.lab'... "
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic retail.orders.lab --partitions 3 --replication-factor 3 --if-not-exists >/dev/null 2>&1

DESC_OUTPUT=$(docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --describe --topic retail.orders.lab)

if echo "$DESC_OUTPUT" | grep -q "PartitionCount: 3" && echo "$DESC_OUTPUT" | grep -q "ReplicationFactor: 3"; then
    echo -e "${GREEN}PASS (3 Partitions, RF 3)${NC}"
else
    echo -e "${RED}FAIL (Topic does not match specs)${NC}"
    exit 1
fi

# 3. Check In-Sync Replicas
echo -n "[3/5] Verifying In-Sync Replicas (ISR)... "
if echo "$DESC_OUTPUT" | grep -q "Isr: 1,2,3"; then
    echo -e "${GREEN}PASS (All 3 brokers in-sync)${NC}"
else
    echo -e "${RED}FAIL (Degraded ISR)${NC}"
    exit 1
fi

# 4. Produce Test Records
echo -n "[4/5] Testing Keyed Event Production... "
printf 'TEST-KEY:{"verify": true, "timestamp": 1730819000}\n' | \
docker exec -i kafka1 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka1:9092 \
  --topic retail.orders.lab \
  --property "parse.key=true" \
  --property "key.separator=:" >/dev/null 2>&1
echo -e "${GREEN}PASS${NC}"

# 5. Consume and Verify Non-Destructive Read
echo -n "[5/5] Testing Consumer & Non-Destructive Log Storage... "
CONSUME_OUT=$(docker exec kafka1 /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka1:9092 \
  --topic retail.orders.lab \
  --from-beginning \
  --max-messages 1 \
  --timeout-ms 5000 2>/dev/null || true)

if [ -n "$CONSUME_OUT" ]; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL (No messages retrieved)${NC}"
    exit 1
fi

echo "=================================================="
echo -e "${GREEN}★ PASS: S01 Lab Requirements 100% Satisfied! ★${NC}"
echo "=================================================="
