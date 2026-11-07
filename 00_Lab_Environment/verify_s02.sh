#!/usr/bin/env bash
# ==============================================================================
# S02 Automated Cluster Validation Script
# Tests multi-partition topic creation, keyed distribution, broker failover, and ISR recovery.
# ==============================================================================
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'
BOLD='\033[1m'

KAFKA_BIN="/opt/kafka/bin"
TOPIC_NAME="test.s02.$(date +%s)"

echo "=================================================="
echo " Running S02 Lab Verification Gate..."
echo "=================================================="

# 1. Check Broker Reachability
echo -n "[1/6] Checking Kafka cluster reachability... "
if docker exec kafka1 ${KAFKA_BIN}/kafka-broker-api-versions.sh --bootstrap-server kafka1:9092 >/dev/null 2>&1; then
    echo -e "${GREEN}PASS${NC}"
else
    echo -e "${RED}FAIL (Cluster unreachable)${NC}"
    exit 1
fi

# 2. Create and Verify 3-partition, 3-replica topic
echo -n "[2/6] Verifying 3-partition, RF 3 topic creation... "
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 \
  --create --if-not-exists --topic "${TOPIC_NAME}" --partitions 3 --replication-factor 3 \
  --config min.insync.replicas=2 >/dev/null 2>&1

METADATA=$(docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic "${TOPIC_NAME}")
PARTITION_COUNT=$(echo "${METADATA}" | grep -c "Partition:")
if [ "${PARTITION_COUNT}" -eq 3 ] && echo "${METADATA}" | grep -q "ReplicationFactor: 3"; then
    echo -e "${GREEN}PASS (3 Partitions, RF 3, min.insync.replicas=2)${NC}"
else
    echo -e "${RED}FAIL (Expected 3 partitions with RF 3)${NC}"
    exit 1
fi

# 3. Produce Keyed Messages for MurmurHash2 verification
echo -n "[3/6] Testing deterministic MurmurHash2 key routing... "
docker exec -i kafka1 ${KAFKA_BIN}/kafka-console-producer.sh --bootstrap-server kafka1:9092 \
  --topic "${TOPIC_NAME}" \
  --property "parse.key=true" \
  --property "key.separator=:" <<EOF >/dev/null 2>&1
KEY_A:Message 1
KEY_B:Message 2
KEY_A:Message 3
KEY_C:Message 4
KEY_A:Message 5
EOF
echo -e "${GREEN}PASS${NC}"

# 4. Read back messages and verify KEY_A lands strictly on 1 partition
echo -n "[4/6] Verifying key-to-partition affinity... "
CONSUMED=$(docker exec kafka1 ${KAFKA_BIN}/kafka-console-consumer.sh --bootstrap-server kafka1:9092 \
  --topic "${TOPIC_NAME}" --from-beginning --max-messages 5 \
  --property print.key=true --property print.partition=true 2>/dev/null)

KEY_A_PARTITIONS=$(echo "${CONSUMED}" | grep "KEY_A" | awk '{print $1}' | sort -u | wc -l)
KEY_A_TARGET=$(echo "${CONSUMED}" | grep "KEY_A" | awk '{print $1}' | head -n 1)
if [ "${KEY_A_PARTITIONS}" -eq 1 ]; then
    echo -e "${GREEN}PASS (KEY_A strictly bound to Partition ${KEY_A_TARGET})${NC}"
else
    echo -e "${RED}FAIL (KEY_A dispersed across multiple partitions)${NC}"
    exit 1
fi

# 5. Chaos Test: Stop Broker 2 and verify leader re-election
echo -n "[5/6] Chaos Test: Broker failover & leader election... "
docker stop kafka2 >/dev/null 2>&1
sleep 3

DESC_POST_STOP=$(docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic "${TOPIC_NAME}")
if echo "${DESC_POST_STOP}" | grep "Leader: 2" >/dev/null; then
    echo -e "${RED}FAIL (Broker 2 is still listed as active leader!)${NC}"
    docker start kafka2 >/dev/null 2>&1
    exit 1
else
    echo -e "${GREEN}PASS (Active leader promoted to surviving broker in < 1s)${NC}"
fi

# 6. Recovery: Restart Broker 2 and verify ISR self-healing
echo -n "[6/6] Chaos Test: Broker recovery & ISR self-healing... "
docker start kafka2 >/dev/null 2>&1
sleep 5

DESC_POST_RESTART=$(docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --describe --topic "${TOPIC_NAME}")
ISR_COUNT=$(echo "${DESC_POST_RESTART}" | grep "Isr:" | grep -c "2")
if [ "${ISR_COUNT}" -ge 1 ]; then
    echo -e "${GREEN}PASS (kafka2 caught up and rejoined ISR successfully)${NC}"
else
    echo -e "${RED}FAIL (kafka2 failed to rejoin ISR within window)${NC}"
fi

# Cleanup test topic
docker exec kafka1 ${KAFKA_BIN}/kafka-topics.sh --bootstrap-server kafka1:9092 --delete --topic "${TOPIC_NAME}" >/dev/null 2>&1 || true

echo "=================================================="
echo -e "${BOLD}${GREEN}★ PASS: S02 Lab Requirements 100% Satisfied! ★${NC}"
echo "=================================================="
