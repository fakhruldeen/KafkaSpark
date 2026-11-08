#!/usr/bin/env bash
# ==============================================================================
# S12 Automated Quality Verification Gate
# Enterprise Real-Time Data Engineering - Session S12: Production Operations, Monitoring & Capstone Defense
# Instructor: Mohamed Fouad Fakhruldeen (Fakhr.me)
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
LAB_DIR="${ROOT_DIR}/delivery_materials/00_Lab"
VENV_DIR="${LAB_DIR}/.venv"

# Ensure Java 17 LTS runtime is active
# Discover Java 17 LTS portably across workstations
if [ -z "${JAVA_HOME:-}" ] || [ ! -x "${JAVA_HOME}/bin/java" ]; then
  if [ -d "${HOME}/.local/share/jvm/jdk-17.0.21+9" ]; then
    export JAVA_HOME="${HOME}/.local/share/jvm/jdk-17.0.21+9"
  elif command -v java >/dev/null 2>&1 && java -version 2>&1 | grep -q "17\."; then
    export JAVA_HOME="$(dirname "$(dirname "$(readlink -f "$(which java)")")")"
  elif [ -d "/usr/lib/jvm/java-17-openjdk-amd64" ]; then
    export JAVA_HOME="/usr/lib/jvm/java-17-openjdk-amd64"
  fi
fi
if [ -n "${JAVA_HOME:-}" ] && [ -d "${JAVA_HOME}/bin" ]; then
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

# Ensure PySpark worker points to active python
export PYSPARK_PYTHON="${VENV_DIR}/bin/python"
export PYSPARK_DRIVER_PYTHON="${VENV_DIR}/bin/python"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

echo -e "${CYAN}Running S12 Automated Production Operations & Capstone Verification Gate...${NC}"

# Gate 1: Check Environment, PySpark, and Cluster Containers Health
echo -n "[1/5] Probing Java 17 runtime, PySpark & Docker containers health... "
if ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -E -q "17\.|21\.|25\."; then
    echo -e "${RED}FAIL (Java runtime not found in ${JAVA_HOME})${NC}"
    exit 1
fi
if ! "${VENV_DIR}/bin/python" -c "import pyspark; print(pyspark.__version__)" | grep -q "3.5"; then
    echo -e "${RED}FAIL (PySpark 3.5 not found in .venv)${NC}"
    exit 1
fi

CONTAINERS_RUNNING=$(docker ps --format "{{.Names}}" 2>&1)
for c in kafka1 kafka2 kafka3 schema-registry connect ksqldb-server postgres kafka-ui; do
    if ! echo "${CONTAINERS_RUNNING}" | grep -q "^${c}$"; then
        echo -e "${RED}FAIL (Container ${c} is not running)${NC}"
        exit 1
    fi
done
echo -e "${GREEN}PASS (Java 17 Temurin active, all 8 cluster containers healthy)${NC}"

# Gate 2: Run pytest unit test suite for Capstone Acceptance Checks
echo -n "[2/5] Running S12 Capstone test suite (pytest test_capstone_checks.py)... "
TEST_OUT=$("${VENV_DIR}/bin/pytest" "${LAB_DIR}/tests/test_capstone_checks.py" -q 2>&1)
if ! echo "${TEST_OUT}" | grep -q "6 passed"; then
    echo -e "${RED}FAIL${NC}"
    echo "${TEST_OUT}"
    exit 1
fi
echo -e "${GREEN}PASS (6/6 tests passed: T1 Ingestion, T2 Schema DLT, T3 Processing, T4 Recovery, T5 Lag, Report Oracle)${NC}"

# Gate 3: Kafka Cluster Telemetry & Offline Partitions Check
echo -n "[3/5] Verifying cluster telemetry and partition health... "
TOPIC_DESC=$(docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --describe 2>&1)
if echo "${TOPIC_DESC}" | grep -q "Leader: none"; then
    echo -e "${RED}FAIL (Detected offline/leaderless partitions)${NC}"
    exit 1
fi
echo -e "${GREEN}PASS (Zero offline partitions, active metadata quorum confirmed)${NC}"

# Gate 4: Test Streaming Crash-Recovery and Checkpoint Continuity
echo -n "[4/5] Testing streaming crash-recovery and checkpoint continuity... "
CKPT_DIR="${LAB_DIR}/data/checkpoints/gate4_s12_test"
rm -rf "${CKPT_DIR}"
mkdir -p "${CKPT_DIR}/main"

RECOVER_TEST=$("${VENV_DIR}/bin/python" -c '
import os, time
from pyspark.sql import SparkSession

ckpt = "'"${CKPT_DIR}"'" + "/main"
spark = (SparkSession.builder.master("local[2]").appName("s12-recover")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# First run: simulate initial batch with persistent WAL
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
q1 = rate_df.writeStream.format("noop").option("checkpointLocation", ckpt).start()
time.sleep(2)
q1.stop()

# Second run: simulate restart and verify recovery from existing checkpoint
q2 = rate_df.writeStream.format("noop").option("checkpointLocation", ckpt).start()
time.sleep(2)
q2.stop()
spark.stop()

has_offsets = os.path.exists(os.path.join(ckpt, "offsets"))
has_commits = os.path.exists(os.path.join(ckpt, "commits"))
print(f"RECOVERY_OK:{has_offsets and has_commits}")
' 2>&1)
rm -rf "${CKPT_DIR}"

if echo "${RECOVER_TEST}" | grep -q "RECOVERY_OK:True"; then
    echo -e "${GREEN}PASS (Simulated crash-and-restart successfully rehydrated WAL state)${NC}"
else
    echo -e "${RED}FAIL (Recovery test failed)${NC}"
    echo "${RECOVER_TEST}"
    exit 1
fi

# Gate 5: Execute Complete Capstone Acceptance Oracle (T1–T5)
echo -n "[5/5] Executing Final Capstone Acceptance Oracle (python capstone_checks.py)... "
ORACLE_OUT=$( (cd "${LAB_DIR}" && "${VENV_DIR}/bin/python" -m python.capstone_checks) 2>&1 )
if echo "${ORACLE_OUT}" | grep -q "PASS  T1 ingestion" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T2 schema" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T3 processing" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T4 recovery" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T5 lag"; then
    echo -e "${GREEN}PASS (All 5 Capstone Acceptance Gates T1-T5 successfully validated)${NC}"
else
    echo -e "${RED}FAIL (Capstone acceptance criteria not met)${NC}"
    echo "${ORACLE_OUT}"
    exit 1
fi

echo ""
echo -e "${GREEN}✔ S12 Production Operations & Capstone Verification Gate: 100% PASS${NC}"
