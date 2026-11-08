#!/usr/bin/env bash
# ==============================================================================
# S11 Automated Quality Verification Gate
# Enterprise Real-Time Data Engineering - Session S11: Kafka & Spark Pipeline Integration
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

echo -e "${CYAN}Running S11 Automated Kafka-Spark Pipeline Integration Verification Gate...${NC}"

# Gate 1: Check Environment and PySpark runtime
echo -n "[1/5] Probing Java 17 runtime & PySpark installation... "
if ! "${JAVA_HOME}/bin/java" -version 2>&1 | grep -E -q "17\.|21\.|25\."; then
    echo -e "${RED}FAIL (Java runtime not found in ${JAVA_HOME})${NC}"
    exit 1
fi
if ! "${VENV_DIR}/bin/python" -c "import pyspark; print(pyspark.__version__)" | grep -q "3.5"; then
    echo -e "${RED}FAIL (PySpark 3.5 not found in .venv)${NC}"
    exit 1
fi
echo -e "${GREEN}PASS (Java runtime active, PySpark loaded)${NC}"

# Gate 2: Run pytest unit test suite for Capstone Checks (T1 to T5 tests)
echo -n "[2/5] Running S11 Capstone test suite (pytest test_capstone_checks.py)... "
TEST_OUT=$("${VENV_DIR}/bin/pytest" "${LAB_DIR}/tests/test_capstone_checks.py" -q 2>&1)
if ! echo "${TEST_OUT}" | grep -q "6 passed"; then
    echo -e "${RED}FAIL${NC}"
    echo "${TEST_OUT}"
    exit 1
fi
echo -e "${GREEN}PASS (6/6 tests passed: T1 Ingestion, T2 Schema DLT, T3 Processing, T4 Recovery, T5 Lag, Report Oracle)${NC}"

# Gate 3: Verify Kafka Pipeline Topics
echo -n "[3/5] Verifying Kafka streaming topics on KRaft cluster... "
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic orders.connect.dlq --partitions 3 --replication-factor 3 --if-not-exists >/dev/null 2>&1
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic kpi --partitions 3 --replication-factor 3 --if-not-exists >/dev/null 2>&1

TOPIC_LIST=$(docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list 2>&1)
if echo "${TOPIC_LIST}" | grep -q "orders.events.v1" && \
   echo "${TOPIC_LIST}" | grep -q "orders.connect.dlq" && \
   echo "${TOPIC_LIST}" | grep -q "kpi"; then
    echo -e "${GREEN}PASS (Kafka topics 'orders.events.v1', 'orders.connect.dlq', and 'kpi' ready)${NC}"
else
    echo -e "${RED}FAIL (Missing required pipeline topics)${NC}"
    exit 1
fi

# Gate 4: Test Spark-Kafka Dual-Branch Architecture with Checkpoint Isolation
echo -n "[4/5] Testing dual-branch streaming logic with checkpoint isolation... "
CKPT_DIR="${LAB_DIR}/data/checkpoints/gate4_s11_test"
rm -rf "${CKPT_DIR}"
mkdir -p "${CKPT_DIR}/main" "${CKPT_DIR}/dlt"

BRANCH_TEST=$("${VENV_DIR}/bin/python" -c '
import os, time
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

ckpt = "'"${CKPT_DIR}"'"
spark = (SparkSession.builder.master("local[2]").appName("s11-verify")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

# Generate synthetic stream with mixed valid and invalid records
rate_df = spark.readStream.format("rate").option("rowsPerSecond", 10).load()
synthetic = rate_df.select(
    F.col("value").cast("string").alias("key"),
    F.when(F.col("value") % 5 == 0, F.lit(None))
     .otherwise(F.concat(F.lit("EVT-"), F.col("value"))).alias("event_id"),
    F.when(F.col("value") % 7 == 0, F.lit(None))
     .otherwise(F.col("value") * 10.5).alias("amount"),
    F.col("timestamp").alias("event_ts")
)

good = synthetic.filter(F.col("event_id").isNotNull() & F.col("amount").isNotNull())
bad = synthetic.filter(F.col("event_id").isNull() | F.col("amount").isNull())

q_main = (good.writeStream.format("memory").queryName("good_stream")
          .option("checkpointLocation", f"{ckpt}/main").start())
q_dlt = (bad.writeStream.format("memory").queryName("dlt_stream")
         .option("checkpointLocation", f"{ckpt}/dlt").start())

time.sleep(2)
q_main.stop()
q_dlt.stop()
spark.stop()

has_main = os.path.exists(os.path.join(ckpt, "main", "offsets"))
has_dlt = os.path.exists(os.path.join(ckpt, "dlt", "offsets"))
print(f"DUAL_SINK_ISOLATED:{has_main and has_dlt}")
' 2>&1)
rm -rf "${CKPT_DIR}"

if echo "${BRANCH_TEST}" | grep -q "DUAL_SINK_ISOLATED:True"; then
    echo -e "${GREEN}PASS (Dual-sink pipelines executing with isolated checkpoint states)${NC}"
else
    echo -e "${RED}FAIL (Dual-sink test failed)${NC}"
    echo "${BRANCH_TEST}"
    exit 1
fi

# Gate 5: Validate Capstone Checks Diagnostic Oracle
echo -n "[5/5] Executing Acceptance Test Oracle (python capstone_checks.py)... "
ORACLE_OUT=$( (cd "${LAB_DIR}" && "${VENV_DIR}/bin/python" -m python.capstone_checks) 2>&1 )
if echo "${ORACLE_OUT}" | grep -q "PASS  T1 ingestion" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T2 schema" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T3 processing" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T4 recovery" && \
   echo "${ORACLE_OUT}" | grep -q "PASS  T5 lag"; then
    echo -e "${GREEN}PASS (All 5 Capstone Acceptance Gates T1-T5 successfully validated)${NC}"
else
    echo -e "${RED}FAIL (Acceptance criteria not met)${NC}"
    echo "${ORACLE_OUT}"
    exit 1
fi

echo ""
echo -e "${GREEN}✔ S11 Kafka & Spark Pipeline Integration Quality Gate: 100% PASS${NC}"
