#!/usr/bin/env bash
# ==============================================================================
# S10 Automated Quality Verification Gate
# Enterprise Real-Time Data Engineering - Session S10: Spark Structured Streaming
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

echo -e "${CYAN}Running S10 Automated Spark Structured Streaming Verification Gate...${NC}"

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

# Gate 2: Run pytest test suite for S10 (Fraud alert logic & watermarking oracle)
echo -n "[2/5] Running S10 unit test suite (pytest test_s10_fraud_alert.py)... "
TEST_OUT=$("${VENV_DIR}/bin/pytest" "${LAB_DIR}/tests/test_s10_fraud_alert.py" -q 2>&1)
if ! echo "${TEST_OUT}" | grep -q "5 passed"; then
    echo -e "${RED}FAIL${NC}"
    echo "${TEST_OUT}"
    exit 1
fi
echo -e "${GREEN}PASS (5/5 tests passed: VelocityAlert, WindowExpiration, VolumeAlert, WatermarkDrop, Independence)${NC}"

# Gate 3: Verify Kafka streaming topics
echo -n "[3/5] Verifying Kafka streaming topics (orders.events.v1 & kpi)... "
docker exec kafka1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka1:9092 \
  --create --topic kpi --partitions 3 --replication-factor 3 --if-not-exists >/dev/null 2>&1

TOPIC_LIST=$(docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:9092 --list 2>&1)
if echo "${TOPIC_LIST}" | grep -q "orders.events.v1" && echo "${TOPIC_LIST}" | grep -q "kpi"; then
    echo -e "${GREEN}PASS (Base stream 'orders.events.v1' and sink topic 'kpi' ready)${NC}"
else
    echo -e "${RED}FAIL (Missing streaming topics)${NC}"
    exit 1
fi

# Gate 4: Test Spark Structured Streaming Engine (Rate stream with transformations)
echo -n "[4/5] Executing live Structured Streaming query with transformations... "
CKPT_DIR="${LAB_DIR}/data/checkpoints/gate4_test"
rm -rf "${CKPT_DIR}"
mkdir -p "${CKPT_DIR}"

STREAM_TEST=$("${VENV_DIR}/bin/python" -c '
import shutil, os, time
from pyspark.sql import SparkSession, functions as F

ckpt = "'"${CKPT_DIR}"'"
spark = (SparkSession.builder.master("local[2]").appName("s10-verify")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
transformed = rate_df.withColumn("doubled", F.col("value") * 2)

query = (transformed.writeStream
         .format("memory")
         .queryName("verify_stream")
         .option("checkpointLocation", ckpt)
         .start())

time.sleep(2)
count = spark.sql("SELECT count(*) as total FROM verify_stream").collect()[0]["total"]
query.stop()
spark.stop()
print(f"ROWS_STREAMED:{count}")
' 2>&1)
rm -rf "${CKPT_DIR}"

if echo "${STREAM_TEST}" | grep -q "ROWS_STREAMED:" && ! echo "${STREAM_TEST}" | grep -q "ROWS_STREAMED:0"; then
    echo -e "${GREEN}PASS (Structured Streaming micro-batch query produced active records)${NC}"
else
    echo -e "${RED}FAIL (Streaming query failed)${NC}"
    echo "${STREAM_TEST}"
    exit 1
fi

# Gate 5: Validate Checkpointing and Offset Commit Mechanics
echo -n "[5/5] Validating Checkpoint directory structure and WAL integrity... "
CHECKPOINT_DIR="${LAB_DIR}/data/checkpoints/verify_s10"
rm -rf "${CHECKPOINT_DIR}"
mkdir -p "${CHECKPOINT_DIR}"

CKPT_TEST=$("${VENV_DIR}/bin/python" -c '
import os, time
from pyspark.sql import SparkSession

ckpt = "'"${CHECKPOINT_DIR}"'"
spark = (SparkSession.builder.master("local[2]").appName("s10-ckpt")
         .config("spark.sql.shuffle.partitions", "2")
         .config("spark.sql.ansi.enabled", "false").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")

rate_df = spark.readStream.format("rate").option("rowsPerSecond", 5).load()
query = (rate_df.writeStream
         .format("memory")
         .queryName("ckpt_stream")
         .option("checkpointLocation", ckpt)
         .start())

time.sleep(2)
query.stop()
spark.stop()

# Check offsets and commits
has_offsets = os.path.exists(os.path.join(ckpt, "offsets"))
has_commits = os.path.exists(os.path.join(ckpt, "commits"))
print(f"CKPT_OK:{has_offsets and has_commits}")
' 2>&1)
rm -rf "${CHECKPOINT_DIR}"

if echo "${CKPT_TEST}" | grep -q "CKPT_OK:True"; then
    echo -e "${GREEN}PASS (Write-Ahead Log offsets and commits verified)${NC}"
else
    echo -e "${RED}FAIL (Checkpoint WAL verification failed)${NC}"
    echo "${CKPT_TEST}"
    exit 1
fi

echo ""
echo -e "${GREEN}✔ S10 Spark Structured Streaming Quality Gate: 100% PASS${NC}"
