#!/usr/bin/env bash
# ==============================================================================
# Course: Enterprise Real-Time Data Engineering
# Instructor: Mohamed Fouad Fakhruldeen (Fakhr.me)
# Session: S08 - Apache Spark Architecture & PySpark DataFrames
# Verification Gate Script: verify_s08.sh
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "${SCRIPT_DIR}/../../00_Lab" && pwd)"

GREEN='\033[0;32m'
RED='\033[0;31m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# Source environment
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
export PYSPARK_PYTHON="${LAB_DIR}/.venv/bin/python"
export PYSPARK_DRIVER_PYTHON="${LAB_DIR}/.venv/bin/python"

PYTHON_BIN="${LAB_DIR}/.venv/bin/python"
[ -f "${PYTHON_BIN}" ] || PYTHON_BIN="python3"

echo -e "${BOLD}${CYAN}Running S08 Automated Apache Spark & PySpark Verification Gate...${NC}"

# 1. Probe Java 17 and PySpark Runtime
echo -n "[1/5] Probing Java 17 runtime & PySpark installation... "
JAVA_VER=$("${JAVA_HOME}/bin/java" -version 2>&1 | head -n 1)
SPARK_VER=$("${PYTHON_BIN}" -c "import pyspark; print(pyspark.__version__)")
if echo "${JAVA_VER}" | grep -q "17\." && [ -n "${SPARK_VER}" ]; then
  echo -e "${GREEN}PASS (Java 17 Temurin active, PySpark ${SPARK_VER} loaded)${NC}"
else
  echo -e "${RED}FAIL (Java: ${JAVA_VER}, PySpark: ${SPARK_VER})${NC}"
  exit 1
fi

# 2. SparkSession Creation & Execution Check
echo -n "[2/5] Initializing SparkSession and Catalyst Engine... "
SESSION_OUT=$("${PYTHON_BIN}" -c "
from pyspark.sql import SparkSession
spark = SparkSession.builder.master('local[1]').appName('s08-verify').config('spark.sql.shuffle.partitions', '4').getOrCreate()
df = spark.createDataFrame([(1, 'OK')], ['id', 'status'])
assert df.filter('id = 1').count() == 1
print('INITIALIZED')
spark.stop()
" 2>&1)

if echo "${SESSION_OUT}" | grep -q "INITIALIZED"; then
  echo -e "${GREEN}PASS (SparkSession created, Catalyst in-memory execution verified)${NC}"
else
  echo -e "${RED}FAIL (SparkSession initialization failed)${NC}"
  echo "${SESSION_OUT}"
  exit 1
fi

# 3. PySpark Batch Rule Unit Tests
echo -n "[3/5] Running S08 PySpark batch test suite (pytest)... "
TEST_OUT=$(cd "${LAB_DIR}" && "${PYTHON_BIN}" -m pytest -q tests/test_s08_batch.py 2>&1)
if echo "${TEST_OUT}" | grep -q "5 passed"; then
  echo -e "${GREEN}PASS (5/5 tests passed: Quarantine, Schema, Deduplication, Balance, Partitioning)${NC}"
else
  echo -e "${RED}FAIL (PySpark unit tests failed)${NC}"
  echo "${TEST_OUT}"
  exit 1
fi

# 4. Live End-to-End Batch ETL Run on orders.jsonl
echo -n "[4/5] Executing full batch ETL on data/orders.jsonl... "
rm -rf "${LAB_DIR}/data/curated"
ETL_OUT=$(cd "${LAB_DIR}" && "${PYTHON_BIN}" -m python.s08_batch --input data/orders.jsonl --output data/curated 2>&1)

if echo "${ETL_OUT}" | grep -q "'clean':" && echo "${ETL_OUT}" | grep -q "'quarantined':"; then
  CLEAN_COUNT=$(echo "${ETL_OUT}" | grep -o "'clean': [0-9]*" | awk '{print $2}')
  QUAR_COUNT=$(echo "${ETL_OUT}" | grep -o "'quarantined': [0-9]*" | awk '{print $2}')
  echo -e "${GREEN}PASS (Clean records: ${CLEAN_COUNT}, Quarantined: ${QUAR_COUNT})${NC}"
else
  echo -e "${RED}FAIL (Batch ETL execution failed)${NC}"
  echo "${ETL_OUT}"
  exit 1
fi

# 5. Output Schema & Parquet Partition Inspection
echo -n "[5/5] Inspecting partitioned Parquet structure and quarantine folder... "
if [ -f "${LAB_DIR}/data/curated/orders/_SUCCESS" ] && [ -d "${LAB_DIR}/data/curated/orders/store_id=CAI-01" ] && [ -f "${LAB_DIR}/data/curated/quarantine/_SUCCESS" ]; then
  STORE_COUNT=$(find "${LAB_DIR}/data/curated/orders" -maxdepth 1 -type d -name "store_id=*" | wc -l)
  echo -e "${GREEN}PASS (_SUCCESS present, ${STORE_COUNT} store partitions found, quarantine verified)${NC}"
else
  echo -e "${RED}FAIL (Missing _SUCCESS or store_id partition folders)${NC}"
  exit 1
fi

echo -e "\n${BOLD}${GREEN}✔ S08 Apache Spark & PySpark DataFrames Quality Gate: 100% PASS${NC}"
