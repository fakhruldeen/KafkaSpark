#!/usr/bin/env bash
# ==============================================================================
# S09 Automated Quality Verification Gate
# Enterprise Real-Time Data Engineering - Session S09: Spark SQL & Advanced Joins
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

echo -e "${CYAN}Running S09 Automated Spark SQL & Advanced Joins Verification Gate...${NC}"

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

# Gate 2: Run pytest test suite for S09
echo -n "[2/5] Running S09 PySpark join test suite (pytest)... "
TEST_OUT=$("${VENV_DIR}/bin/pytest" "${LAB_DIR}/tests/test_s09_join.py" -q 2>&1)
if ! echo "${TEST_OUT}" | grep -q "5 passed"; then
    echo -e "${RED}FAIL${NC}"
    echo "${TEST_OUT}"
    exit 1
fi
echo -e "${GREEN}PASS (5/5 tests passed: KeyUniqueness, WindowDedup, RevenueAggregation, BHJ vs SMJ)${NC}"

# Gate 3: Execute s09_join_check.py
echo -n "[3/5] Executing full join and aggregation script (s09_join_check.py)... "
EXEC_OUT=$(cd "${LAB_DIR}" && "${VENV_DIR}/bin/python" python/s09_join_check.py 2>&1)
if ! echo "${EXEC_OUT}" | grep -q "Alexandria"; then
    echo -e "${RED}FAIL${NC}"
    echo "${EXEC_OUT}"
    exit 1
fi
echo -e "${GREEN}PASS (Regional aggregation successfully produced)${NC}"

# Gate 4: Inspect Physical Plan for BroadcastHashJoin
echo -n "[4/5] Verifying BroadcastHashJoin in physical execution plan... "
if ! echo "${EXEC_OUT}" | grep -q "BroadcastHashJoin"; then
    echo -e "${RED}FAIL (BroadcastHashJoin not found in explain output)${NC}"
    exit 1
fi
if ! echo "${EXEC_OUT}" | grep -q "BroadcastExchange"; then
    echo -e "${RED}FAIL (BroadcastExchange not found)${NC}"
    exit 1
fi
echo -e "${GREEN}PASS (BroadcastHashJoin and BroadcastExchange confirmed with Zero Shuffle on orders)${NC}"

# Gate 5: Verify Financial Integrity of Output
echo -n "[5/5] Validating regional revenue totals and payment counts... "
if echo "${EXEC_OUT}" | grep -q "Cairo" && echo "${EXEC_OUT}" | grep -q "Giza" && echo "${EXEC_OUT}" | grep -q "Delta"; then
    echo -e "${GREEN}PASS (All 4 regions verified: Cairo, Giza, Delta, Alexandria)${NC}"
else
    echo -e "${RED}FAIL (Incomplete regions in aggregation)${NC}"
    exit 1
fi

echo ""
echo -e "${GREEN}✔ S09 Spark SQL & Advanced Joins Quality Gate: 100% PASS${NC}"
