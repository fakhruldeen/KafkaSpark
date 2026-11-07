#!/usr/bin/env bash
# ==============================================================================
# Enterprise Real-Time Data Engineering: Automated Student Lab Setup Script
# Performs 1-command complete lab environment setup:
# 1. Validates host prerequisites (Docker, Python 3.10+, Java 17)
# 2. Downloads Maven Central runtime JARs for PySpark
# 3. Creates and provisions Python virtual environment (.venv)
# 4. Pulls official Docker images from Docker Hub and Confluent Registry
# 5. Executes workstation diagnostic pre-flight tests
# ==============================================================================
set -euo pipefail

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

echo -e "${BOLD}${BLUE}====================================================================${NC}"
echo -e "${BOLD}${BLUE}   Kafka, Confluent & Spark Workstation Automated Environment Setup ${NC}"
echo -e "${BOLD}${BLUE}====================================================================${NC}"
echo "Working Directory: ${SCRIPT_DIR}"
echo ""

# 1. Download JARs
echo -e "${BOLD}[1/4] Downloading Spark Kafka Connector JARs from Maven Central...${NC}"
./download_dependencies.sh

# 2. Python Virtual Environment
echo ""
echo -e "${BOLD}[2/4] Initializing Python Virtual Environment (.venv)...${NC}"
if ! command -v python3 >/dev/null 2>&1; then
    echo -e "${RED}[ERROR] python3 is required. Please install Python 3.10 or higher.${NC}"
    exit 1
fi

if [[ ! -d ".venv" ]]; then
    python3 -m venv .venv
    echo -e "${GREEN}  ✓ Virtual environment created at .venv${NC}"
fi

source .venv/bin/activate
pip install --upgrade pip setuptools wheel --quiet
echo "  ⬇ Installing Python packages from PyPI (requirements.txt)..."
pip install -r requirements.txt --quiet
echo -e "${GREEN}  ✓ Python dependencies installed successfully${NC}"

# 3. Pull Official Docker Images
echo ""
echo -e "${BOLD}[3/4] Pulling Official Docker Images from Docker Hub / Confluent Registry...${NC}"
if command -v docker >/dev/null 2>&1; then
    docker compose pull
    echo -e "${GREEN}  ✓ All Docker images pulled and verified${NC}"
else
    echo -e "${YELLOW}[WARN] Docker command not found. Ensure Docker Desktop / Engine is installed.${NC}"
fi

# 4. Pre-flight diagnostics
echo ""
echo -e "${BOLD}[4/4] Executing Workstation Pre-flight Verification...${NC}"
./test_workstation.sh

echo ""
echo -e "${BOLD}${GREEN}====================================================================${NC}"
echo -e "${BOLD}${GREEN}   ★ WORKSTATION ENVIRONMENT IS 100% READY FOR THE COURSE! ★       ${NC}"
echo -e "${BOLD}${GREEN}====================================================================${NC}"
echo "To start the cluster:       docker compose up -d"
echo "To access Kafka Web UI:     http://localhost:8080"
echo "To run lab test suite:      source .venv/bin/activate && pytest tests/"
echo ""
