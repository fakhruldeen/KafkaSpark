#!/usr/bin/env bash
# ==============================================================================
# Workstation Pre-flight Verification Script
# Course: Apache Kafka Core, Confluent Developer & Apache Spark
# ==============================================================================
set -e

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo "=================================================="
echo " Workstation Readiness & Pre-flight Diagnostics"
echo "=================================================="

# 1. Check OS & CPU
echo -n "[1/7] Checking CPU Cores... "
CORES=$(nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 1)
if [ "$CORES" -ge 4 ]; then
    echo -e "${GREEN}PASS ($CORES cores detected)${NC}"
else
    echo -e "${YELLOW}WARN ($CORES cores detected; minimum 4 recommended)${NC}"
fi

# 2. Check RAM
echo -n "[2/7] Checking System Memory (RAM)... "
if command -v free >/dev/null 2>&1; then
    TOTAL_RAM_KB=$(grep MemTotal /proc/meminfo | awk '{print $2}')
    TOTAL_RAM_GB=$((TOTAL_RAM_KB / 1024 / 1024))
    if [ "$TOTAL_RAM_GB" -ge 14 ]; then
        echo -e "${GREEN}PASS (~$TOTAL_RAM_GB GB RAM)${NC}"
    else
        echo -e "${YELLOW}WARN (~$TOTAL_RAM_GB GB RAM; 16 GB recommended)${NC}"
    fi
else
    echo -e "${GREEN}PASS (RAM check skipped on non-Linux)${NC}"
fi

# 3. Check Docker & Compose
echo -n "[3/7] Checking Docker Engine & Compose v2... "
if command -v docker >/dev/null 2>&1; then
    DOCKER_VER=$(docker --version | awk '{print $3}' | tr -d ',')
    if docker compose version >/dev/null 2>&1; then
        COMPOSE_VER=$(docker compose version --short 2>/dev/null || echo "v2")
        echo -e "${GREEN}PASS (Docker $DOCKER_VER, Compose $COMPOSE_VER)${NC}"
    else
        echo -e "${RED}FAIL (Docker installed, but Compose v2 plugin missing)${NC}"
        exit 1
    fi
else
    echo -e "${RED}FAIL (Docker not found in PATH)${NC}"
    exit 1
fi

# 4. Check Python 3
echo -n "[4/7] Checking Python 3 Runtime... "
if command -v python3 >/dev/null 2>&1; then
    PY_VER=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
    PY_MAJ=$(echo "$PY_VER" | cut -d. -f1)
    PY_MIN=$(echo "$PY_VER" | cut -d. -f2)
    if [ "$PY_MAJ" -eq 3 ] && [ "$PY_MIN" -ge 10 ]; then
        echo -e "${GREEN}PASS (Python $PY_VER)${NC}"
    else
        echo -e "${YELLOW}WARN (Python $PY_VER detected; 3.10+ recommended)${NC}"
    fi
else
    echo -e "${RED}FAIL (python3 not found)${NC}"
    exit 1
fi

# 5. Check Java JDK
echo -n "[5/7] Checking Java JDK (Required for Spark)... "
if command -v java >/dev/null 2>&1; then
    JAVA_VER=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}' | cut -d. -f1)
    if [ "$JAVA_VER" -eq 17 ] || [ "$JAVA_VER" -eq 11 ]; then
        echo -e "${GREEN}PASS (Java $JAVA_VER LTS)${NC}"
    elif [ "$JAVA_VER" -eq 21 ]; then
        echo -e "${GREEN}PASS (Java 21 LTS - experimental in Spark 3.5)${NC}"
    elif [ "$JAVA_VER" -gt 21 ] 2>/dev/null; then
        echo -e "${YELLOW}WARN (Java $JAVA_VER detected; Apache Spark 3.5 requires OpenJDK 17 LTS. JDK >21 removes Subject.getSubject)${NC}"
    else
        echo -e "${YELLOW}WARN (Java $JAVA_VER detected; OpenJDK 17 LTS recommended)${NC}"
    fi
else
    echo -e "${RED}FAIL (Java JDK not found in PATH; install openjdk-17-jdk-headless)${NC}"
    exit 1
fi

# 6. Check Git
echo -n "[6/7] Checking Git... "
if command -v git >/dev/null 2>&1; then
    echo -e "${GREEN}PASS ($(git --version))${NC}"
else
    echo -e "${RED}FAIL (git not found)${NC}"
    exit 1
fi

# 7. Check Port Conflicts
echo -n "[7/7] Checking Port Availability (9092, 8080, 8081, 5432)... "
PORTS_TO_CHECK=(9092 8080 8081 5432)
CONFLICTS=()
for P in "${PORTS_TO_CHECK[@]}"; do
    if command -v ss >/dev/null 2>&1; then
        if ss -tuln | grep -q ":$P "; then
            # Check if occupied by our own docker
            if ! docker ps --format '{{.Ports}}' | grep -q "$P"; then
                CONFLICTS+=("$P")
            fi
        fi
    fi
done

if [ ${#CONFLICTS[@]} -eq 0 ]; then
    echo -e "${GREEN}PASS (Ports available)${NC}"
else
    echo -e "${YELLOW}WARN (Port(s) occupied by non-lab services: ${CONFLICTS[*]})${NC}"
fi

echo "=================================================="
echo -e "${GREEN}★ WORKSTATION PRE-FLIGHT CHECK COMPLETE! ★${NC}"
echo "=================================================="
