#!/usr/bin/env bash
# ==============================================================================
# Enterprise Real-Time Data Engineering: Spark Kafka JARs Downloader
# Downloads required runtime JARs directly from Maven Central
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JARS_DIR="${SCRIPT_DIR}/jars"
mkdir -p "${JARS_DIR}"

MAVEN_BASE="https://repo1.maven.org/maven2"

declare -A JARS=(
  ["spark-sql-kafka-0-10_2.12-3.5.1.jar"]="${MAVEN_BASE}/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar"
  ["spark-token-provider-kafka-0-10_2.12-3.5.1.jar"]="${MAVEN_BASE}/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar"
  ["kafka-clients-3.4.1.jar"]="${MAVEN_BASE}/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar"
  ["commons-pool2-2.11.1.jar"]="${MAVEN_BASE}/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
)

echo "===================================================================="
echo " Downloading Spark Structured Streaming Kafka JARs from Maven Central"
echo "===================================================================="

for jar in "${!JARS[@]}"; do
  dest="${JARS_DIR}/${jar}"
  if [[ -f "${dest}" && -s "${dest}" ]]; then
    echo "  ✓ Already present: ${jar}"
  else
    url="${JARS[$jar]}"
    echo "  ⬇ Downloading ${jar}..."
    curl -fsSL --retry 3 --connect-timeout 10 -o "${dest}" "${url}"
    echo "  ✓ Downloaded ${jar}"
  fi
done

echo ""
echo "All Spark Kafka dependencies are staged in: ${JARS_DIR}"
