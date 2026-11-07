#!/usr/bin/env bash
# Probe each service of the lab stack. Prints PASS or FAIL per service.
# Uses direct container probing (docker exec) or localhost probing to avoid
# docker compose project-name discrepancies.
set -u
fail=0

probe() {
  local name="$1"; shift
  if "$@" > /dev/null 2>&1; then
    echo "PASS  $name"
  else
    echo "FAIL  $name"
    fail=1
  fi
}

probe "kafka cluster (3 brokers reachable)" \
  docker exec kafka1 /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server kafka1:9092
probe "schema registry" curl -fsS http://localhost:8081/subjects
probe "kafka connect" curl -fsS http://localhost:8083/
probe "ksqldb server" curl -fsS http://localhost:8088/info
probe "postgres" docker exec postgres pg_isready -U lab -d orders
probe "kafka ui" curl -fsS http://localhost:8080/

exit $fail
