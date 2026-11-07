#!/usr/bin/env bash
# Create every topic the labs use. Auto-creation is off, so each topic is created on purpose.
set -euo pipefail
BROKER=kafka1:9092
run() { docker compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" "$@"; }

create() {  # create NAME PARTITIONS
  run --create --if-not-exists --topic "$1" --partitions "$2" --replication-factor 3 \
      --config min.insync.replicas=2
}

create orders.events.v1 3          # raw order events, keyed by order_id
create orders.events.v1.dlt 1      # dead-letter topic for invalid records (S04, S11)
create orders.staging.v1 1         # S06: Connect source writes here, the SMT renames it
create orders.copy.v1 1            # S06: renamed copy, read by the Connect sink
create orders.connect.dlq 1        # S06: Connect error topic
create orders.kpi.minute.v1 3      # S10, S11: curated KPI output
create orders.avro.v1 3            # S05: Avro events with Schema Registry (separate from the JSON topic)
create customers.v1 3              # S07: customer table for the ksqlDB join (key = customer_id)
echo "topics ready"
run --list
