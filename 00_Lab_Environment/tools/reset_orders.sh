#!/usr/bin/env bash
# Empty the orders topics so the next run starts from a known state.
# Use it between labs. Counts in the lab documents assume one baseline run after a reset.
set -euo pipefail
BROKER=kafka1:9092
run() { docker compose exec -T kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server "$BROKER" "$@"; }

for t in orders.events.v1 orders.events.v1.dlt; do
  run --delete --if-exists --topic "$t" || true
done
sleep 3   # give the controller time to finish the delete before recreating
bash "$(dirname "$0")/create_topics.sh" > /dev/null
echo "orders topics reset: $(run --describe --topic orders.events.v1 | head -1)"
