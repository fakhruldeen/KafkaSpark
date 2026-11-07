"""S03 lab: a keyed, reliable producer with a configurable delivery policy.

Run:  python -m python.s03_producer --acks all --idempotent --orders 500
"""
from __future__ import annotations

import argparse
import sys
from collections import Counter

from confluent_kafka import Producer

from python.common import BOOTSTRAP, TOPIC_ORDERS, encode_json, key_bytes, validate_event
from tools.events import generate


def build_config(args: argparse.Namespace) -> dict:
    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "acks": args.acks,                       # "0", "1" or "all"
        "enable.idempotence": args.idempotent,   # duplicates from retries are dropped by the broker
        "linger.ms": args.linger_ms,             # wait up to this long to fill a batch
        "batch.size": args.batch_bytes,          # maximum bytes per batch per partition
        "compression.type": args.compression,    # none, gzip, snappy, lz4 or zstd
        "delivery.timeout.ms": args.delivery_timeout_ms,
        "client.id": "s03-producer",
    }
    if args.idempotent:
        conf["max.in.flight.requests.per.connection"] = 5   # safe ceiling with idempotence
    return conf


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S03 keyed producer")
    parser.add_argument("--acks", default="all", choices=["0", "1", "all"])
    parser.add_argument("--idempotent", action="store_true", default=True)
    parser.add_argument("--no-idempotent", dest="idempotent", action="store_false")
    parser.add_argument("--linger-ms", type=int, default=5)
    parser.add_argument("--batch-bytes", type=int, default=32_768)
    parser.add_argument("--compression", default="lz4")
    parser.add_argument("--delivery-timeout-ms", type=int, default=30_000)
    parser.add_argument("--orders", type=int, default=500)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args(argv)

    producer = Producer(build_config(args))
    outcome: Counter = Counter()

    def on_delivery(err, msg):
        # Every send ends here, success or failure. Check it. Ignoring it hides data loss.
        outcome["delivered" if err is None else "failed"] += 1
        if err is not None:
            print(f"delivery failed for key={msg.key()}: {err}", file=sys.stderr)

    skipped = 0
    for event in generate(args.orders, args.seed):
        verdict = validate_event(event)
        if not verdict.ok:
            skipped += 1                          # bad records are not sent; S04 shows the DLT path
            continue
        producer.produce(TOPIC_ORDERS, key=key_bytes(event["order_id"]),
                         value=encode_json(event), on_delivery=on_delivery)
        producer.poll(0)                          # serve delivery callbacks while producing

    producer.flush(60)                            # wait until every message is acknowledged
    print(f"sent={sum(outcome.values())} delivered={outcome['delivered']} "
          f"failed={outcome['failed']} skipped_invalid={skipped}")
    return 0 if outcome["failed"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
