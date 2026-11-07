"""S04 lab: a consumer that commits only after successful processing, with a dead-letter topic.

Run:  python -m python.s04_consumer --group notifications --max-messages 500
"""
from __future__ import annotations

import argparse
import signal
import sys
from collections import Counter

from confluent_kafka import Consumer, KafkaError, Producer

from python.common import BOOTSTRAP, TOPIC_DLT, TOPIC_ORDERS, decode_json, validate_event

running = True


def _stop(signum, frame):  # Ctrl+C asks for a clean shutdown: finish the batch, commit, close
    global running
    running = False


signal.signal(signal.SIGINT, _stop)


def process(event: dict) -> None:
    """Stand-in for the real work (sending an email). Raise an error to simulate a failure."""
    if event.get("amount") is not None and event["amount"] > 850:
        raise RuntimeError("amount above review limit (850): simulated downstream failure")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S04 consumer with manual commit and DLT")
    parser.add_argument("--group", default="notifications")
    parser.add_argument("--client-id", default=None, help="Optional client identifier for consumer instance")
    parser.add_argument("--reset", default="earliest", choices=["earliest", "latest"])
    parser.add_argument("--max-messages", type=int, default=0, help="0 means run until Ctrl+C")
    parser.add_argument("--retries", type=int, default=2)
    args = parser.parse_args(argv)

    conf = {
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.group,
        "auto.offset.reset": args.reset,
        "enable.auto.commit": False,              # we commit ourselves, after processing
        "partition.assignment.strategy": "cooperative-sticky",
    }
    if args.client_id:
        conf["client.id"] = args.client_id

    consumer = Consumer(conf)
    dlt = Producer({"bootstrap.servers": BOOTSTRAP, "acks": "all", "enable.idempotence": True})

    def on_assign(c, partitions):
        assigned = sorted([p.partition for p in partitions])
        worker_label = f"Worker {args.client_id} " if args.client_id else "Consumer "
        print(f"{worker_label}assigned partitions: {assigned}", flush=True)
        c.assign(partitions)

    consumer.subscribe([TOPIC_ORDERS], on_assign=on_assign)
    stats: Counter = Counter()

    try:
        while running and (args.max_messages == 0 or sum(stats.values()) < args.max_messages):
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print(msg.error(), file=sys.stderr)
                continue

            event = decode_json(msg.value())
            verdict = validate_event(event)
            if not verdict.ok:
                stats["invalid"] += 1
                # Invalid data will never succeed on retry, so it goes straight to the DLT.
                dlt.produce(TOPIC_DLT, key=msg.key(), value=msg.value(),
                            headers=[("error", verdict.reason.encode()),
                                     ("source-partition", str(msg.partition()).encode()),
                                     ("source-offset", str(msg.offset()).encode())])
            else:
                for attempt in range(1, args.retries + 2):
                    try:
                        process(event)
                        stats["processed"] += 1
                        break
                    except RuntimeError as error:
                        if attempt > args.retries:          # retries used up: park the record
                            stats["dead-lettered"] += 1
                            dlt.produce(TOPIC_DLT, key=msg.key(), value=msg.value(),
                                        headers=[("error", str(error).encode())])
                            break
            dlt.poll(0)
            # Commit the NEXT offset only after the record is handled (processed or parked).
            consumer.commit(message=msg, asynchronous=False)
    finally:
        dlt.flush(30)
        consumer.close()                          # leaves the group cleanly, partitions are reassigned
        print(dict(stats))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
