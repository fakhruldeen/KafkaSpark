"""Shared event generator for every lab.

Produces a repeatable stream of Nile Retail order events. The same seed always gives the
same events, so every trainee sees identical data and results can be checked.

Injected problems (the lab needs them):
  * late events: event_time is earlier than the wall clock by up to LATE_SECONDS
  * duplicates: a small share of events is sent twice with the same event_id
  * bad records: a small share has no amount, to test data-quality checks
"""
from __future__ import annotations

import json
import random
from datetime import datetime, timedelta, timezone
from typing import Iterator

EVENT_TYPES = ("OrderPlaced", "PaymentAccepted", "Refunded")
STORES = ("CAI-01", "CAI-02", "ALX-01", "ALX-02", "GIZ-01")
BASE_TIME = datetime(2026, 11, 27, 9, 0, tzinfo=timezone.utc)  # a Friday, for the sale story
LATE_SECONDS = 600
DUPLICATE_RATE = 0.01
BAD_RATE = 0.005


def _iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def generate(n_orders: int, seed: int = 42, n_customers: int = 200) -> Iterator[dict]:
    """Yield order events. Each order has a placed event and, most times, a payment event."""
    rng = random.Random(seed)
    clock = BASE_TIME
    for i in range(1, n_orders + 1):
        clock += timedelta(seconds=rng.randint(1, 4))            # orders arrive a few seconds apart
        order_id = f"O{i:06d}"
        customer_id = f"C{rng.randint(1, n_customers):04d}"
        store_id = rng.choice(STORES)
        amount = round(rng.uniform(20, 900), 2)

        for event_type in ("OrderPlaced", "PaymentAccepted"):
            # the payment usually arrives a little after the order
            offset = 0 if event_type == "OrderPlaced" else rng.randint(1, 30)
            event_time = clock + timedelta(seconds=offset)
            # some events are late: their event_time is in the past relative to arrival
            if rng.random() < 0.03:
                event_time -= timedelta(seconds=rng.randint(60, LATE_SECONDS))

            event = {
                "event_id": f"{order_id}-{event_type[:3].lower()}",
                "order_id": order_id,
                "customer_id": customer_id,
                "store_id": store_id,
                "event_type": event_type,
                "amount": amount,
                "event_time": _iso(event_time),
            }
            if rng.random() < BAD_RATE:
                event["amount"] = None                           # bad record for data-quality labs
            yield event

            if rng.random() < DUPLICATE_RATE:
                yield dict(event)                                # same event_id sent again


def write_jsonl(path: str, n_orders: int, seed: int = 42) -> int:
    """Write events to a JSON Lines file (one JSON object per line). Returns the line count."""
    count = 0
    with open(path, "w", encoding="utf-8") as f:
        for event in generate(n_orders, seed):
            f.write(json.dumps(event, ensure_ascii=False) + "\n")
            count += 1
    return count


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate Nile Retail order events.")
    parser.add_argument("--orders", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--out", default="data/orders.jsonl")
    args = parser.parse_args()
    lines = write_jsonl(args.out, args.orders, args.seed)
    print(f"wrote {lines} events to {args.out}")
