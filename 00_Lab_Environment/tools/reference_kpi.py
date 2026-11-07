"""Reference answer for the S10 and S11 labs, computed in plain Python (no Spark).

Revenue = sum of amount for valid PaymentAccepted events, grouped by store and minute of
event_time, after removing duplicate event_ids. Compare these totals with the Spark output.
"""
import json
import sys
from collections import defaultdict
from datetime import datetime


def reference(path: str) -> dict:
    seen = set()
    totals = defaultdict(lambda: [0.0, 0])
    with open(path, encoding="utf-8") as f:
        for line in f:
            e = json.loads(line)
            if e["event_id"] in seen:
                continue
            seen.add(e["event_id"])
            if e["event_type"] != "PaymentAccepted" or e["amount"] is None or e["amount"] <= 0:
                continue
            ts = datetime.strptime(e["event_time"], "%Y-%m-%dT%H:%M:%S.%fZ")
            minute = ts.replace(second=0, microsecond=0).isoformat()
            key = (e["store_id"], minute)
            totals[key][0] += e["amount"]
            totals[key][1] += 1
    return {k: (round(v[0], 2), v[1]) for k, v in totals.items()}


if __name__ == "__main__":
    result = reference(sys.argv[1] if len(sys.argv) > 1 else "data/orders.jsonl")
    print(f"minutes with payments: {len(result)}")
    print(f"total revenue: {round(sum(v[0] for v in result.values()), 2)}")
    print(f"total payments: {sum(v[1] for v in result.values())}")
