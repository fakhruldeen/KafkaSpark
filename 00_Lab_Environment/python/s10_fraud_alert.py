"""S10 stretch challenge: money-laundering style alert rules, reference implementation in plain Python.

Rules (illustrative, not a compliance rule set):
  R1 velocity: more than MAX_TXN transactions by one customer in a sliding window of WINDOW_MIN minutes.
  R2 volume:   total amount above AMOUNT_LIMIT for one customer in the same window.
Late events beyond the watermark are dropped and counted, exactly as Spark does with withWatermark.

Trainees port these rules to PySpark Structured Streaming in the challenge. This file is the answer key
for the logic and the test oracle. Tested offline in tests/test_s10_fraud_alert.py.
"""
from __future__ import annotations

from datetime import datetime, timedelta

WINDOW_MIN = 10
MAX_TXN = 3
AMOUNT_LIMIT = 5000.0
WATERMARK_MIN = 5


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def detect(transactions: list[dict]) -> dict:
    """Process transactions in arrival order. Returns alerts and the count of late events dropped."""
    window = timedelta(minutes=WINDOW_MIN)
    watermark_delay = timedelta(minutes=WATERMARK_MIN)
    max_seen: datetime | None = None
    history: dict[str, list[tuple[datetime, float]]] = {}
    alerts: list[dict] = []
    late = 0

    for txn in transactions:
        when = _ts(txn["event_time"])
        if max_seen is None or when > max_seen:
            max_seen = when
        watermark = max_seen - watermark_delay
        if when < watermark:  # too late: dropped from stateful processing
            late += 1
            continue
        customer = txn["customer_id"]
        rows = history.setdefault(customer, [])
        rows.append((when, float(txn["amount"])))
        rows[:] = [(t, a) for t, a in rows if t > when - window]  # keep only the sliding window
        count = len(rows)
        total = sum(a for _, a in rows)
        if count > MAX_TXN:
            alerts.append({"customer_id": customer, "rule": "R1_velocity", "at": txn["event_time"], "count": count})
        if total > AMOUNT_LIMIT:
            alerts.append({"customer_id": customer, "rule": "R2_volume", "at": txn["event_time"], "total": round(total, 2)})
    return {"alerts": alerts, "late_dropped": late}
