"""S04 lab helper: retry a handler, then route the record to a dead-letter list.

Pure Python. The real consumer (python/s04_consumer.py) calls the same pattern with Kafka.
Tested offline in tests/test_s04_consumer_logic.py.
"""
from __future__ import annotations

from typing import Callable, Iterable


def process_with_dlt(
    records: Iterable[dict],
    handler: Callable[[dict], None],
    dead_letters: list[dict],
    retries: int = 2,
) -> dict:
    """Run handler on each record. Retry up to `retries` extra times, then dead-letter it.

    Returns counts: {"ok": n, "dead_lettered": n, "attempts": n}.
    A record is never silently dropped: it is either processed or appended to dead_letters with the error.
    """
    counts = {"ok": 0, "dead_lettered": 0, "attempts": 0}
    for record in records:
        last_error = None
        for _attempt in range(retries + 1):
            counts["attempts"] += 1
            try:
                handler(record)
                counts["ok"] += 1
                last_error = None
                break
            except Exception as exc:  # the handler's own errors are data, not crashes
                last_error = exc
        if last_error is not None:
            dead_letters.append({"record": record, "error": str(last_error)})
            counts["dead_lettered"] += 1
    return counts
