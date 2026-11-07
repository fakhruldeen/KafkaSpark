"""S03 lab helper: turn an order event into the (key, value) bytes a producer sends.

Pure Python. No broker is needed, so this file is tested offline (tests/test_s03_producer_logic.py).
"""
from __future__ import annotations

from python.common import encode_json, key_bytes, validate_event


def to_record(event: dict) -> tuple[bytes, bytes]:
    """Validate the event, then return (key, value).

    The key is the order_id, so every event of one order lands in one partition and keeps its order.
    Invalid events raise ValueError before anything is sent; the producer counts them as skipped_invalid.
    """
    verdict = validate_event(event)
    if not verdict.ok:
        raise ValueError(verdict.reason)
    return key_bytes(event["order_id"]), encode_json(event)


class IdempotentBrokerTracker:
    """Simulates Kafka broker-side Producer ID (PID) & sequence number deduplication per partition."""

    def __init__(self) -> None:
        self._state: dict[tuple[int, int], int] = {}  # (pid, partition) -> last_sequence

    def process_batch(self, pid: int, partition: int, base_sequence: int, record_count: int = 1) -> tuple[bool, str]:
        """Verify sequence monotonic increment and reject duplicates."""
        key = (pid, partition)
        last_seq = self._state.get(key, -1)

        # Duplicate delivery attempt (network retry after commit)
        if base_sequence <= last_seq:
            return False, f"DUPLICATE_IGNORED: seq {base_sequence} <= last committed {last_seq}"

        # Monotonic sequential ordering expected
        if base_sequence == last_seq + 1:
            self._state[key] = base_sequence + record_count - 1
            return True, f"COMMITTED: new last sequence is {self._state[key]}"

        return False, f"OUT_OF_ORDER: expected {last_seq + 1}, received {base_sequence}"
