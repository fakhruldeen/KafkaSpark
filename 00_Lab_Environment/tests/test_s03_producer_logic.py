import pytest

from python.common import decode_json
from python.s03_producer_logic import to_record


def event(**overrides):
    base = {"event_id": "e1", "order_id": "O1", "customer_id": "C1",
            "event_type": "OrderPlaced", "event_time": "2026-11-01T10:00:00Z", "amount": 420}
    base.update(overrides)
    return base


def test_valid_event_becomes_key_and_value():
    key, value = to_record(event())
    assert key == b"O1"
    assert decode_json(value)["amount"] == 420


def test_same_order_gives_same_key():
    k1, _ = to_record(event(event_id="e1", event_type="OrderPlaced"))
    k2, _ = to_record(event(event_id="e2", event_type="PaymentAccepted"))
    assert k1 == k2


def test_missing_order_id_is_rejected():
    with pytest.raises(ValueError, match="missing order_id"):
        to_record(event(order_id=""))


@pytest.mark.parametrize("amount", [0, -5, None])
def test_bad_amount_is_rejected(amount):
    with pytest.raises(ValueError, match="amount"):
        to_record(event(amount=amount))


def test_unknown_type_is_rejected():
    with pytest.raises(ValueError, match="unknown event_type"):
        to_record(event(event_type="Teleported"))


def test_idempotent_broker_accepts_first_write():
    from python.s03_producer_logic import IdempotentBrokerTracker
    tracker = IdempotentBrokerTracker()
    ok, msg = tracker.process_batch(pid=1001, partition=0, base_sequence=0, record_count=1)
    assert ok is True
    assert "COMMITTED" in msg


def test_idempotent_broker_rejects_retry_duplicate():
    from python.s03_producer_logic import IdempotentBrokerTracker
    tracker = IdempotentBrokerTracker()
    # Batch 0 succeeds
    ok1, _ = tracker.process_batch(pid=1001, partition=0, base_sequence=0, record_count=1)
    assert ok1 is True
    # Producer resends Batch 0 due to transient ACK network drop
    ok2, msg2 = tracker.process_batch(pid=1001, partition=0, base_sequence=0, record_count=1)
    assert ok2 is False
    assert "DUPLICATE_IGNORED" in msg2


def test_idempotent_broker_rejects_out_of_order_sequence():
    from python.s03_producer_logic import IdempotentBrokerTracker
    tracker = IdempotentBrokerTracker()
    # Expecting seq 0, but receiving seq 5
    ok, msg = tracker.process_batch(pid=1001, partition=0, base_sequence=5, record_count=1)
    assert ok is False
    assert "OUT_OF_ORDER" in msg
