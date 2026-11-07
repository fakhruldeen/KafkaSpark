from python.common import validate_event, dedupe, key_bytes, encode_json, decode_json


def good(**kw):
    e = {"event_id": "O1-ord", "order_id": "O1", "customer_id": "C1",
         "event_type": "OrderPlaced", "event_time": "2026-11-27T09:00:00Z", "amount": 10.0}
    e.update(kw)
    return e


def test_valid_event_passes():
    assert validate_event(good()).ok


def test_missing_amount_fails():
    v = validate_event(good(amount=None))
    assert not v.ok and "amount" in v.reason


def test_unknown_type_fails():
    assert not validate_event(good(event_type="Shipped")).ok


def test_dedupe_keeps_first_and_counts_removed():
    events = [good(), good(), good(event_id="O2-ord")]
    unique, removed = dedupe(events)
    assert len(unique) == 2 and removed == 1


def test_same_order_same_key_bytes():
    assert key_bytes("O1") == key_bytes("O1")


def test_json_roundtrip():
    e = good()
    assert decode_json(encode_json(e)) == e
