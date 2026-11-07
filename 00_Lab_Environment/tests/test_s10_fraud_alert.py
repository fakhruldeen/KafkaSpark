from python.s10_fraud_alert import detect


def txn(cid, minute, amount=100.0):
    return {"customer_id": cid, "event_time": f"2026-11-01T10:{minute:02d}:00Z", "amount": amount}


def test_velocity_alert_fires_on_fourth_transaction_in_window():
    txns = [txn("C1", 0), txn("C1", 1), txn("C1", 2), txn("C1", 3)]
    result = detect(txns)
    rules = [a["rule"] for a in result["alerts"]]
    assert rules == ["R1_velocity"]


def test_old_transactions_leave_the_window():
    txns = [txn("C1", 0), txn("C1", 1), txn("C1", 2), txn("C1", 12)]
    assert detect(txns)["alerts"] == []


def test_volume_alert_on_large_total():
    txns = [txn("C2", 0, 3000.0), txn("C2", 1, 2500.0)]
    alerts = detect(txns)["alerts"]
    assert alerts == [{"customer_id": "C2", "rule": "R2_volume", "at": "2026-11-01T10:01:00Z", "total": 5500.0}]


def test_event_later_than_watermark_is_dropped_and_counted():
    # The watermark is 5 minutes behind the latest event seen. An event 12 minutes late is dropped.
    txns = [txn("C3", 0), txn("C3", 20), txn("C3", 0, 9999.0)]
    result = detect(txns)
    assert result["late_dropped"] == 1
    assert all(a["rule"] != "R2_volume" for a in result["alerts"])


def test_customers_are_independent():
    txns = [txn("C1", 0), txn("C2", 0), txn("C1", 1), txn("C2", 1)]
    assert detect(txns)["alerts"] == []
