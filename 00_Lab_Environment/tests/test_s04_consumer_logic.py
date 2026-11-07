from python.s04_consumer_logic import process_with_dlt


def test_all_records_succeed_without_retries():
    dlt = []
    counts = process_with_dlt([{"n": 1}, {"n": 2}], lambda r: None, dlt)
    assert counts == {"ok": 2, "dead_lettered": 0, "attempts": 2}
    assert dlt == []


def test_always_failing_record_goes_to_dlt_after_retries():
    dlt = []

    def handler(record):
        if record["amount"] > 850:
            raise RuntimeError("above review limit")

    records = [{"amount": 100}, {"amount": 900}]
    counts = process_with_dlt(records, handler, dlt, retries=2)
    assert counts["ok"] == 1
    assert counts["dead_lettered"] == 1
    assert counts["attempts"] == 1 + 3  # 1 success + 3 tries for the bad record
    assert dlt[0]["record"] == {"amount": 900}
    assert "review limit" in dlt[0]["error"]


def test_transient_failure_succeeds_on_retry():
    dlt = []
    calls = {"n": 0}

    def flaky(record):
        calls["n"] += 1
        if calls["n"] == 1:
            raise TimeoutError("broker busy")

    counts = process_with_dlt([{"id": 1}], flaky, dlt, retries=2)
    assert counts == {"ok": 1, "dead_lettered": 0, "attempts": 2}
    assert dlt == []
