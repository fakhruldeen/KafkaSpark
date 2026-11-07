"""S08 lab tests: the batch ETL rules in python/s08_batch.py, run on a tiny local dataset.

Needs PySpark and a Java runtime (Java 17 in Lab 00), but no Kafka and no Docker.
Skipped with a clear reason if PySpark is not installed, so `make test` still runs offline.
"""
import json

import pytest

pytest.importorskip("pyspark")

from python import s08_batch  # noqa: E402


def _event(event_id, event_type="OrderPlaced", amount=10.0, **extra):
    row = {
        "event_id": event_id,
        "order_id": event_id.split("-")[0],
        "customer_id": "C0001",
        "store_id": "CAI-01",
        "event_type": event_type,
        "amount": amount,
        "event_time": "2026-11-27T09:00:00.000000Z",
    }
    row.update(extra)
    return row


@pytest.fixture(scope="module")
def spark():
    session = s08_batch.build_session("s08-tests")
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row) + "\n")
    return str(path)


def test_row_with_missing_or_non_positive_amount_is_quarantined(spark, tmp_path):
    src = _write_jsonl(tmp_path / "in.jsonl", [
        _event("O1-ord"),                      # good
        _event("O2-ord", amount=None),         # missing amount
        _event("O3-ord", amount=-5.0),         # non-positive amount
    ])
    counts = s08_batch.run(spark, src, str(tmp_path / "out"))
    assert counts["clean"] == 1
    assert counts["quarantined"] == 2


def test_unknown_event_type_is_quarantined(spark, tmp_path):
    src = _write_jsonl(tmp_path / "in.jsonl", [
        _event("O1-ord"),
        _event("O2-ord", event_type="Shipped"),   # not one of the three known types
    ])
    counts = s08_batch.run(spark, src, str(tmp_path / "out"))
    assert counts["quarantined"] == 1


def test_duplicate_event_id_is_kept_once(spark, tmp_path):
    src = _write_jsonl(tmp_path / "in.jsonl", [
        _event("O1-ord"),
        _event("O1-ord"),                      # the same event arrives twice
        _event("O1-pay", event_type="PaymentAccepted"),
    ])
    counts = s08_batch.run(spark, src, str(tmp_path / "out"))
    assert counts["clean"] == 2
    assert counts["duplicates_removed"] == 1


def test_counts_always_add_up_to_the_input(spark, tmp_path):
    src = _write_jsonl(tmp_path / "in.jsonl", [
        _event("O1-ord"),
        _event("O1-ord"),                      # duplicate
        _event("O2-ord", amount=None),         # quarantined
        _event("O3-pay", event_type="PaymentAccepted"),
        _event("O4-ord", event_type="Shipped"),  # quarantined
    ])
    counts = s08_batch.run(spark, src, str(tmp_path / "out"))
    assert counts["read"] == 5
    assert counts["read"] == counts["clean"] + counts["quarantined"] + counts["duplicates_removed"]


def test_clean_output_is_partitioned_by_store(spark, tmp_path):
    src = _write_jsonl(tmp_path / "in.jsonl", [
        _event("O1-ord", store_id="CAI-01"),
        _event("O2-ord", store_id="ALX-01"),
    ])
    out = tmp_path / "out"
    s08_batch.run(spark, src, str(out))
    folders = sorted(p.name for p in (out / "orders").iterdir() if p.name.startswith("store_id="))
    assert folders == ["store_id=ALX-01", "store_id=CAI-01"]
    assert (out / "orders" / "_SUCCESS").exists()
