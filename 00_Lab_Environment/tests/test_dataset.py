"""Offline checks: the seed-42 dataset must reproduce the numbers quoted in the lab documents.

If one of these fails, a lab document is wrong or the data file changed. Fix the source of truth first.
Runs with `make test`, no Docker and no Spark needed.
"""
import json
import pathlib

from python.common import dedupe, validate_event
from tools.reference_kpi import reference

DATA = pathlib.Path(__file__).resolve().parent.parent / "data" / "orders.jsonl"


def _rows():
    return [json.loads(line) for line in DATA.open(encoding="utf-8")]


def test_raw_line_count_is_4033():
    assert len(_rows()) == 4033


def test_validation_rejects_23_and_keeps_4010():
    valid = [r for r in _rows() if validate_event(r).ok]
    assert len(valid) == 4010
    assert len(_rows()) - len(valid) == 23


def test_dedupe_removes_33_and_leaves_3977():
    valid = [r for r in _rows() if validate_event(r).ok]
    unique, removed = dedupe(valid)
    assert (len(unique), removed) == (3977, 33)


def test_payments_after_dedupe_are_1985():
    valid = [r for r in _rows() if validate_event(r).ok]
    unique, _ = dedupe(valid)
    assert sum(1 for r in unique if r["event_type"] == "PaymentAccepted") == 1985


def test_reference_kpi_matches_lab_values():
    result = reference(str(DATA))
    assert len(result) == 415
    assert round(sum(v[0] for v in result.values()), 2) == 900163.5
    assert sum(v[1] for v in result.values()) == 1985
