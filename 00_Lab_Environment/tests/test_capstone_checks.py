"""Unit tests for the capstone acceptance tests T1 to T5 and the report function."""
from python.capstone_checks import report, t1_ingestion, t2_schema, t3_processing, t4_recovery, t5_lag


def test_t1_needs_4000_and_no_errors():
    assert t1_ingestion(sent=4010, send_errors=0)[0] is True
    assert t1_ingestion(sent=3999, send_errors=0)[0] is False
    assert t1_ingestion(sent=4010, send_errors=1)[0] is False


def test_t2_needs_at_least_one_routed_to_dlt():
    assert t2_schema(total=4033, invalid_routed_to_dlt=23)[0] is True
    assert t2_schema(total=4033, invalid_routed_to_dlt=0)[0] is False


def test_t3_exact_match_by_default():
    assert t3_processing(spark_rows=1985, reference_rows=1985)[0] is True
    assert t3_processing(spark_rows=1984, reference_rows=1985)[0] is False


def test_t4_gaps_must_be_zero_and_duplicates_documented():
    assert t4_recovery(gaps=0, duplicates=5, documented_duplicates=5)[0] is True
    assert t4_recovery(gaps=0, duplicates=6, documented_duplicates=5)[0] is False
    assert t4_recovery(gaps=1, duplicates=0, documented_duplicates=0)[0] is False


def test_t5_lag_must_be_zero():
    assert t5_lag(lag_after_60s=0)[0] is True
    assert t5_lag(lag_after_60s=3)[0] is False


def test_report_returns_zero_only_when_all_pass(capsys):
    good = {"T5 lag": t5_lag(0)}
    bad = {"T5 lag": t5_lag(3)}
    assert report(good) == 0
    assert report(bad) == 1
    assert "FAIL" in capsys.readouterr().out


def test_main_missing_metrics_fails_without_fallback(monkeypatch, capsys):
    from python.capstone_checks import main
    monkeypatch.setattr("sys.argv", ["capstone_checks.py"])
    rc = main()
    assert rc == 1
    out = capsys.readouterr().out
    assert "FAIL: Missing required pipeline measurement(s)" in out


def test_main_demo_baseline_succeeds(monkeypatch, capsys):
    from python.capstone_checks import main
    monkeypatch.setattr("sys.argv", ["capstone_checks.py", "--demo-baseline"])
    rc = main()
    assert rc == 0
    out = capsys.readouterr().out
    assert "PASS  T1 ingestion" in out
    assert "PASS  T3 processing" in out
