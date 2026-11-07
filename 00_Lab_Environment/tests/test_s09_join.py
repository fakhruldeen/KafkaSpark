"""S09 lab tests: the join rules in python/s09_join_check.py, on a tiny local dataset.

Needs PySpark and a Java runtime (Java 17 in Lab 00), but no Kafka and no Docker.
Skipped with a clear reason if PySpark is not installed, so `make test` still runs offline.
"""
import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402

from python.s09_join_check import (  # noqa: E402
    assert_unique_key,
    join_revenue,
    latest_per_customer,
    revenue_by_region,
)


@pytest.fixture(scope="module")
def spark():
    session = (SparkSession.builder.master("local[1]").appName("s09-tests")
               .config("spark.sql.shuffle.partitions", "2")
               .getOrCreate())
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def _customers(spark):
    # Same shape as data/customers.csv: C0007 appears twice, the older row first.
    return spark.createDataFrame(
        [("C0007", "Giza", "2026-09-01"),
         ("C0007", "Cairo", "2026-10-14"),
         ("C0060", "Delta", "2026-05-02")],
        ["customer_id", "region", "updated_at"],
    )


def _orders(spark):
    return spark.createDataFrame(
        [("O1-ord", "C0007", "OrderPlaced", 100.0, "CAI-01"),
         ("O1-pay", "C0007", "PaymentAccepted", 100.0, "CAI-01"),
         ("O2-pay", "C0060", "PaymentAccepted", 50.0, "ALX-01")],
        ["event_id", "customer_id", "event_type", "amount", "store_id"],
    )


def test_unique_key_check_raises_on_duplicate_customers(spark):
    with pytest.raises(ValueError, match="not unique"):
        assert_unique_key(_customers(spark), "customer_id")


def test_unique_key_check_passes_after_deduplication(spark):
    assert_unique_key(latest_per_customer(_customers(spark)), "customer_id")


def test_latest_per_customer_keeps_newest_row_not_first_arrival(spark):
    rows = {r["customer_id"]: r["region"] for r in latest_per_customer(_customers(spark)).collect()}
    assert rows == {"C0007": "Cairo", "C0060": "Delta"}


def test_revenue_counts_payments_once_per_order(spark):
    result = revenue_by_region(join_revenue(_orders(spark), _customers(spark)))
    totals = {r["region"]: (r["revenue"], r["payments"]) for r in result.collect()}
    assert totals == {"Cairo": (100.0, 1), "Delta": (50.0, 1)}


def test_broadcast_and_sort_merge_give_the_same_answer(spark):
    orders, customers = _orders(spark), _customers(spark)
    broadcast = revenue_by_region(join_revenue(orders, customers, broadcast_small=True)).collect()
    sort_merge = revenue_by_region(join_revenue(orders, customers, broadcast_small=False)).collect()
    assert [tuple(r) for r in broadcast] == [tuple(r) for r in sort_merge]
