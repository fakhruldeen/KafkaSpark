"""S09 lab: join orders to customers safely, with a row-count check that catches fan-out.

Two rules this file teaches:
  1. A join on a key that is not unique multiplies order rows. We check the key first.
  2. The customer table is small, so we broadcast it: the big orders table is not shuffled.
"""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, Window, functions as F


def assert_unique_key(df: DataFrame, key: str) -> None:
    dupes = df.groupBy(key).count().filter(F.col("count") > 1).count()
    if dupes:
        raise ValueError(f"{dupes} keys in '{key}' are not unique; joining would multiply rows")


def latest_per_customer(customers: DataFrame) -> DataFrame:
    """Keep one row per customer_id: the one with the latest updated_at.

    The raw CSV can hold more than one row per customer (a changed address, an old copy).
    Ranking by updated_at and keeping row 1 gives a unique key before the join.
    """
    w = Window.partitionBy("customer_id").orderBy(F.col("updated_at").desc())
    return (customers
            .withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn"))


def join_revenue(orders: DataFrame, customers: DataFrame, broadcast_small: bool = True) -> DataFrame:
    customers = latest_per_customer(customers)
    assert_unique_key(customers, "customer_id")                       # fail fast, before the join
    small = F.broadcast(customers) if broadcast_small else customers  # hint: copy the small table everywhere
    joined = orders.join(small, on="customer_id", how="left")
    if joined.count() != orders.count():                              # a left join must keep the row count
        raise ValueError("join changed the row count: check the join key")
    return joined


def revenue_by_region(joined: DataFrame) -> DataFrame:
    """Revenue is money received: count PaymentAccepted only, so an order is not counted twice
    (once for OrderPlaced and once for the payment)."""
    payments = joined.filter(F.col("event_type") == "PaymentAccepted")
    return (payments.groupBy("region")
            .agg(F.round(F.sum("amount"), 2).alias("revenue"), F.count("*").alias("payments"))
            .orderBy("region"))


def explain_plan(df: DataFrame) -> None:
    df.explain(mode="formatted")   # look for BroadcastHashJoin and the absence of a shuffle on orders


if __name__ == "__main__":
    spark = (SparkSession.builder.appName("s09-join")
             # explicit: Spark 4 defaults to ANSI on; labs keep Spark 3 behaviour (see MIGRATION.md)
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    orders = spark.read.parquet("data/curated/orders")
    customers = spark.read.option("header", True).csv("data/customers.csv")
    joined = join_revenue(orders, customers)
    revenue_by_region(joined).show()
    explain_plan(joined)
    spark.stop()
