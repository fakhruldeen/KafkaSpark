"""S08 lab: a batch ETL job with PySpark DataFrames.

Reads the raw JSON Lines file, applies the data-quality rules from python/common.py,
writes clean Parquet partitioned by store and a quarantine folder for rejected rows.

Run:  python3 -m python.s08_batch --input data/orders.jsonl --output data/curated   (from the repository root)
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

RAW_SCHEMA = StructType([           # explicit schema: no inference, so the job is predictable
    StructField("event_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("store_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("event_time", StringType(), True),
])


def build_session(app: str = "s08-batch") -> SparkSession:
    return (SparkSession.builder.appName(app)
            .config("spark.sql.ansi.enabled", "false")   # explicit: Spark 4 defaults to ANSI on; labs keep Spark 3 behaviour (see MIGRATION.md)
             .config("spark.sql.shuffle.partitions", "8")   # small data: fewer shuffle partitions
            .getOrCreate())


def run(spark: SparkSession, input_path: str, output_path: str) -> dict:
    raw = spark.read.schema(RAW_SCHEMA).json(input_path)

    # Rule set. Each rule is a column expression that is TRUE for a valid row.
    valid = (F.col("event_id").isNotNull()
             & F.col("order_id").isNotNull()
             & F.col("customer_id").isNotNull()
             & F.col("store_id").isNotNull()
             & F.col("event_type").isin("OrderPlaced", "PaymentAccepted", "Refunded")
             & F.col("amount").isNotNull() & (F.col("amount") > 0))

    checked = raw.withColumn("is_valid", valid)
    clean = (checked.filter("is_valid")
             .drop("is_valid")
             .dropDuplicates(["event_id"])                    # the same event may arrive twice
             .withColumn("event_ts", F.to_timestamp("event_time")))
    quarantine = checked.filter(~F.col("is_valid")).drop("is_valid")

    clean.write.mode("overwrite").partitionBy("store_id").parquet(f"{output_path}/orders")
    quarantine.write.mode("overwrite").parquet(f"{output_path}/quarantine")

    counts = {"read": raw.count(), "clean": clean.count(), "quarantined": quarantine.count()}
    counts["duplicates_removed"] = counts["read"] - counts["clean"] - counts["quarantined"]
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S08 batch ETL")
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args(argv)
    spark = build_session()
    try:
        counts = run(spark, args.input, args.output)
        print(counts)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
