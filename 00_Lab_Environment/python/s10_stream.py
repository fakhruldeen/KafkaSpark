"""S10 lab: Structured Streaming from Kafka with event-time windows and a watermark.

Run:  python3 -m python.s10_stream   (from the repository root; the Kafka connector is fetched on first run)
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from python.common import BOOTSTRAP, TOPIC_KPI, TOPIC_ORDERS

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()), StructField("order_id", StringType()),
    StructField("customer_id", StringType()), StructField("store_id", StringType()),
    StructField("event_type", StringType()), StructField("amount", DoubleType()),
    StructField("event_time", StringType()),
])


def build(spark: SparkSession, watermark: str, checkpoint: str, sink: str):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC_ORDERS)
           .option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true")          # visible failure beats silent loss
           .load())

    events = (raw.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))
              .select("e.*")
              .withColumn("event_ts", F.to_timestamp("event_time"))   # event time, from the record
              .filter(F.col("event_type") == "PaymentAccepted")
              .filter(F.col("amount").isNotNull()))

    kpis = (events.withWatermark("event_ts", watermark)               # how late is still accepted
            .groupBy(F.window("event_ts", "1 minute"), "store_id")
            .agg(F.sum("amount").alias("revenue"), F.count("*").alias("payments")))

    out = kpis.select(F.col("store_id").alias("key"),
                      F.to_json(F.struct("window", "revenue", "payments")).alias("value"))

    return (out.writeStream.outputMode("append")                      # a window is final once closed
            .format("kafka")
            .option("kafka.bootstrap.servers", BOOTSTRAP)
            .option("topic", TOPIC_KPI)
            .option("checkpointLocation", checkpoint)
            .start())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S10 streaming KPIs")
    parser.add_argument("--watermark", default="10 minutes")
    parser.add_argument("--checkpoint", default="data/checkpoints/kpi")
    args = parser.parse_args(argv)
    spark = (SparkSession.builder.appName("s10-stream")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")   # Kafka source/sink connector for Spark 3.5.x
             .config("spark.sql.ansi.enabled", "false")   # explicit: Spark 4 defaults to ANSI on; labs keep Spark 3 behaviour (see MIGRATION.md)
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    query = build(spark, args.watermark, args.checkpoint, sink=TOPIC_KPI)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
