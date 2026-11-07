"""S11 lab: the capstone integration job, Kafka -> Spark -> Kafka, with checkpoint recovery.

Recovery rule used here: the checkpoint directory is the only source of progress.
Do not delete it to 'fix' a problem. Delete it only to deliberately restart from the start,
and say so in the runbook.
"""
from __future__ import annotations

import argparse
import json
import time

from pyspark.sql import SparkSession, functions as F

from python.s10_stream import EVENT_SCHEMA
from python.common import BOOTSTRAP, TOPIC_DLT, TOPIC_KPI, TOPIC_ORDERS


def build(spark: SparkSession, checkpoint: str, max_per_trigger: int):
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", BOOTSTRAP)
           .option("subscribe", TOPIC_ORDERS)
           .option("startingOffsets", "earliest")
           .option("maxOffsetsPerTrigger", max_per_trigger)     # caps each batch (back-pressure)
           .load())

    parsed = raw.select(F.col("key").cast("string").alias("key"),
                        F.col("value").cast("string").alias("raw"),
                        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e"))

    good = parsed.filter(F.col("e.event_id").isNotNull() & F.col("e.amount").isNotNull())
    bad = parsed.filter(F.col("e.event_id").isNull() | F.col("e.amount").isNull())

    # Duplicates: keep one row per event_id within a bounded window of time.
    deduped = (good.select("key", "raw", "e.*")
               .withColumn("event_ts", F.to_timestamp("event_time"))
               .withWatermark("event_ts", "15 minutes")
               .dropDuplicates(["event_id"]))

    enriched = deduped.select(
        F.col("key"),
        F.to_json(F.struct("event_id", "order_id", "store_id", "event_type", "amount",
                           "event_ts")).alias("value"))

    main_query = (enriched.writeStream.outputMode("append").format("kafka")
                  .option("kafka.bootstrap.servers", BOOTSTRAP)
                  .option("topic", TOPIC_KPI)
                  .option("checkpointLocation", f"{checkpoint}/main")
                  .start())

    # Invalid records are routed to the DLT, not dropped silently.
    dlt_query = (bad.select(F.col("key"), F.col("raw").alias("value"))
                 .writeStream.format("kafka")
                 .option("kafka.bootstrap.servers", BOOTSTRAP)
                 .option("topic", TOPIC_DLT)
                 .option("checkpointLocation", f"{checkpoint}/dlt")
                 .start())
    return main_query, dlt_query



def lag_of(progress: dict) -> int:
    """Records not yet read: the newest offset minus the offset read in the last batch, per partition.
    Zero means the job has caught up with the topic."""
    def as_dict(value):
        return json.loads(value) if isinstance(value, str) else (value or {})
    total = 0
    for source in progress.get("sources", []):
        end, latest = as_dict(source.get("endOffset")), as_dict(source.get("latestOffset"))
        for topic, partitions in latest.items():
            for partition, offset in partitions.items():
                total += max(0, offset - end.get(topic, {}).get(str(partition), offset))
    return total

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="S11 capstone stage 4 job")
    parser.add_argument("--checkpoint", default="data/checkpoints/capstone")
    parser.add_argument("--max-per-trigger", type=int, default=5000)
    parser.add_argument("--run-seconds", type=int, default=0, help="Run streaming query for N seconds then shut down cleanly (0 means run forever)")
    args = parser.parse_args(argv)
    spark = (SparkSession.builder.appName("s11-pipeline")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")   # Kafka source/sink connector for Spark 3.5.x
             .config("spark.sql.ansi.enabled", "false")   # explicit: Spark 4 defaults to ANSI on; labs keep Spark 3 behaviour (see MIGRATION.md)
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    build(spark, args.checkpoint, args.max_per_trigger)
    report_every = 2 if args.run_seconds > 0 else 15   # responsive polling when bounded
    start_time = time.time()
    print("[Pipeline] Starting Kafka -> Spark -> Kafka streaming engine...", flush=True)
    print(f"[Pipeline] Main sink: {TOPIC_KPI} | Checkpoint: {args.checkpoint}/main", flush=True)
    print(f"[Pipeline] DLT sink: {TOPIC_DLT} | Checkpoint: {args.checkpoint}/dlt", flush=True)
    while spark.streams.active:
        time.sleep(report_every)
        for query in spark.streams.active:
            progress = query.lastProgress
            if progress:
                input_rate = progress.get('inputRowsPerSecond', 0.0)
                proc_rate = progress.get('processedRowsPerSecond', 0.0)
                print(f"[Batch {progress['batchId']}] Processed {progress['numInputRows']} rows | "
                      f"Input rate: {input_rate:.1f} rows/s | Processing rate: {proc_rate:.1f} rows/s", flush=True)
        if args.run_seconds > 0 and (time.time() - start_time) >= args.run_seconds:
            for query in list(spark.streams.active):
                query.stop()
            print("[Pipeline] Clean shutdown completed.", flush=True)
            break
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
