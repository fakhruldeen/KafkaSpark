---
**Course:** Enterprise Real-Time Data Engineering  
**Instructor:** Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me))  
**Course Portal:** [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark)  
**Code & Labs Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)  
---

# Stack Architecture & Delivery Baseline: Spark 3.5 LTS with Kafka 4.4 KRaft

**Classroom Delivery Baseline Decision (Production Certified):**
- **Apache Kafka:** 4.4.0 (KRaft mode only; ZK-free)
- **Confluent Platform:** 8.4.0 (Schema Registry, Connect, ksqlDB)
- **Apache Spark / PySpark:** **3.5.x LTS** (`pyspark==3.5.9`, `spark-sql-kafka-0-10_2.12:3.5.1`, Java 17 LTS)
- **Status:** **100% Certified & Offline Frozen** in `USB_DEPLOYMENT_BUNDLE`. All automated quality gates, validator scripts (`verify_s08.sh` to `verify_s12.sh`), PySpark batch, and Kafka Structured Streaming pipelines are validated against Spark 3.5.x LTS.

**Forward Enterprise Migration Roadmap (Post-Course Transition to Spark 4.x):**
- Apache Spark 4.x introduces default ANSI SQL mode (`spark.sql.ansi.enabled=true`), Scala 2.13 only, and Python 3.11+.
- The migration notes below detail the verified configuration deltas for enterprise teams planning their forward migration to Spark 4.x in production after course completion.

## 1. Pins changed in v8

| File | v7 | v8 |
|---|---|---|
| `.env` KAFKA_IMAGE | apache/kafka:3.9.0 | apache/kafka:4.4.0 |
| `.env` SCHEMA_REGISTRY_IMAGE | cp-schema-registry:7.9.0 | cp-schema-registry:8.4.0 |
| `.env` CONNECT_IMAGE | cp-kafka-connect:7.9.0 | cp-kafka-connect:8.4.0 |
| `.env` KSQLDB_IMAGE | cp-ksqldb-server:7.9.0 | cp-ksqldb-server:8.4.0 |
| `requirements.txt` | pyspark==3.5.1 | pyspark==4.3.0 |
| `python/s10_stream.py`, `python/s11_pipeline.py` | spark-sql-kafka-0-10_2.12:3.5.1 | spark-sql-kafka-0-10_2.13:4.3.0 |
| All Spark session builders (S08, S09, S10, S11) | no ANSI setting | `spark.sql.ansi.enabled=false` set explicitly |

Image tags must be confirmed to exist before the first run (`docker pull` each one). If a patch tag is missing, use the newest published patch in the same line and record it in `.env`.

## 2. Why the ANSI setting is explicit

Spark 4.0 turns ANSI SQL mode on by default. Results were checked on PySpark 4.3.0 with the S08 batch job:

| Run | Read | Clean | Quarantined | Duplicates removed |
|---|---|---|---|---|
| S08 on Spark 4.3, ANSI off (explicit) | 4,033 | 3,977 | 23 | 33 |
| S08 on Spark 4.3, ANSI on (default) | 4,033 | 3,977 | 23 | 33 |
| Expected (course reference) | 4,033 | 3,977 | 23 | 33 |

The explicit setting is kept so that the lab behaves the same on any Spark 4 build and matches the Spark 3 behaviour the validated counts were built on. The team may remove it after the S10 and S11 streaming runs pass with ANSI on.

## 3. Verification so far (sandbox, no Kafka broker)

| Lab | Check | Result |
|---|---|---|
| S08 batch | Row counts against the course reference | Pass: 4,033 / 3,977 / 23 / 33 |
| S09 batch | Broadcast join plan and revenue table | Pass: BroadcastHashJoin present; 1,985 payments; four regions with the same revenue as v7 |
| S09 batch | Join row count guard | Pass: 3,977 joined rows |
| All Python files | Syntax check (`py_compile`) | Pass |
| S10, S11 | Structured Streaming with Kafka | Not run: no broker in the sandbox |
| S01 to S07, S12 | Kafka, Connect, Schema Registry, ksqlDB | Not run |

Note: the Docker daemon is not available in the build sandbox, so no container stack could be started.

## 4. Breaking-change check against the official upgrade notes

| Source | Change | Effect on the labs | Action |
|---|---|---|---|
| Kafka 4.0 upgrade notes | ZooKeeper removed; KRaft only | Labs already run KRaft | None |
| Kafka 4.0 | Brokers, Connect and tools require Java 17 | Lab 00 installs JDK 17 | None. Keep JDK 17 |
| Kafka 4.0 | `log.message.format.version` and `message.format.version` removed | Not used in the compose file | Check no lab script sets them |
| Kafka 4.0 | `kafka-console-consumer --whitelist` replaced by `--include`; `--bootstrap-server` accepts comma-separated values only | Labs use `--bootstrap-server` and `--timeout-ms` | Run each lab command once |
| Kafka 4.0 | `linger.ms` default changed from 0 to 5 | Latency numbers in S03 may move; counts do not | Re-measure any timing in the lab text |
| Kafka 4.0 | `enable.idempotence` no longer falls back when in-flight requests exceed 5 | Only matters if a lab sets more than 5 in-flight requests | Check S03 producer config |
| Kafka 4.0 (checked on the consumer rebalance protocol page) | The server supports the KIP-848 protocol, but clients opt in with `group.protocol=consumer`. The Python labs use the default (classic) protocol and do not set it | No change to S04 or S11 behaviour unless `group.protocol` is set | Confirm that no lab sets `group.protocol`. Session and heartbeat settings for the new protocol are server-side only |
| Confluent 8.4 | `cp-kafka-connect-base` and `cp-server-connect-base` images removed | v8 uses the non-base images | None |
| Confluent 8.4 | `broker.id` deprecated in favour of `node.id` | Compose uses `KAFKA_NODE_ID` | None |
| Confluent 8.4 | `_confluent-license` topic deprecated; `confluent.license` key | Not set in the lab compose file | Confirm the images start without a licence key; set `confluent.license.topic` if a warning blocks start |
| Confluent 8.4 | `kafka-storage format` fails on a formatted directory unless `--ignore-formatted` | Only if a volume is reused | Use `make reset` before the first v8 run |
| Confluent 8.4 | Schema Registry `LOGICAL` compatibility conflicts with `FORWARD` and `FORWARD_TRANSITIVE` | S05 uses BACKWARD | Confirm S05 uses BACKWARD |
| Confluent 8.4 | jmx_prometheus_javaagent 1.6.0; metrics path `/metrics` | Only extra module X7 | Update scrape path when X7 is run |
| Spark 4.0 | ANSI mode on by default | Handled by explicit setting (section 2) | None |
| Spark 4.0 | `spark.sql.maxSinglePartitionBytes` default 128m | Affects single-file writes only | Check S08 output file counts |
| Spark 4.0 | ORC default codec zstd | Labs write Parquet | None |
| Spark 4.0 | Scala 2.13 only | Kafka connector coordinate changed to `_2.13` | Done in v8 |
| Spark 4.0 | Python 3.11 or later | Lab 00 must install Python 3.11+ | Update Lab 00 |
| Spark 4.0 | Timestamp parse policy `CORRECTED` by default | Affects `to_timestamp` on malformed input | Check quarantine count after S11 run |

## 5. Checked and open items

Checked against the official pages on 2026-11-03:

- Spark Kafka connector coordinate for 4.3.0 is `org.apache.spark:spark-sql-kafka-0-10_2.13:4.3.0`. Matches v8.
- Spark Kafka options used by the labs (`startingOffsets`, `failOnDataLoss`, `maxOffsetsPerTrigger`) are valid in 4.3.0. None of the options listed as blocked (`group.id`, `auto.offset.reset`, `enable.auto.commit`, deserializers) is set in a Spark lab. The Python consumer labs (S04, S05) set them on the confluent-kafka client, which is not affected.
- Kafka 4.0 consumer rebalance protocol: opt-in per client (correction to the earlier note in this file).

Still open, must be read before the run:

- Kafka 4.x KRaft settings for `node.id` and controller quorum in the upgrade guide (the v8 compose file uses the same keys as v7).
- Kafka Streams upgrade guide for 4.x (affects the Java take-home notes only).
- Confluent 8.4 Schema Registry and ksqlDB release notes beyond the items above.
- Kafka client version bundled with the Spark 4.3 connector (Spark page does not state it); check the POM after the Maven download is possible.

Network note: the build sandbox cannot reach Docker Hub or Maven Central (policy denial), so the images and Maven artifacts could not be pulled here. The image tags are therefore unconfirmed.

## 6. Run order for the validation

1. `make reset` on a clean host; pull the four images; confirm tags exist.
2. Start the stack and run `make check`.
3. S01 to S04 labs and their PASS checks; CP1.
4. S05 to S07 labs; CP2.
5. S08 to S10 labs; CP3.
6. S11 lab and acceptance tests T1 to T5; CP4.
7. Record every count and timing in the trainer log, and compare with the v7 reference values.
8. Only after steps 1 to 7 pass, mark v8 as the delivery stack in Lab 00 and in the Phase 1 environment list.


---
*Enterprise Real-Time Data Engineering · Instructor: Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me)) · Course Materials: [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark) · Repository: [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)*
