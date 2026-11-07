---
**Course:** Enterprise Real-Time Data Engineering  
**Instructor:** Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me))  
**Course Portal:** [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark)  
**Code & Labs Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)  
---

# Runbook for the local Claude CLI agent: validate the v8 stack

Target stack (not yet validated): Apache Kafka 4.4.0 (KRaft), Confluent Platform 8.4.0 (Schema Registry, Connect, ksqlDB), PostgreSQL 16, Kafka UI v0.7.2, Spark 4.3.0 (Scala 2.13), Python 3.11+, JDK 17 or 21.

Status: the offline unit suite passes in the build environment (46 tests). Every Kafka, Connect, Schema Registry, ksqlDB and streaming step is unverified. Your run is the validation. Record every result in the trainer log; do not mark v8 as the delivery stack until section 6 passes.

## 0. Prerequisites on the host

Check each item and stop if one fails. Do not work around a registry or policy block.

1. Docker Engine with Compose v2: `docker version` and `docker compose version`.
2. Network access to Docker Hub (`apache/kafka`, `confluentinc/cp-*`, `postgres`, `provectuslabs/kafka-ui`) and to Maven Central (Spark connector jars).
3. Python 3.11 or later, JDK 17 or 21 (`java -version`), and `pip`.
4. At least 16 GB RAM and 20 GB free disk for the stack.

## 1. Environment setup

```bash
cd kafka-spark-lab-v8
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt          # pyspark==4.3.0, confluent-kafka[avro], pytest
python -c "import pyspark; print(pyspark.__version__)"   # expect 4.3.0
make test                                  # offline suite, no Docker: expect 46 passed
```

## 2. Confirm the image tags exist (before any compose up)

```bash
for img in apache/kafka:4.4.0 confluentinc/cp-schema-registry:8.4.0 \
           confluentinc/cp-kafka-connect:8.4.0 confluentinc/cp-ksqldb-server:8.4.0 \
           postgres:16 provectuslabs/kafka-ui:v0.7.2; do
  docker manifest inspect "$img" >/dev/null && echo "OK   $img" || echo "FAIL $img"
done
```

Any FAIL: stop and record the tag. Check the tag on the vendor page and update `.env` and MIGRATION.md together.

## 3. Start the stack

```bash
make reset          # clean state: deletes all lab volumes
make up             # docker compose up -d --wait
make check          # one PASS or FAIL line per service
```

Expected: every service PASS. Kafka must report `4.4.0` in its log (`make logs SVC=kafka1`). Schema Registry and ksqlDB must report `8.4.0`.

## 4. Lab sequence

Run the labs in course order. For each lab, follow its guide in `python/` and `docs/` and record the PASS line.

| Step | Labs | Gate |
|---|---|---|
| A | S01 to S04 (topics, producer, consumer, delivery semantics) | PASS checks; then CP1 |
| B | S05 to S07 (Schema Registry, Connect, Streams and ksqlDB) | `make connect-source` and `make connect-sink`, then `make connect-status`; then CP2 |
| C | S08 to S10 (Spark batch, joins, streaming) | Spark jobs print the expected counts; then CP3 |
| D | S11 pipeline and acceptance tests T1 to T5; then CP4 | All five tests PASS |
| E | S12 faults (four Part B faults) | Each fault reproduced and recovered |

Spark jobs need the Kafka connector on first run. The first run downloads `org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1` from Maven Central. If Maven is blocked, the job fails at class loading: record it and stop.

## 5. Expected values to compare

| Check | Expected (v7 reference, historical) | v8 result |
|---|---|---|
| S08 batch counts | 4,033 / 3,977 / 23 / 33 | |
| S09 revenue table | four regions, 1,985 payments, revenue 900163.5 | |
| S09 dedupe | 201 raw, 200 latest, C0007 resolves to Cairo | |
| S11 good events and rejects (seed 42) | 3,977 good, 23 rejected | |
| Kafka version in logs | 4.4.0 | |
| Confluent version (SR, Connect, ksqlDB) | 8.4.0 | |

Record any difference. The v7 numbers are a reference, not a pass rule for a new stack: a changed value must be explained before it is accepted.

## 6. Acceptance and promotion

1. `make check` passes after a full run.
2. Labs S01 to S12 pass in order, with CP1 to CP4 recorded.
3. The offline suite passes (`make test`).
4. Record counts and timings in the trainer log.
5. Only then update Lab 00 and the Phase 1 environment list from "target, not validated" to "validated on <date>".

If any step fails, stop, capture `docker compose logs`, the failing command and its output, and do not change pins without recording the reason in MIGRATION.md.

## 7. Teardown

```bash
make down           # stop, keep data
make reset          # stop and delete all lab data
```

## Known open items

- Image tags and Maven artifacts have not been pulled in the build environment.
- X6 Delta and Iceberg coordinates are placeholders. Match them to Spark 4.3 before teaching X6.
- Windows and WSL 2 desktop run for S06 to S12 is pending.
- Java notes (`Java_Take_Home_Notes.md`) are unverified and need a JDK 17 or 21 build run.


---
*Enterprise Real-Time Data Engineering · Instructor: Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me)) · Course Materials: [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark) · Repository: [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)*
