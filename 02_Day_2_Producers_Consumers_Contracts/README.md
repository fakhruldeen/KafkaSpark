# Day 2: High-Throughput Production, Reliable Consumption & S02 Foundations
**Course:** Enterprise Real-Time Data Engineering · Mohamed Fouad Fakhruldeen  
**Course Portal:** [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark)  
**Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)

---

### Overview & Pedagogical Delivery Model (30% / 20% / 50%)

Day 2 focuses on high-throughput event production, resilient consumer group scaling, and schema contracts, while re-anchoring foundational partition and replication mechanics:

- **30% Conceptual Lectures (90 min):** Architectural grounding for partition hashing, producer `RecordAccumulator`, and consumer cooperative rebalancing.
- **20% Instructor Live Demos (60 min):** Code inspection, segment forensic log dumping, and live terminal failover demonstrations.
- **50% Trainee Workstation Labs (150 min):** Dedicated hands-on practice where students write code, simulate node crashes, and validate against automated verification test gates.

For the comprehensive hourly schedule and lab commands, refer to **[`DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.md`](./DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.md)** or **[`DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.pdf`](./DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.pdf)**.

---

### Module Sessions in this Day:

- **[S02: Topics, Partitions, Replication & Storage (Remediation & Labs)](S02_Topics_Partitions_Replication/README.md)**: MurmurHash2 deterministic key partitioning, physical log segments (`.log`, `.index`), In-Sync Replicas (ISR), and chaos broker failover.
- **[S03: Producer Internals & Delivery Semantics](S03_Producer_Internals_Delivery_Semantics/README.md)**: `RecordAccumulator` micro-batching, BufferPool allocation, `acks` quorums, Snappy compression, and idempotence deduplication.
- **[S04: Consumer Groups, Rebalancing & Reliability](S04_Consumer_Internals_Rebalance_Reliability/README.md)**: Offset commit semantics (`__consumer_offsets`), CooperativeSticky incremental rebalance, and Dead Letter Topics (`.DLT`).
- **[S05: Confluent Schema Registry & Data Contracts](S05_Confluent_Schema_Registry_Data_Contracts/README.md)**: Apache Avro binary framing, wire protocol 5-byte header, and backward/forward compatibility rules.

---

### Automated Verification Gates & Quizzes (Run from `00_Lab_Environment`)
Every trainee should execute these verification scripts on their workstations:
```bash
cd 00_Lab_Environment
./verify_s02.sh    # S02 Verification Gate
./verify_s03.sh    # S03 Verification Gate
./verify_s04.sh    # S04 Verification Gate
./verify_s05.sh    # S05 Verification Gate

# Interactive Terminal Knowledge Gates
python3 quiz.py s02
python3 quiz.py s03
python3 quiz.py s04
python3 quiz.py s05
```
