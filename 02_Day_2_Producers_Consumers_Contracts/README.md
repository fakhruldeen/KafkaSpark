# Day 2: High-Throughput Production, Reliable Consumption & S02 Foundations
**Course:** Enterprise Real-Time Data Engineering · Mohamed Fouad Fakhruldeen  
**Course Portal:** [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark)  
**Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)

---

### Overview & Pedagogical Delivery Model (15-Min Kick-Off + 30% / 20% / 50%)

Day 2 focuses on high-throughput event production, resilient consumer group scaling, and schema contracts, while re-anchoring foundational partition and replication mechanics:

- **09:00 – 09:15 (15 Min): Morning Kick-Off Revision**  
  Visual architecture recall, 3 diagnostic killer questions, and a 2-minute student workstation stack verification (`tools/check_stack.sh`). Refer to **[`DAY_2_15_MINUTE_MORNING_REVISION.md`](./DAY_2_15_MINUTE_MORNING_REVISION.md)** / **[`DAY_2_15_MINUTE_MORNING_REVISION.pdf`](./DAY_2_15_MINUTE_MORNING_REVISION.pdf)**.
- **30% Conceptual Lectures (90 min):** Architectural grounding for partition hashing, producer `RecordAccumulator`, and consumer cooperative rebalancing.
- **20% Instructor Live Demos (60 min):** Code inspection, segment forensic log dumping, and live terminal failover demonstrations.
- **50% Trainee Workstation Labs (150 min):** Dedicated hands-on practice where students write code, simulate node crashes, and validate against automated verification test gates.

---

### Core Day 2 Documents & Guides:

- 🌅 **[Day 2 15-Minute Morning Kick-Off Guide (MD)](./DAY_2_15_MINUTE_MORNING_REVISION.md)** · **[PDF Version](./DAY_2_15_MINUTE_MORNING_REVISION.pdf)**
- 📘 **[Day 2 Master Training & Lab Guide (MD)](./DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.md)** · **[PDF Version](./DAY_2_MASTER_TRAINING_AND_LAB_GUIDE.pdf)**
- 📖 **[Kafka: Complete Architecture & Reference Guide (MD)](./KAFKA_COMPLETE_REFERENCE.md)** · **[PDF Version](./KAFKA_COMPLETE_REFERENCE.pdf)**
- 📊 **[Day 2 Complete Slides (PDF)](./DAY_2_COMPLETE_SLIDES.pdf)**

---

### Module Sessions in this Day:

- **[S02: Topics, Partitions, Replication & Storage (Remediation & Labs)](S02_Topics_Partitions_Replication/README.md)**: MurmurHash2 deterministic key partitioning, physical log segments (`.log`, `.index`), In-Sync Replicas (ISR), and chaos broker failover.
- **[S03: Producer Internals & Delivery Semantics](S03_Producer_Internals_Delivery_Semantics/README.md)**: `RecordAccumulator` micro-batching, BufferPool allocation, `acks` quorums, Snappy compression, and idempotent producer sequence deduplication.
- **[S04: Consumer Groups, Rebalancing & Reliability](S04_Consumer_Internals_Rebalance_Reliability/README.md)**: Offset commit semantics (`__consumer_offsets`), CooperativeSticky incremental rebalance, and Dead Letter Topics (`.DLT`).
- **[S05: Confluent Schema Registry & Data Contracts](S05_Confluent_Schema_Registry_Data_Contracts/README.md)**: Apache Avro binary framing, wire protocol 5-byte header, and backward/forward compatibility rules.

---

### Automated Verification Gates & Quizzes (Run from `00_Lab_Environment`)
Every trainee executes these verification scripts directly on their workstations:
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
