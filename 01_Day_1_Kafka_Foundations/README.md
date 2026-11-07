# Day 1: Apache Kafka Architecture & Storage Internals
**Course:** Enterprise Real-Time Data Engineering · Mohamed Fouad Fakhruldeen
**Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)

---
### Overview & Focus
Foundational distributed systems principles, KRaft consensus, Commit Log storage anatomy, and partition replication.

### Module Sessions in this Day:

- **[S00: Course Launch & Workstation Readiness](S00_Refresh/README.md)**: Orientation, distributed systems theory, and Docker stack pre-flight verification.
- **[S01: Kafka Core Foundations & Architecture](S01_Kafka_Foundations/README.md)**: Decoupling, Commit Log mechanics, Linux Zero-Copy sendfile(2), and KRaft metadata mode.
- **[S02: Topics, Partitions & Replication](S02_Topics_Partitions_Replication/README.md)**: MurmurHash2 key partitioning, log segments, Leader-Follower quorums, and In-Sync Replicas (ISR).