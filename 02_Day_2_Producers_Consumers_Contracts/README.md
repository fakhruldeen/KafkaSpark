# Day 2: High-Throughput Production, Reliable Consumption & Schemas
**Course:** Enterprise Real-Time Data Engineering · Mohamed Fouad Fakhruldeen
**Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)

---
### Overview & Focus
Idempotent event generation, rebalance management, and strong schema enforcement.

### Module Sessions in this Day:

- **[S03: Producer Internals & Delivery Semantics](S03_Producer_Internals_Delivery_Semantics/README.md)**: RecordAccumulator buffering, BufferPool allocation, acks quorums, and idempotence deduplication.
- **[S04: Consumer Groups, Rebalancing & Reliability](S04_Consumer_Internals_Rebalance_Reliability/README.md)**: Offset commit semantics, CooperativeSticky incremental rebalance, and Dead Letter Topics (DLT).
- **[S05: Confluent Schema Registry & Data Contracts](S05_Confluent_Schema_Registry_Data_Contracts/README.md)**: Apache Avro binary framing, wire protocol 5-byte header, and backward/forward compatibility rules.