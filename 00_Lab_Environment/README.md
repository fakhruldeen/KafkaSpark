# Hands-On Lab Environment & Infrastructure
**Course:** Enterprise Real-Time Data Engineering (Apache Kafka, Confluent, Apache Spark)  
**Lead Instructor:** Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me))  
**Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)

---

## 🚀 1-Command Automated Quick Start

To provision the complete lab environment on your machine (Linux, macOS, or Windows 10/11 WSL 2):

```bash
cd 00_Lab_Environment
./setup_environment.sh
```

### What `setup_environment.sh` executes automatically:
1. **Downloads Spark Kafka Connector JARs** directly from **Maven Central** into `jars/`.
2. **Creates a local Python virtual environment (`.venv`)** and installs all dependencies (`pyspark`, `confluent-kafka`, `pytest`, `fastavro`) from **PyPI**.
3. **Pulls official Docker images** from **Docker Hub** and **Confluent Registry**:
   - `apache/kafka:3.7.0` (KRaft consensus mode, 3 multi-broker nodes)
   - `confluentinc/cp-schema-registry:7.5.0`
   - `confluentinc/cp-kafka-connect:7.5.0`
   - `confluentinc/cp-ksqldb-server:7.5.0`
   - `provectuslabs/kafka-ui:v0.7.2`
   - `postgres:15-alpine`
4. **Executes `test_workstation.sh`** to verify system memory, CPU cores, Docker permissions, and open network ports.

---

## 🛠️ Cluster Lifecycle Commands

Use the bundled `Makefile` or Docker Compose directly:

| Action | Command | Purpose |
|---|---|---|
| **Start Core Cluster** | `docker compose up -d kafka1 kafka2 kafka3 kafka-ui` | Boots 3-broker KRaft cluster & Web UI |
| **Start Full Stack** | `docker compose up -d` | Boots all brokers, Schema Registry, Connect, and Postgres |
| **Check Stack Health** | `make check` | Runs endpoint HTTP/port healthchecks across services |
| **Open Web UI** | Navigate to `http://localhost:8080` in your browser | Visual cluster management, topics, and consumers |
| **Run Automated Tests**| `pytest tests/` | Runs 24 integration tests across all course modules |
| **Stop Cluster** | `docker compose down` | Gracefully stops containers (data is preserved in volumes) |
| **Reset State** | `make clean` | Removes volumes, checkpoints, and resets all topics |

---

## 📚 Essential Lab Runbooks & Guides
- 📑 **[`WORKSTATION_SETUP_GUIDE.pdf`](WORKSTATION_SETUP_GUIDE.pdf)**: Detailed step-by-step setup guide for Linux, macOS, and Windows WSL 2.
- 📑 **[`RUNBOOK_CLI.pdf`](RUNBOOK_CLI.pdf)**: Daily Kafka & Spark CLI command reference sheet.
- 📑 **[`RUNBOOK_TROUBLESHOOTING.pdf`](RUNBOOK_TROUBLESHOOTING.pdf)**: Emergency troubleshooting and resolution guide.
