---
**Course:** Enterprise Real-Time Data Engineering  
**Instructor:** Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me))  
**Course Portal:** [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark)  
**Code & Labs Repository:** [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)  
---

# Troubleshooting Runbook: Local Lab Environment
## Diagnostic Solutions for Trainees Working from Home or Office Laptops
**Course:** Apache Kafka Core, Confluent Developer & Apache Spark

---

## 1. Top Issues & Quick Fixes

### Issue 1: "Connection Refused" or `TimeoutException`
* **Symptom:** You run a Python script or CLI command and it hangs for 60 seconds, then crashes with `TimeoutException` or `Connection refused`.
* **The Root Cause:** Network context mismatch.
* **The Rule:**
  * **From your laptop terminal:** Connect to `localhost:9092`.
  * **From inside a Docker container (e.g. `docker exec`):** Connect to `kafka1:9092`.
  * *Never type `localhost:9092` inside a container! Inside a container, `localhost` means the container itself, which is not listening on that port.*

---

### Issue 2: Port Conflict (`port is already allocated` / `address already in use`)
* **Symptom:** `docker compose up` fails with:
  `Bind for 0.0.0.0:5432 failed: port is already allocated` or port `8080` / `9092`.
* **The Root Cause:** You have an existing local installation of PostgreSQL, Jenkins, Tomcat, or an older Kafka instance running on your laptop.
* **The Fix:**
  Find and stop the occupying service:
  ```bash
  # Check what process is using port 5432 (Postgres) or 8080 (Web)
  sudo lsof -i :5432
  sudo lsof -i :8080

  # On Ubuntu, stop system PostgreSQL if running:
  sudo systemctl stop postgresql
  ```

---

### Issue 3: Docker Out of Memory / Containers Exit with Code 137
* **Symptom:** A container stops unexpectedly. `docker ps -a` shows `Exited (137)`.
* **The Root Cause:** Exit code 137 is the Linux kernel OOM Killer (Out Of Memory). The workstation did not allocate enough RAM to Docker.
* **The Fix:**
  * **Windows / macOS Docker Desktop:**
    Open Docker Desktop $\rightarrow$ Settings $\rightarrow$ Resources $\rightarrow$ Set Memory slider to **at least 8 GB** (10 GB recommended).
  * **Windows WSL 2 `.wslconfig`:**
    Create a file named `C:\Users\<YourUsername>\.wslconfig` with:
    ```ini
    [wsl2]
    memory=10GB
    processors=4
    ```
    Then restart WSL in PowerShell: `wsl --shutdown`.

---

### Issue 4: Docker Permission Denied (`permission denied while trying to connect to the Docker daemon socket`)
* **Symptom:** Running `docker ps` outputs `permission denied`.
* **The Fix:** Add your Linux user to the `docker` group:
  ```bash
  sudo usermod -aG docker $USER
  newgrp docker
  ```

---

### Issue 5: PySpark JVM Initialization Error (`Py4JJavaError` / `JAVA_HOME is not set`)
* **Symptom:** Running `python -c "import pyspark"` throws `Java not found` or `JAVA_HOME is not set`.
* **The Fix:**
  1. Confirm OpenJDK 17 is installed:
     ```bash
     sudo apt install -y openjdk-17-jdk-headless
     ```
  2. Set `JAVA_HOME` in your `~/.bashrc`:
     ```bash
     echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
     echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
     source ~/.bashrc
     ```

---

### Issue 6: Cleaning and Resetting the Entire Stack (The Nuclear Option)
If your topics, offsets, or schemas get hopelessly corrupted:
```bash
cd 00_Lab

# Stop all containers and wipe all lab volumes cleanly
make reset

# Start fresh
make up
make check
```


---
*Enterprise Real-Time Data Engineering · Instructor: Mohamed Fouad Fakhruldeen ([Fakhr.me](https://fakhr.me)) · Course Materials: [fakhr.me/KafkaSpark](https://fakhr.me/KafkaSpark) · Repository: [github.com/fakhruldeen/KafkaSpark](https://github.com/fakhruldeen/KafkaSpark)*
