<div style="background-color:#fff8e7; color:#2b2b2b; padding:20px; border-radius:10px;">

# 📡 Real-Time IoT Data Pipeline: Kafka ➔ Spark ➔ Redis ➔ Grafana

[![CI](https://github.com/theofanis-tsakanikas/kafka-spark-redis-streaming-etl/actions/workflows/ci.yml/badge.svg)](https://github.com/theofanis-tsakanikas/kafka-spark-redis-streaming-etl/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](./LICENSE)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark%203.5-E25A1C?logo=apachespark&logoColor=white)
![Redis](https://img.shields.io/badge/Redis%20Stack-DC382D?logo=redis&logoColor=white)
![Grafana](https://img.shields.io/badge/Grafana-F46800?logo=grafana&logoColor=white)

![Project Overview](./images/kafka-spark-redis-streaming-etl.png)

This project demonstrates a robust, scalable real-time IoT data processing pipeline. It simulates data from multiple environmental sensors, streams it through `Apache Kafka`, processes it using `Apache Spark` Structured Streaming, stores time-series data in `Redis`, and visualizes live insights via `Grafana` dashboards.

The entire infrastructure is containerized using `Docker` and managed with convenient shell scripts.

---

## 📑 Table of Contents

- [What This Demonstrates](#-what-this-demonstrates)
- [Key Features](#-key-features)
- [Data Engineering & Transformation (PySpark)](#-data-engineering--transformation-pyspark)
- [Infrastructure Ecosystem (Docker Compose)](#-infrastructure-ecosystem-docker-compose)
- [CLI Automation Wrapper (run.sh)](#-cli-automation-wrapper-runsh)
- [Project Structure](#-project-structure)
- [Quick Start Guide](#-quick-start-guide)
- [Data Validation & Verification](#-data-validation--verification)
- [Grafana Visualization Dashboards](#-grafana-visualization-dashboards)
- [Project Shutdown](#-project-shutdown)
- [License](#-license)

> For a deeper engineering reference — service ports, end-to-end data flow, test coverage, and known failure modes — see [CLAUDE.md](./CLAUDE.md).

---

## 🎯 What This Demonstrates

This is a portfolio project built to demonstrate end-to-end **real-time data engineering** skills across a modern streaming stack:

- **Streaming architecture:** Designing a decoupled producer → broker → stream-processor → store → dashboard pipeline, where each stage scales independently.
- **Stateful stream processing:** Using Spark Structured Streaming with micro-batches, checkpointing for fault tolerance, and a connection-per-partition sink pattern.
- **Data quality engineering:** Schema enforcement, regex sanitisation of dirty fields, null handling, and range-based outlier filtering — validated by a unit-tested transformation function.
- **Time-series storage modelling:** Labelled Redis TimeSeries keys with retention policies, queryable by metric or sensor.
- **Production-minded tooling:** Containerised infrastructure, a one-command developer workflow, automated linting and tests in CI, and infrastructure-as-code Grafana provisioning.

---

## 🚀 Key Features

* **IoT Simulation:** Python-based simulator generating temperature, humidity, and pressure data for 5 distinct sensors.
* **Scalable Messaging:** Utilizes Apache Kafka as a high-throughput, distributed event streaming platform.
* **Real-Time Processing:** Implements Apache Spark Structured Streaming for stateful time-series transformations.
* **Data Validation & Cleaning:** Drops corrupted values and validates ranges using PySpark functions.
* **NoSQL Storage:** Leverages Redis with the TimeSeries module for efficient metric storage and retrieval.
* **Interactive Visualization:** Real-time dashboards created in Grafana, showcasing network-wide trends and specific sensor deep dives.
* **Containerized Infrastructure:** Full Docker deployment using docker-compose.

---

## 🛠️ Data Engineering & Transformation (PySpark)

The core of the project is the Apache Spark Structured Streaming job (`scripts/spark_transform.py`). Instead of blindly moving data, it applies enterprise-level engineering practices:

* **Strict Schema Enforcement:** Incoming JSON payloads are parsed using static Spark StructType fields.
* **Regex Data Cleaning:** Sanitizes messy string fields (e.g., checks if humidity is a valid number via regex) before casting to double.
* **Automated Range Outlier Filtering:** Drops rows with null values and applies range-checks to filter out corrupted hardware readings:
  * Temperature: 10°C to 45°C
  * Humidity: 0% to 100%
  * Pressure: 950hPa to 1050hPa
* **Native RedisTimeSeries Sink (High Performance):** Uses Spark’s `.foreachPartition()` to open one pipelined connection per partition (Spark Best Practice) and pushes metrics via native `TS.ADD` commands, bypassing expensive ORMs.
* **Dead-Letter Queue (Observability):** Rows that fail validation are not silently dropped — they are routed to a `sensor_data_rejected` Kafka topic tagged with a `rejection_reason` (e.g. `invalid_humidity`, `pressure_out_of_range`), so data-quality issues are inspectable in Kafka-UI.

---

## 🐳 Infrastructure Ecosystem (Docker Compose)

The environment spin-ups a fully integrated telemetry-processing stack. All services live in a dedicated Docker bridge network (`stream-net`):

* **Event Bus:** Apache Kafka in single-node KRaft mode (Confluent OSS image) — no Zookeeper.
* **Storage:** Redis Stack (includes native support for time-series and RedisInsight).
* **Processors:** Custom Docker images (multi-stage builds) for the Python Sensor Simulator and PySpark Workers.
* **Monitoring:** Grafana UI, Kafka-UI (Provectus), and RedisInsight for live visual debugging of message offsets, topic traffic, and RedisTimeSeries keys.

---

## 🔧 CLI Automation Wrapper (run.sh)

To abstract away complex Docker Compose commands and make developer onboarding seamless, a POSIX-compliant shell wrapper is provided:

* `./run.sh up` - Spins up the entire ecosystem in detached mode.
* `./run.sh down` - Tears down the stack and releases ports.
* `./run.sh build` - Recompiles custom Dockerfiles for spark & simulators.
* `./run.sh logs` - Attached stream to observe real-time pipeline print statements.
* `./run.sh ps` - View active running container statuses.

---

## 📂 Project Structure
```text
kafka-spark-redis-streaming-etl/
├── .venv/                   # Python Virtual Environment (Local)
├── data/                    # Local volume storage for logs & checkpoints
│   ├── checkpoints/         # Spark Structured Streaming checkpoints
│   └── logs/                # Simulator and processing logs
├── docker/                  # Dockerfiles for custom images
│   ├── Dockerfile.simulator # Image for the Python Sensor Simulator
│   └── Dockerfile.spark     # Image for Apache Spark Processing
├── images/                  # Directory for project screenshots
├── infra/                   # Infrastructure configuration
│   └── docker-compose.yml   # Main Docker Compose orchestration file
├── scripts/                 # Source code scripts
│   ├── sensor_simulator.py  # Python producer simulating IoT devices
│   └── spark_transform.py   # PySpark job managing streaming ETL
├── .env                     # Configuration file for environment variables (ignored by git)
├── .env.example             # Template for environment configuration
├── .gitignore               # Files excluded from Git version control
├── LICENSE                  # Project License
├── README.md                # Project documentation
├── requirements.txt         # Local Python dependencies (for setup)
├── run.sh                   # Utility script for Docker management
└── setup.sh                 # Initial environment setup script
```
---

## ⚙️ Quick Start Guide

This project includes automated scripts to make deployment seamless.

### 1. Initial Setup

Clone the repository and prepare the environment. This will create your local Python environment, install dependencies, create local data directories for Docker volumes, and prepare your .env file.
```bash
git clone https://github.com/yourusername/kafka-spark-redis-streaming-etl.git
cd kafka-spark-redis-streaming-etl
```
```bash
# Give execution permissions to the scripts (Only needs to be done once)
chmod +x setup.sh run.sh
```
```bash
# Run the setup
./setup.sh
```
### 2. Configure Environment Variables

The `setup.sh` script automatically creates a local .env file by copying the `.env.example` template. 

By default, the template is pre-configured for **Full Docker Integration** (running both the infrastructure and the Python scripts inside the Docker network). 

If you decide to run your Python scripts **locally on your host machine** (outside Docker), make sure to update the hosts in your .env:

* Change `KAFKA_BROKER=kafka-broker:9092` to `KAFKA_BROKER=localhost:29092`
* Change `REDIS_HOST=redis` to `REDIS_HOST=localhost`

### 3. Build Custom Docker Images

We need to build the images for the simulator and the Spark processor before launching the full stack.
```bash
./run.sh build
```
### 4. Start the Infrastructure

Start all containers in the background using the run.sh script.
```bash
./run.sh up
```
### 5. Verify Running Services

Check the status of all containers to ensure they are healthy.
```bash
./run.sh ps
```
---

## 🔍 Data Validation & Verification

We can verify that data is flowing correctly at each stage of the pipeline.

### 1. Kafka Ingestion (Proof of Producing)

You can view the live messages flowing into the Kafka topic. 

**Verification Screenshot:**

Below is a view of the Kafka Topic messages, displaying the first 15 JSON messages ingested from various simulated sensors.

![Kafka Live Messages](./images/kafka-live-messages.png)

### 2. RedisTimeSeries Storage (Proof of Sink)

After Spark processes the data, we can verify that the time-series keys are populated in Redis.

**Verification Visualization (via RedisInsight Charting):**

This screenshot shows: 
1. The temperature data across all 5 sensors using the filter `TS.MRANGE - + FILTER metric=temperature`.
2. The last reading for Sensor 1 temperature using the `TS.GET sensor:sensor_1:temperature`.

![Redis GET Query](./images/redis-get-query.png)

---

## 📊 Grafana Visualization Dashboards

The final stage is visualization. Access Grafana at `http://localhost:3000` (default login: admin/admin). Data sources and dashboards are pre-configured.

### Dashboard 1: "All Sensors Temperature"

This dashboard focuses on network-wide trends, displaying real-time temperature data from all 5 sensors on a single time-series chart.

![Grafana All Sensors](./images/grafana-all-sensors.png)

### Dashboard 2: "Technical Panel Edit Mode"

This screenshot is taken inside the panel edit mode, proving that Grafana is querying Redis using the specialized Redis Data Source and the `TS.MRANGE` command.

![Grafana Panel Edit Proof](./images/grafana-panel.png)

### Dashboard 3: "Sensor 1 Deep Dive (Last 5 min)"

This dashboard is dedicated to the granular analysis of a single sensor (Sensor 1), demonstrating comprehensive monitoring capabilities.

**Components:**
* **Current Temperature:** Rendered in a real-time Gauge.
* **Current Humidity:** Displayed as a large Stat number.
* **Pressure Trend (Last 5 min):** A detailed Time Series line graph showing the recent history of atmospheric pressure.

![Grafana Sensor Deep Dive](./images/grafana-sensor1-deep-dive.png)

---

## 🛑 Project Shutdown

When you are finished, stop and remove all containers.
```bash
./run.sh down
```
---

## 📜 License

This project is licensed under the LICENSE file.