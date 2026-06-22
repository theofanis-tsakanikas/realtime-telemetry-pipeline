# IoT Streaming Pipeline — Engineering Reference

Real-time pipeline: IoT sensor simulation → Apache Kafka → Spark Structured Streaming → Redis TimeSeries → Grafana.

---

## Repo Structure

```
kafka-spark-redis-streaming-etl/
├── .github/
│   └── workflows/
│       └── ci.yml              # Ruff linting + pytest on push and PR
├── docker/
│   ├── Dockerfile.simulator    # Python 3.12-slim image for the sensor producer
│   └── Dockerfile.spark        # Bitnami Spark 3.5 image for the PySpark job
├── infra/
│   ├── docker-compose.yml      # Orchestrates all 7 services (Kafka runs in KRaft mode)
│   └── grafana/
│       ├── provisioning/
│       │   ├── datasources/redis.yml    # Auto-provisions Redis datasource on startup
│       │   └── dashboards/provider.yml  # Tells Grafana where to find dashboard JSON
│       └── dashboards/
│           └── iot-sensors.json         # Provisioned dashboard (8 panels incl. Data Quality & Drift)
├── app/                        # Streamlit "Sensor Wall" — standalone deployable (own pinned reqs)
│   ├── sensor_data.py          # Data layer: in-process demo synth + Redis (live) reader
│   ├── streamlit_app.py        # Live auto-refreshing UI
│   └── requirements.txt        # App-only dependencies (pinned)
├── scripts/
│   ├── sensor_simulator.py     # Confluent Kafka producer; emits ~20% anomalous sensor data
│   ├── metrics_spec.py         # Sensor data contract: valid ranges + normal-operation baselines (single source of truth)
│   ├── data_quality.py         # Per-batch data-quality metrics (accept rate, rejections by reason) → Redis TS
│   ├── drift.py                # Statistical drift detection (z-test of batch mean vs baseline) → Redis TS
│   └── spark_transform.py      # PySpark job; valid → Redis, rejected → DLQ topic, DQ+drift → Redis observability
├── tests/
│   ├── conftest.py             # Session-scoped SparkSession fixture (tmp warehouse/derby — no repo leak)
│   ├── test_spark_transform.py # clean_data() unit tests
│   ├── test_clean_data_extra.py# clean_data() range/boundary + from_json schema enforcement
│   ├── test_rejected_data.py   # rejected_data() reasons + clean/rejected partition the input
│   ├── test_data_quality.py    # data-quality metrics
│   ├── test_drift.py           # drift detector
│   ├── test_redis_sink.py      # write_row()/key building vs a mocked Redis client
│   ├── test_sensor_simulator.py# generate_sensor_data() schema, anomaly rates, determinism
│   └── test_app_sensor_data.py # Streamlit data layer + contract-drift guard vs metrics_spec
├── .env.example                # Template — copy to .env before building Docker images
├── .pre-commit-config.yaml     # Optional local ruff + hygiene hooks
├── Makefile                    # Task runner (wraps run.sh; adds test, coverage, lint, clean)
├── pyproject.toml              # Project metadata + ruff / pytest / coverage config (replaces pytest.ini)
├── requirements.txt            # Runtime Python dependencies (used by Docker images)
├── requirements-dev.txt        # Dev-only dependencies: pytest, pytest-cov, ruff, pandas
├── run.sh                      # Docker Compose wrapper (up/down/build/logs/ps/restart)
└── setup.sh                    # One-time local dev environment setup
```

---

## Service URLs and Ports

| Service | URL / Address | Notes |
|---|---|---|
| Grafana | http://localhost:3000 | Login: admin / admin |
| Kafka-UI | http://localhost:8085 | Topic browser, consumer group offsets |
| RedisInsight (embedded) | http://localhost:8001 | Built into redis-stack image |
| RedisInsight (standalone) | http://localhost:5540 | Separate container; same Redis instance |
| Spark UI | http://localhost:4040 | Available only while spark-processor is running; binds after first micro-batch |
| Redis | localhost:6379 | Direct TCP — use `redis-cli` or RedisInsight |
| Kafka broker (external) | localhost:29092 | For producers/consumers running on your host machine |
| Kafka broker (internal) | kafka-broker:9092 | Docker-network-only; used by the simulator and Spark containers |

---

## Prerequisites

- **Docker Desktop** with **≥ 8 GB RAM** allocated to Docker. Kafka (KRaft) + Spark together are memory-hungry. 6 GB is the absolute minimum; 8 GB is recommended for stable operation.
- **Docker Compose V2** — the `run.sh` and Makefile use `docker compose` (no hyphen). Verify with `docker compose version`.
- **Python 3.12** — for local test execution. Only needed for `make test` and `make lint`; not required if you only run the Docker stack.
- **Java 17** — required locally for `make test`. Install via `brew install openjdk@17`. The Makefile sets `JAVA_HOME=/opt/homebrew/opt/openjdk@17` automatically so no manual export is needed after installation. Add the following to `~/.zshrc` for `java` to work in your terminal outside of make:
  ```bash
  export JAVA_HOME=/opt/homebrew/opt/openjdk@17
  export PATH="$JAVA_HOME/bin:$PATH"
  ```

---

## Quick Start

```bash
# 1. One-time setup: creates .venv, installs deps, creates data/ dirs, copies .env.example → .env
chmod +x setup.sh run.sh
./setup.sh

# 2. Build the custom simulator and spark-processor Docker images
make build          # or: ./run.sh build

# 3. Start the full stack in the background
make start          # or: ./run.sh up

# 4. Verify all containers are running
make ps             # or: ./run.sh ps

# 5. Stream live logs from all containers
make logs           # or: ./run.sh logs    (Ctrl+C to detach without stopping containers)

# 6. Tear down the stack
make stop           # or: ./run.sh down
```

> **Note:** Configuration is injected at runtime via docker-compose `environment:` blocks —
> the `.env` file is only used when running the Python scripts locally on the host.

---

## Makefile Targets

| Target | Action |
|---|---|
| `make start` | Start all containers (`./run.sh up`) |
| `make stop` | Stop and remove containers (`./run.sh down`) |
| `make build` | Build custom Docker images (`./run.sh build`) |
| `make restart` | Restart all containers |
| `make logs` | Stream live container logs |
| `make ps` | Show container statuses |
| `make test` | Run pytest unit tests via local `.venv` |
| `make coverage` | Run pytest with a coverage report (terminal + `htmlcov/`) |
| `make lint` | Run ruff linter on `scripts/`, `tests/`, and `app/` |
| `make clean` | Stop containers and clear `data/checkpoints` and `data/logs` |

---

## Data Flow (End-to-End)

```
sensor_simulator.py
  │  Confluent Kafka producer
  │  5 sensors, ~1 msg/sec each, ~20% deliberate anomaly rate:
  │    - temperature=None         (5% of messages)
  │    - humidity="N/A"           (5% of messages)
  │    - pressure=2000–3000 hPa   (5% of messages, extreme outlier)
  │    - missing timestamp key    (5% of messages)
  ▼
Kafka topic: sensor_data  (1 partition, replication factor 1)
  │  Spark reads with startingOffsets=latest
  ▼
spark_transform.py — clean_data()
  │  1. Schema enforcement    — JSON parsed with static StructType; bad types become null
  │  2. Regex cleaning        — humidity matched against ^[0-9.]+$; "N/A" etc. → null
  │  3. Timestamp parsing     — ISO-8601 string → Spark TimestampType via to_timestamp()
  │  4. Null drop             — drops rows missing any of: sensor_id, temperature,
  │                             humidity, pressure, timestamp
  │  5. Range filter          — temperature: 10–45°C  |  humidity: 0–100%  |  pressure: 950–1050 hPa
  ▼
spark_transform.py — rejected_data()  [parallel DLQ branch]
  │  Complement of clean_data(); adds rejection_reason (first violated rule)
  │  → Kafka topic sensor_data_rejected (inspect in Kafka-UI)
  │
spark_transform.py — write_observability()  [3rd foreachBatch on the parsed stream]
  │  data_quality.py: per-batch total / valid / accept_rate / rejected-by-reason → dq:* TS keys
  │  drift.py: per-metric z-test of the batch mean vs the metrics_spec baseline
  │            (catches a miscalibrated sensor whose readings are still individually valid)
  │            → drift:{metric}:z / :mean TS keys; >3σ raises a drift alert
  │  Errors are logged, never raised — observability can't take down ingestion
  │
spark_transform.py — write_to_redis()  [via foreachBatch + foreachPartition]
  │  One pipelined Redis connection per Spark partition (flushed every 500 commands)
  │  TS.ADD sensor:{id}:{metric} {timestamp_ms} {value}
  │         RETENTION 604800000 ON_DUPLICATE LAST LABELS sensor_id {id} metric {name}
  │  (TS.ADD auto-creates the series on first write; ON_DUPLICATE LAST = idempotent replays)
  ▼
Redis TimeSeries (redis-stack)
  │  15 keys total (5 sensors × 3 metrics)
  │  7-day retention; labeled for MRANGE/MGET filtering by metric or sensor_id
  ▼
Grafana — redis-datasource plugin
  │  Datasource auto-provisioned from infra/grafana/provisioning/datasources/redis.yml
  │  Queries: TS.MRANGE, TS.GET, TS.MGET with FILTER labels
  ▼
Grafana dashboard (infra/grafana/dashboards/iot-sensors.json)
```

---

## Running Tests Locally

Tests cover the transformation/observability logic (`clean_data`, `rejected_data`,
data-quality, drift, the Redis sink command-building, the simulator, and the Streamlit data
layer). They use a local SparkSession (`local[*]`) and a mocked Redis client, and require
**no running containers** — no Kafka, no Redis, no Docker.

```bash
# Activate venv first (setup.sh creates it)
source .venv/bin/activate

make test    # runs: pytest tests/ -v
make lint    # runs: ruff check scripts/ tests/ app/
```

The Spark Kafka connector JAR is **not** downloaded during tests. The test SparkSession is
initialized once per session (session-scoped fixture) for performance.

**Test coverage of `clean_data()`:**

| Test | Scenario |
|---|---|
| `test_valid_row_passes_through` | All valid values → 1 row out |
| `test_null_temperature_is_dropped` | `temperature=None` → 0 rows |
| `test_humidity_string_na_is_dropped` | `humidity="N/A"` → cleaned to null → 0 rows |
| `test_valid_numeric_humidity_string_is_cast` | `humidity="45.5"` → 1 row, value is float 45.5 |
| `test_null_timestamp_is_dropped` | `timestamp=None` → 0 rows |
| `test_temperature_below_minimum_is_filtered` | `temperature=9.9` → 0 rows |
| `test_temperature_above_maximum_is_filtered` | `temperature=45.1` → 0 rows |
| `test_pressure_outlier_filtered` | `pressure=2500.0` (simulator anomaly) → 0 rows |
| `test_temperature_at_boundary_values` | `temp=10.0` and `temp=45.0` → both pass (inclusive) |
| `test_mixed_batch_correct_count` | 5 rows: 3 valid + 2 anomalies → 3 rows out |

---

## Grafana Dashboard Setup

The Redis datasource (uid `redis-iot`) and the dashboard are **automatically provisioned** on
`docker compose up` via the files in `infra/grafana/provisioning/` and
`infra/grafana/dashboards/iot-sensors.json`. The committed dashboard has 8 panels — the
sensor readings plus a **Data Quality & Drift** row backed by the observability metrics the
Spark job publishes to Redis each batch:

| Panel | Type | Query |
|---|---|---|
| All Sensors — Temperature | timeseries | `TS.MRANGE` filter `metric=temperature`, legend by `sensor_id` |
| Sensor 1 — Current Temperature | gauge | `TS.GET sensor:sensor_1:temperature` |
| Sensor 1 — Current Humidity | stat | `TS.GET sensor:sensor_1:humidity` |
| Sensor 1 — Pressure Trend | timeseries | `TS.RANGE sensor:sensor_1:pressure` |
| All Sensors — Humidity | timeseries | `TS.MRANGE` filter `metric=humidity`, legend by `sensor_id` |
| Data Quality — Accept Rate | gauge | `TS.GET dq:accept_rate` |
| Rejections by Reason | timeseries | `TS.MRANGE` filter `series=rejection`, legend by `metric` |
| Drift — Z-Score per Metric | timeseries | `TS.MRANGE` filter `kind=drift series=z`, 3σ threshold band |

Grafana polls the dashboard directory every 30 seconds (`updateIntervalSeconds: 30` in
`provider.yml`), so edits to the JSON file reload automatically while the stack is running —
no container restart needed. To change panels in the UI and persist them, export via
**Dashboard Settings → JSON Model** and overwrite the file.

> **Note on first boot:** `GF_INSTALL_PLUGINS=redis-datasource` causes Grafana to download the
> plugin from the Grafana plugin registry on startup. This adds ~30–60 seconds to the first
> `make start`. Subsequent starts are instant (plugin is cached in the container layer).

---

## Known Failure Modes

### Spark crashes on first run
Spark downloads the Kafka connector JAR (`spark-sql-kafka-0-10_2.12:3.5.1`, ~200 MB) from Maven
Central at startup. This requires outbound internet access from Docker.

```bash
docker logs spark-processor | head -40
# Look for lines containing "Downloading" or "Unable to resolve"
```

### Simulator exits before Kafka is ready
The broker takes 10–15 seconds to elect a leader after the container starts. The simulator retries
10 × 3 seconds (30s total). If it still fails after 30s, Kafka itself likely failed to start:

```bash
docker logs kafka-broker | tail -20
docker logs simulator | tail -20
```

### Grafana shows "No data" on panels
Check in order:
1. **Redis has keys:** Open RedisInsight → run `TS.KEYS *` — you should see 15 keys
2. **Spark is processing:** `docker logs spark-processor | grep -E "(Batch|ERROR)"`
3. **Datasource is reachable:** Grafana → Connections → Data Sources → Redis → Test

### Spark UI not loading at localhost:4040
Port 4040 only binds after the first micro-batch completes (~20–30 seconds after Spark starts).
Refresh the page after waiting.

### Port conflict on startup
If any port is already in use (3000, 4040, 5540, 6379, 8001, 8085, 9092, 29092), edit the
left-hand port number in `infra/docker-compose.yml` and update this file accordingly.

### DLQ topic is empty
The `sensor_data_rejected` topic only receives rows after the simulator has emitted anomalies
(~20% of messages). Check Kafka-UI (http://localhost:8085) → Topics → `sensor_data_rejected`
and inspect the `rejection_reason` field of each message.
