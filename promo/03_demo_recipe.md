# Promo Demo — Recipe (how to make the stream look alive on camera)

Two ways to film the wall. **Demo mode** is the safe default (no Docker, no cold-start risk).
**Live mode** adds a real "Kafka→Spark→Redis" beat if you want it. Both look identical on screen.

## TL;DR
| Choice | Value | Why |
|---|---|---|
| **Mode** | **Demo stream** | Zero infra, no cold-start, fully repeatable. |
| **Refresh** | **1–2s** | Visible motion without flicker. |
| **Window** | **8 min** | Full sparklines; lines have shape. |
| **Focus metric** | **temperature** | Most legible movement and band-crossings. |
| **Warm-up** | **~60s before recording** | So no chart is half-empty. |

## Option A — Demo mode (recommended for the hero)
```
cd app
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```
Open http://localhost:8501 → **Demo stream**. The values are a continuous function of wall-clock
time, so the series **extends naturally** across refreshes — it genuinely moves. Let it run ~1
minute, then record.

### What's real vs synthesised
- **Real:** the sensors (5), metrics, the **clean ranges** Spark enforces (temp 10–45 °C, humidity
  0–100 %, pressure 950–1050 hPa), the `sensor:{id}:{metric}` key scheme, and the ~20% anomaly rate
  — all mirror `scripts/sensor_simulator.py` + `scripts/spark_transform.py`.
- **Synthesised:** the in-process value stream (so no Kafka/Spark/Redis needed).
- **Caption honesty:** keep the **"◆ DEMO STREAM"** badge visible; say "the pattern scales", not
  "production throughput".

## Option B — Live mode (the real stack)
```
./run.sh up        # brings up Kafka, Spark, Redis, Grafana, Kafka-UI, RedisInsight
./run.sh ps        # confirm all healthy
./run.sh logs      # watch until: "Spark Streaming Job Initialized" + batches processing
```
Give it ~1–2 minutes so Redis TimeSeries fill, then in the wall pick **"Redis (live)"**
(host `localhost`, port `6379`). Badge flips to **"● LIVE · REDIS TIMESERIES"**.

Supporting UIs you can cut to (all from `infra/docker-compose.yml`):
- **Grafana** — http://localhost:3000 (admin/admin) — the provisioned real-time dashboards.
- **Kafka-UI** — http://localhost:8085 — show the `sensor_data` topic and live message flow.
- **RedisInsight** — http://localhost:5540 — the TimeSeries keys (`sensor:*:*`).
- **Spark UI** — http://localhost:4040 — the streaming query / micro-batches.

### After the shoot
```
./run.sh down      # stop and remove all containers
```

## Filming tips specific to streaming
- The motion is the asset — **record live refresh ticks**, never a static frame with a fake zoom.
- Time a take so a metric **crosses out of band** (red dot) — it's a great 1-second story beat.
- If filming live, **warm everything up off-camera**; never record a cold-start or a connection error.
