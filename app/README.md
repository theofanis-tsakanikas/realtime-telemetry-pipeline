# 📡 Real-Time Sensor Wall

A live operations wall over the **Kafka → Spark → Redis** IoT streaming
pipeline, built with **Streamlit**. It auto-refreshes per-sensor cards with
moving sparklines, a combined multi-sensor chart, fleet KPIs, a pipeline-health
strip and the data-quality contract.

It shares the dark/cyan branding of the `multi-cloud-self-healing-agent` and the
fleet command center, so the portfolio reads as one coherent product suite.

> Built for presentations & promo recordings: demo mode runs **fully in-process**
> — no Docker, Kafka, Spark or Redis needed — yet stays faithful to the real
> sensors, ranges, key scheme and ~20% anomaly rate.

---

## Quick start (demo mode)

```bash
cd app
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
streamlit run streamlit_app.py
```

Open http://localhost:8501. The demo stream is the default. Use the sidebar to
change the time window, refresh rate and focus metric.

## Live mode (Redis)

Bring the pipeline up (`./run.sh` / docker-compose) so the Spark job is writing
`sensor:{id}:{metric}` TimeSeries to Redis, then pick **"Redis (live)"** in the
sidebar. It connects to `localhost:6379` by default (the docker-compose port
mapping). If Redis is unreachable, the wall falls back to the demo stream.

---

## How faithful is the demo?

[`sensor_data.py`](sensor_data.py) mirrors the real pipeline:

* **Sensors & metrics** — 5 sensors, `temperature` / `humidity` / `pressure`, as
  in [`scripts/sensor_simulator.py`](../scripts/sensor_simulator.py).
* **Clean ranges** — values are clamped to the exact post-filter ranges enforced
  by `clean_data` in [`scripts/spark_transform.py`](../scripts/spark_transform.py)
  (temp 10–45 °C, humidity 0–100 %, pressure 950–1050 hPa).
* **Key scheme** — live mode reads `sensor:{id}:{metric}` via `TS.RANGE`,
  exactly the keys the Spark Redis sink creates.
* **Anomaly rate** — the rejected-anomalies KPI uses the simulator's ~20% rate.

## Files

| File | Purpose |
|---|---|
| `streamlit_app.py` | UI: KPIs, sensor cards, combined chart, pipeline-health, DQ panel (auto-refresh) |
| `sensor_data.py` | Data layer: demo stream synthesis + Redis TimeSeries reader (one interface) |
| `requirements.txt` | UI dependencies (light; no Spark/Kafka) |
| `.streamlit/config.toml` | Dark/cyan theme matching the rest of the portfolio |
