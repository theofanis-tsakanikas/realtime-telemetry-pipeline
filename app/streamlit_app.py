"""Real-Time Sensor Wall — Streamlit UI.

A live operations wall over the Kafka → Spark → Redis IoT streaming pipeline.
It renders per-sensor cards with moving sparklines, a combined multi-sensor
chart, fleet KPIs, a pipeline-health strip and a data-quality panel — all
auto-refreshing.

Run locally:
    pip install -r app/requirements.txt
    streamlit run app/streamlit_app.py

Data source:
    * Demo (default) — a continuous in-process stream, no Docker/Kafka/Redis,
      faithful to the pipeline's sensors, ranges and ~20% anomaly rate.
    * Redis (live) — reads the real sensor:{id}:{metric} TimeSeries the Spark
      job writes (defaults to localhost:6379 from docker-compose).
"""

from __future__ import annotations

import os
import time

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

import sensor_data as sd

# --------------------------------------------------------------------------- #
# Page config + secrets
# --------------------------------------------------------------------------- #

st.set_page_config(page_title="Real-Time Sensor Wall", page_icon="📡", layout="wide")

try:
    for _k, _v in st.secrets.items():
        if not os.getenv(_k):
            os.environ[_k] = str(_v)
except Exception:
    pass

# --------------------------------------------------------------------------- #
# Theme — shared dark/cyan branding
# --------------------------------------------------------------------------- #

st.markdown("""
<style>
.stApp { background: linear-gradient(160deg, #060c1a 0%, #0d1b35 45%, #070e20 100%); color: #e2e8f0; }
.main .block-container { padding-top: 1.2rem; padding-bottom: 3rem; }
[data-testid="stHeader"] { background: rgba(6,12,26,0.97) !important;
    border-bottom: 1px solid rgba(56,189,248,0.10) !important; backdrop-filter: blur(16px) !important; }
[data-testid="stDecoration"] { display: none !important; }
[data-testid="stSidebar"] { background: rgba(10,18,40,0.97) !important;
    border-right: 1px solid rgba(56,189,248,0.18); }
[data-testid="stSidebar"] p, [data-testid="stSidebar"] span, [data-testid="stSidebar"] label { color: #94a3b8 !important; }
[data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3 { color: #e2e8f0 !important; }
h1, h2, h3 { color: #e2e8f0 !important; } p, li { color: #cbd5e1 !important; }
[data-testid="stMetric"] { background: rgba(30,41,59,0.7) !important;
    border: 1px solid rgba(56,189,248,0.25) !important; border-radius: 12px !important;
    padding: 0.7rem 0.9rem !important; }
[data-testid="stMetricValue"] { color: #38bdf8 !important; font-weight: 700 !important; font-size: 1.15rem !important; }
[data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.72rem !important;
    text-transform: uppercase; letter-spacing: 0.05em; }
hr { border: none !important; border-top: 1px solid rgba(56,189,248,0.15) !important; }
[data-testid="stDeployButton"], .stDeployButton { display: none !important; }
.hero { background: linear-gradient(135deg, rgba(29,78,216,0.18) 0%, rgba(14,165,233,0.10) 100%);
    border: 1px solid rgba(56,189,248,0.25); border-radius: 18px; padding: 1.3rem 1.8rem; margin-bottom: 1.3rem; }
.hero h1 { margin: 0; font-size: 1.85rem; } .hero p { margin: 0.3rem 0 0; color: #94a3b8 !important; }
.badge { display: inline-block; padding: 0.2rem 0.7rem; border-radius: 999px; font-size: 0.72rem; font-weight: 700; }
.badge-demo { background: rgba(234,179,8,0.15); color: #fde047; border: 1px solid rgba(234,179,8,0.4); }
.badge-live { background: rgba(34,197,94,0.15); color: #86efac; border: 1px solid rgba(34,197,94,0.4); }
.scard { border-radius: 14px; padding: 0.9rem 1rem; border: 1px solid rgba(56,189,248,0.2);
    background: rgba(30,41,59,0.5); margin-bottom: 0.6rem; }
.scard .title { font-weight: 700; color: #e2e8f0; font-size: 1rem; }
.dot { height: 10px; width: 10px; border-radius: 50%; display: inline-block; margin-right: 6px; }
.flow { display: flex; align-items: center; gap: 0.6rem; flex-wrap: wrap; }
.flow .node { background: rgba(30,41,59,0.6); border: 1px solid rgba(56,189,248,0.25);
    border-radius: 12px; padding: 0.7rem 1rem; text-align: center; min-width: 120px; }
.flow .node .n { font-size: 0.78rem; color: #94a3b8; } .flow .node .v { font-size: 1.2rem; font-weight: 700; color: #38bdf8; }
.flow .arrow { color: #38bdf8; font-size: 1.3rem; }
</style>
""", unsafe_allow_html=True)


# --------------------------------------------------------------------------- #
# Sidebar — controls
# --------------------------------------------------------------------------- #

with st.sidebar:
    st.markdown("### 📡 Sensor Wall")
    st.caption("Live view of the Kafka → Spark → Redis IoT pipeline.")
    st.divider()

    redis_cfg = sd.RedisConfig()
    redis_up = sd.redis_available(redis_cfg)
    source_opts = ["Demo stream", "Redis (live)"]
    source = st.radio("Data source", source_opts,
                      index=0 if not redis_up else 0,
                      help="Demo runs fully in-process. Redis reads the real "
                           "TimeSeries written by the Spark job.")
    if source == "Redis (live)":
        redis_cfg.host = st.text_input("Redis host", redis_cfg.host)
        redis_cfg.port = int(st.number_input("Redis port", value=redis_cfg.port, step=1))
        if not sd.redis_available(redis_cfg):
            st.warning("Redis not reachable — falling back to the demo stream.")

    st.divider()
    window_min = st.slider("Time window (min)", 1, 30, 8)
    refresh_s = st.slider("Refresh (sec)", 1, 10, 2)
    metric = st.selectbox("Focus metric", sd.METRICS, format_func=lambda m: f"{sd.METRIC_META[m]['icon']} {m}")
    st.divider()
    st.caption("**Pipeline:** 5 sensors → Kafka topic `sensor_data` → Spark "
               "Structured Streaming (clean + range-filter) → Redis TimeSeries.")

use_live = source == "Redis (live)" and sd.redis_available(redis_cfg)
mode_badge = ('<span class="badge badge-live">● LIVE · REDIS TIMESERIES</span>' if use_live
              else '<span class="badge badge-demo">◆ DEMO STREAM</span>')

# --------------------------------------------------------------------------- #
# Hero
# --------------------------------------------------------------------------- #

st.markdown(f"""
<div class="hero">
  <h1>📡 Real-Time Sensor Wall</h1>
  <p>Streaming IoT telemetry, cleaned in flight and served from Redis. {mode_badge}</p>
</div>
""", unsafe_allow_html=True)


# --------------------------------------------------------------------------- #
# Data fetch
# --------------------------------------------------------------------------- #

def fetch() -> pd.DataFrame:
    if use_live:
        df = sd.read_timeseries(redis_cfg, minutes=window_min)
        if not df.empty:
            return df
    return sd.backfill(minutes=window_min, step_seconds=max(1, refresh_s))


def spark_color(metric: str, value: float) -> str:
    return sd.METRIC_META[metric]["color"] if sd.in_band(metric, value) else "#ef4444"


# --------------------------------------------------------------------------- #
# Live-updating section (auto-refresh)
# --------------------------------------------------------------------------- #

@st.fragment(run_every=refresh_s)
def live_wall() -> None:
    df = fetch()
    if df.empty:
        st.info("Waiting for sensor data…")
        return

    sensors = sorted(df["sensor_id"].unique())
    samples = len(df)
    window_s = max(1, window_min * 60)
    rate = samples / window_s

    # ── KPIs ──────────────────────────────────────────────────────────────
    k1, k2, k3, k4 = st.columns(4)
    k1.metric("🟢 Active sensors", len(sensors))
    k2.metric("📈 Ingest rate", f"{rate:.1f}/s", f"{samples} samples")
    k3.metric("✅ Clean (kept)", f"{samples:,}")
    k4.metric("🧹 Anomalies rejected", f"{sd.estimate_rejects(samples):,}",
              "~20% of raw" if not use_live else "est.")

    st.divider()

    # ── Per-sensor cards with focus-metric sparkline ─────────────────────────
    st.markdown(f"##### {sd.METRIC_META[metric]['icon']} Sensor wall — {metric}")
    cols = st.columns(len(sensors))
    for col, sid in zip(cols, sensors):
        sdf = df[df["sensor_id"] == sid]
        with col:
            # current values per metric
            cur = {}
            for m in sd.METRICS:
                msdf = sdf[sdf["metric"] == m].sort_values("timestamp")
                cur[m] = float(msdf["value"].iloc[-1]) if len(msdf) else float("nan")
            focus = cur.get(metric, float("nan"))
            dot = spark_color(metric, focus)
            st.markdown(
                f'<div class="scard"><span class="dot" style="background:{dot}"></span>'
                f'<span class="title">{sid}</span></div>',
                unsafe_allow_html=True,
            )
            for m in sd.METRICS:
                meta = sd.METRIC_META[m]
                st.metric(f"{meta['icon']} {m}", f"{cur[m]:.1f} {meta['unit']}")

            series = sdf[sdf["metric"] == metric].sort_values("timestamp")
            fig = go.Figure(go.Scatter(
                x=series["timestamp"], y=series["value"], mode="lines",
                line=dict(color=sd.METRIC_META[metric]["color"], width=2), fill="tozeroy",
                fillcolor="rgba(56,189,248,0.08)",
            ))
            fig.update_layout(height=90, margin=dict(l=0, r=0, t=0, b=0),
                              paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                              xaxis=dict(visible=False),
                              yaxis=dict(visible=False, range=sd.METRIC_META[metric]["band"]))
            st.plotly_chart(fig, use_container_width=True,
                            config={"displayModeBar": False}, key=f"spark_{sid}_{metric}")

    st.divider()

    # ── Combined multi-sensor chart ──────────────────────────────────────────
    st.markdown(f"##### 📊 All sensors — {metric} over the last {window_min} min")
    big = go.Figure()
    palette = ["#38bdf8", "#f472b6", "#a78bfa", "#34d399", "#fbbf24", "#fb7185"]
    for i, sid in enumerate(sensors):
        s = df[(df["sensor_id"] == sid) & (df["metric"] == metric)].sort_values("timestamp")
        big.add_trace(go.Scatter(x=s["timestamp"], y=s["value"], mode="lines",
                                 name=sid, line=dict(color=palette[i % len(palette)], width=2)))
    lo, hi = sd.METRIC_META[metric]["band"]
    big.add_hrect(y0=lo, y1=hi, fillcolor="rgba(34,197,94,0.07)", line_width=0,
                  annotation_text="normal band", annotation_position="top left")
    big.update_layout(height=340, margin=dict(l=10, r=10, t=10, b=10),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                      font_color="#cbd5e1", legend=dict(orientation="h"),
                      xaxis=dict(gridcolor="rgba(148,163,184,0.12)"),
                      yaxis=dict(gridcolor="rgba(148,163,184,0.12)"))
    st.plotly_chart(big, use_container_width=True, key=f"big_{metric}")

    # ── Pipeline health strip ────────────────────────────────────────────────
    st.divider()
    st.markdown("##### 🩺 Pipeline health")
    keys = len(sensors) * len(sd.METRICS)
    st.markdown(f"""
    <div class="flow">
      <div class="node"><div class="n">🌡️ Simulator</div><div class="v">{len(sensors)} sensors</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">📨 Kafka topic</div><div class="v">{rate:.1f} msg/s</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">🧠 Spark clean</div><div class="v">~80% kept</div></div>
      <div class="arrow">→</div>
      <div class="node"><div class="n">⚡ Redis TS keys</div><div class="v">{keys}</div></div>
    </div>
    """, unsafe_allow_html=True)

    st.caption(f"Last update: {pd.Timestamp.now(tz='UTC').strftime('%H:%M:%S UTC')} · "
               f"refresh every {refresh_s}s · source: {'Redis' if use_live else 'demo'}")


live_wall()


# --------------------------------------------------------------------------- #
# Data quality (static reference — the cleaning contract)
# --------------------------------------------------------------------------- #

with st.expander("🧪 Data-quality contract (enforced by the Spark job)"):
    st.markdown(
        "The Spark `clean_data` step rejects ~20% of raw messages before they "
        "ever reach Redis:"
    )
    dq = pd.DataFrame([
        {"Rule": "Cast humidity", "Detail": "Only `^[0-9.]+$` strings → double, else null (drops `\"N/A\"`)"},
        {"Rule": "Drop nulls", "Detail": "sensor_id, temperature, humidity, pressure, timestamp required"},
        {"Rule": "Temperature range", "Detail": "keep 10–45 °C (rejects sentinel/outliers)"},
        {"Rule": "Humidity range", "Detail": "keep 0–100 %"},
        {"Rule": "Pressure range", "Detail": "keep 950–1050 hPa (rejects 2000–3000 outliers)"},
    ])
    st.dataframe(dq, use_container_width=True, hide_index=True)
    st.caption("Source of truth: `scripts/spark_transform.py` → `clean_data`. "
               "This wall reuses the same sensors, ranges and key scheme.")
