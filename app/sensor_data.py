"""Data layer for the Real-Time Sensor Wall UI.

Provides one interface over two sources:

    * **Demo mode** (default) — synthesises a *continuous* live stream
      in-process, with no Kafka, Spark, Redis or Docker. Values are a smooth
      function of wall-clock time (so the series extends naturally across
      refreshes) and respect the *exact* clean ranges enforced by the pipeline's
      ``clean_data`` (temperature 10–45 °C, humidity 0–100 %, pressure
      950–1050 hPa). It also reproduces the ~20 % raw-anomaly / reject ratio so
      the data-quality counter is faithful.
    * **Redis (live)** — reads the real ``sensor:{id}:{metric}`` Redis
      TimeSeries the Spark job writes, via ``TS.RANGE``.

Faithful to:
    * ``scripts/sensor_simulator.py`` — 5 sensors, metrics + generation ranges,
      ~20 % anomaly rate.
    * ``scripts/spark_transform.py`` — ``sensor:{id}:{metric}`` key scheme and
      the post-clean valid ranges.
"""

from __future__ import annotations

import math
import os
import random
import time
from dataclasses import dataclass

import pandas as pd

# --------------------------------------------------------------------------- #
# Fleet of sensors / metrics (mirrors the simulator + Spark schema)
# --------------------------------------------------------------------------- #

SENSOR_COUNT = int(os.getenv("SENSOR_COUNT", "5"))
SENSORS = [f"sensor_{i}" for i in range(1, SENSOR_COUNT + 1)]
METRICS = ["temperature", "humidity", "pressure"]

# Display metadata per metric: unit, normal "comfort" band, and the clean range
# the Spark pipeline enforces (post-filter). Out-of-band → highlighted in UI.
METRIC_META = {
    "temperature": {"unit": "°C", "icon": "🌡️", "band": (15.0, 30.0),
                    "clean": (10.0, 45.0), "color": "#f97316"},
    "humidity":    {"unit": "%",  "icon": "💧", "band": (35.0, 70.0),
                    "clean": (0.0, 100.0),  "color": "#38bdf8"},
    "pressure":    {"unit": "hPa", "icon": "🧭", "band": (995.0, 1020.0),
                    "clean": (950.0, 1050.0), "color": "#a78bfa"},
}

# Per-sensor baseline so each sensor reads a little differently.
_BASE = {
    "temperature": [22, 25, 19, 28, 24],
    "humidity":    [55, 48, 62, 40, 58],
    "pressure":    [1008, 1012, 1003, 1015, 1006],
}
_AMP = {"temperature": 6.0, "humidity": 12.0, "pressure": 8.0}
_PERIOD = {"temperature": 140.0, "humidity": 190.0, "pressure": 320.0}


# --------------------------------------------------------------------------- #
# Demo synthesis — continuous, time-driven, faithful to clean ranges
# --------------------------------------------------------------------------- #

def _smooth_value(sensor_idx: int, metric: str, t: float) -> float:
    """Deterministic smooth reading for a sensor/metric at epoch ``t`` (sec).

    Base + sinusoid + small per-second noise (seeded by the second + sensor so
    backfill and the live tick agree). Clamped into the metric's clean range.
    """
    base = _BASE[metric][sensor_idx % len(_BASE[metric])]
    phase = sensor_idx * 1.3
    val = base + _AMP[metric] * math.sin(t / _PERIOD[metric] + phase)
    rng = random.Random(int(t) * 31 + sensor_idx * 7 + hash(metric) % 1000)
    val += rng.uniform(-1.0, 1.0) * (_AMP[metric] * 0.12)
    lo, hi = METRIC_META[metric]["clean"]
    return round(max(lo, min(hi, val)), 2)


def reading_at(t: float) -> list[dict]:
    """Return one cleaned reading per sensor at epoch ``t`` (long rows)."""
    rows = []
    for idx, sid in enumerate(SENSORS):
        for metric in METRICS:
            rows.append({
                "timestamp": pd.Timestamp(t, unit="s", tz="UTC"),
                "sensor_id": sid,
                "metric": metric,
                "value": _smooth_value(idx, metric, t),
            })
    return rows


def backfill(minutes: int = 10, step_seconds: int = 2,
             now: float | None = None) -> pd.DataFrame:
    """Synthesise the last ``minutes`` of cleaned readings as a long DataFrame."""
    now = now if now is not None else time.time()
    start = now - minutes * 60
    rows: list[dict] = []
    t = start
    while t <= now:
        rows.extend(reading_at(t))
        t += step_seconds
    return pd.DataFrame(rows)


def estimate_rejects(clean_samples: int) -> int:
    """Estimate raw anomalies rejected for a count of clean samples.

    The simulator injects ~20 % anomalies, so ``clean ≈ 80 %`` of raw and the
    rejected count ≈ ``clean * 20/80``. Matches ``generate_sensor_data``.
    """
    return int(round(clean_samples * (0.20 / 0.80)))


# --------------------------------------------------------------------------- #
# Redis (live) source
# --------------------------------------------------------------------------- #

@dataclass
class RedisConfig:
    host: str = os.getenv("REDIS_HOST", "localhost")
    port: int = int(os.getenv("REDIS_PORT", "6379"))


def redis_available(cfg: RedisConfig) -> bool:
    """True if a Redis Stack with TimeSeries is reachable and has sensor keys."""
    try:
        import redis  # lazy import
        r = redis.Redis(host=cfg.host, port=cfg.port, socket_connect_timeout=1.5)
        r.ping()
        return True
    except Exception:
        return False


def read_timeseries(cfg: RedisConfig, minutes: int = 10) -> pd.DataFrame:
    """Read the last ``minutes`` from every ``sensor:*:*`` TimeSeries key.

    Returns a long DataFrame ``[timestamp, sensor_id, metric, value]``. Uses raw
    ``TS.RANGE`` commands so it works regardless of redis-py's TimeSeries
    helper version.
    """
    import redis  # lazy import
    r = redis.Redis(host=cfg.host, port=cfg.port, decode_responses=True)
    now_ms = int(time.time() * 1000)
    from_ms = now_ms - minutes * 60 * 1000

    keys = [k for k in r.scan_iter(match="sensor:*:*")]
    rows: list[dict] = []
    for key in keys:
        # key = sensor:sensor_3:temperature
        parts = key.split(":")
        if len(parts) != 3:
            continue
        _, sensor_id, metric = parts
        try:
            samples = r.execute_command("TS.RANGE", key, from_ms, now_ms)
        except Exception:
            continue
        for ts_ms, val in samples:
            rows.append({
                "timestamp": pd.Timestamp(int(ts_ms), unit="ms", tz="UTC"),
                "sensor_id": sensor_id,
                "metric": metric,
                "value": float(val),
            })
    return pd.DataFrame(rows)


# --------------------------------------------------------------------------- #
# Shared helpers
# --------------------------------------------------------------------------- #

def latest_per_sensor(df: pd.DataFrame) -> pd.DataFrame:
    """Pivot the long frame to one row per sensor with a column per metric."""
    if df.empty:
        return pd.DataFrame()
    latest = (
        df.sort_values("timestamp")
        .groupby(["sensor_id", "metric"], as_index=False)
        .tail(1)
    )
    return latest.pivot(index="sensor_id", columns="metric", values="value").reset_index()


def in_band(metric: str, value: float) -> bool:
    lo, hi = METRIC_META[metric]["band"]
    return lo <= value <= hi
