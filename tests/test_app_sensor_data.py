"""Unit tests for the Streamlit app's data layer (app/sensor_data.py).

These cover the pure, framework-free helpers (no Streamlit, no Docker): demo
synthesis determinism + clamping, the reject estimator, banding, and the
long→wide pivot. The Redis-backed reader is integration-deferred.

The final test is a *contract-drift guard*: the app is a standalone deployable
that intentionally re-declares the metric ranges (it cannot import scripts/ on
Streamlit Cloud), so this asserts those ranges stay identical to the pipeline's
single source of truth, scripts/metrics_spec.py.
"""
import pandas as pd
import sensor_data as sd
from metrics_spec import METRICS as SPEC_METRICS

# --- estimate_rejects -------------------------------------------------------

def test_estimate_rejects_matches_twenty_percent_ratio():
    # ~20% of raw are anomalies, so rejected ≈ clean * (0.20 / 0.80).
    assert sd.estimate_rejects(80) == 20
    assert sd.estimate_rejects(0) == 0
    assert sd.estimate_rejects(800) == 200


# --- _smooth_value ----------------------------------------------------------

def test_smooth_value_is_deterministic():
    a = sd._smooth_value(0, "temperature", 1_700_000_000.0)
    b = sd._smooth_value(0, "temperature", 1_700_000_000.0)
    assert a == b


def test_smooth_value_clamped_to_clean_range():
    for metric in sd.METRICS:
        lo, hi = sd.METRIC_META[metric]["clean"]
        for step in range(0, 4000, 37):
            v = sd._smooth_value(2, metric, 1_700_000_000.0 + step)
            assert lo <= v <= hi


# --- reading_at / backfill --------------------------------------------------

def test_reading_at_has_one_row_per_sensor_metric():
    rows = sd.reading_at(1_700_000_000.0)
    assert len(rows) == len(sd.SENSORS) * len(sd.METRICS)
    assert {r["sensor_id"] for r in rows} == set(sd.SENSORS)
    assert {r["metric"] for r in rows} == set(sd.METRICS)


def test_backfill_shape_columns_and_window():
    now = 1_700_000_000.0
    df = sd.backfill(minutes=2, step_seconds=30, now=now)
    assert list(df.columns) == ["timestamp", "sensor_id", "metric", "value"]
    # t runs start..now inclusive in 30s steps over 120s -> 5 ticks.
    ticks = 5
    assert len(df) == ticks * len(sd.SENSORS) * len(sd.METRICS)
    assert not df.empty


def test_backfill_is_deterministic_for_fixed_now():
    now = 1_700_000_000.0
    a = sd.backfill(minutes=1, step_seconds=20, now=now)
    b = sd.backfill(minutes=1, step_seconds=20, now=now)
    pd.testing.assert_frame_equal(a, b)


def test_backfill_values_within_clean_ranges():
    df = sd.backfill(minutes=3, step_seconds=15, now=1_700_000_000.0)
    for metric in sd.METRICS:
        lo, hi = sd.METRIC_META[metric]["clean"]
        vals = df[df["metric"] == metric]["value"]
        assert vals.between(lo, hi).all()


# --- in_band ----------------------------------------------------------------

def test_in_band():
    lo, hi = sd.METRIC_META["temperature"]["band"]
    assert sd.in_band("temperature", (lo + hi) / 2)
    assert not sd.in_band("temperature", hi + 10)
    assert sd.in_band("temperature", lo)   # inclusive
    assert sd.in_band("temperature", hi)   # inclusive


# --- latest_per_sensor ------------------------------------------------------

def test_latest_per_sensor_pivots_to_one_row_each():
    ts = pd.Timestamp("2024-01-01T00:00:00Z")
    df = pd.DataFrame(
        [
            (ts, "sensor_1", "temperature", 20.0),
            (ts + pd.Timedelta(seconds=1), "sensor_1", "temperature", 21.0),  # newer
            (ts, "sensor_1", "humidity", 50.0),
            (ts, "sensor_2", "temperature", 30.0),
        ],
        columns=["timestamp", "sensor_id", "metric", "value"],
    )
    out = sd.latest_per_sensor(df)
    assert len(out) == 2  # one row per sensor
    s1 = out[out["sensor_id"] == "sensor_1"].iloc[0]
    assert s1["temperature"] == 21.0  # most recent kept


def test_latest_per_sensor_empty_frame():
    assert sd.latest_per_sensor(pd.DataFrame()).empty


# --- contract-drift guard (app vs metrics_spec) -----------------------------

def test_app_clean_ranges_match_metrics_spec():
    """The app's hardcoded clean ranges must equal the pipeline's contract."""
    spec_by_name = {m.name: m for m in SPEC_METRICS}
    # The app and the contract must cover exactly the same metrics.
    assert set(sd.METRICS) == set(spec_by_name)
    for name in sd.METRICS:
        spec = spec_by_name[name]
        assert sd.METRIC_META[name]["clean"] == (spec.valid_min, spec.valid_max), (
            f"app clean range for {name} drifted from metrics_spec"
        )
