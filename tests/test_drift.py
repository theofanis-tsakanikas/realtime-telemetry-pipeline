"""Tests for the statistical drift detector."""

from unittest.mock import MagicMock

from drift import (
    DEFAULT_K,
    STATUS_DRIFT,
    STATUS_STABLE,
    batch_drift,
    drift_score,
    drift_timeseries,
    write_drift_metrics,
)
from metrics_spec import HUMIDITY, PRESSURE, TEMPERATURE


def test_readings_near_baseline_are_stable():
    r = drift_score(TEMPERATURE, n=100, batch_mean=25.3)
    assert r.status == STATUS_STABLE
    assert abs(r.z_score) <= DEFAULT_K


def test_miscalibrated_sensor_flagged_even_when_in_range():
    # mean shifted +5 °C — every reading still inside the valid band [10, 45], but drifted.
    r = drift_score(TEMPERATURE, n=100, batch_mean=30.0)
    assert r.status == STATUS_DRIFT
    assert r.z_score > DEFAULT_K
    assert r.alert


def test_small_shift_with_few_samples_not_flagged():
    # a 2 °C shift with only 4 samples → small z → stable (not enough evidence)
    r = drift_score(TEMPERATURE, n=4, batch_mean=27.0)
    assert r.status == STATUS_STABLE


def test_empty_batch_is_stable():
    assert drift_score(HUMIDITY, n=0, batch_mean=0.0).status == STATUS_STABLE


def test_negative_shift_flagged():
    r = drift_score(PRESSURE, n=200, batch_mean=995.0)  # well below normal 1007.5
    assert r.status == STATUS_DRIFT
    assert r.z_score < 0


def test_batch_drift_skips_missing_metrics():
    results = batch_drift({"temperature": (100, 30.0)})
    assert {r.metric for r in results} == {"temperature"}
    assert results[0].alert


def test_batch_drift_all_metrics():
    summaries = {"temperature": (100, 25.0), "humidity": (100, 55.0), "pressure": (100, 1007.5)}
    results = batch_drift(summaries)
    assert {r.metric for r in results} == {"temperature", "humidity", "pressure"}
    assert all(r.status == STATUS_STABLE for r in results)


def test_drift_timeseries_keys():
    results = batch_drift({"temperature": (100, 30.0)})
    ts = drift_timeseries(results)
    assert "drift:temperature:z" in ts
    assert "drift:temperature:mean" in ts


def test_write_drift_metrics_issues_ts_add():
    r = MagicMock()
    write_drift_metrics(r, batch_drift({"temperature": (100, 30.0)}), 1704110400000)
    cmds = [c.args[0] for c in r.execute_command.call_args_list]
    assert cmds and all(c == "TS.ADD" for c in cmds)
