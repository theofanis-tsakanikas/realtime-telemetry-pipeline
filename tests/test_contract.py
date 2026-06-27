"""Tests for the declarative Pandera data-quality contract (scripts/contract.py)."""

from __future__ import annotations

import pandas as pd
import pandera.pandas as pa
import pytest
from contract import CONTRACT, ContractReport, validate, validation_report
from metrics_spec import METRIC_NAMES, METRICS


def _valid_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "sensor_id": ["sensor_1", "sensor_2"],
            "temperature": [22.5, 30.0],
            "humidity": [55.0, 48.0],
            "pressure": [1008.0, 1012.0],
        }
    )


def test_contract_columns_match_metrics_spec():
    """The contract must cover sensor_id + every metric — guards against spec drift."""
    cols = set(CONTRACT.columns)
    assert "sensor_id" in cols
    for name in METRIC_NAMES:
        assert name in cols, f"contract is missing metric column {name!r}"


def test_valid_frame_passes():
    out = validate(_valid_frame())
    assert len(out) == 2


def test_out_of_range_temperature_fails():
    bad = _valid_frame()
    bad.loc[0, "temperature"] = 99.0  # above the 45 C max
    with pytest.raises(pa.errors.SchemaErrors):
        validate(bad)


def test_null_sensor_id_fails():
    bad = _valid_frame()
    bad.loc[0, "sensor_id"] = None
    with pytest.raises(pa.errors.SchemaErrors):
        validate(bad)


def test_ranges_track_metrics_spec():
    """Each metric's just-out-of-range value is rejected at exactly the spec bound."""
    for m in METRICS:
        bad = _valid_frame()
        bad.loc[0, m.name] = m.valid_max + 1.0
        report = validation_report(bad)
        assert not report.passed
        assert report.by_column.get(m.name, 0) >= 1


def test_validation_report_passes_cleanly():
    report = validation_report(_valid_frame())
    assert isinstance(report, ContractReport)
    assert report.passed
    assert report.failures == 0
    assert "PASS" in report.summary()


def test_validation_report_counts_violations():
    bad = _valid_frame()
    bad.loc[0, "temperature"] = 99.0
    bad.loc[1, "pressure"] = 5000.0
    report = validation_report(bad)
    assert not report.passed
    assert report.failures >= 2
    assert "FAIL" in report.summary()
