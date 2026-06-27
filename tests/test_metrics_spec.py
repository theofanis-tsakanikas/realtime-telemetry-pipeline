"""Tests for the metrics contract — derived drift baselines and simulator agreement."""

from math import sqrt

import sensor_simulator as sim
from metrics_spec import HUMIDITY, METRICS, PRESSURE, TEMPERATURE, spec


def test_baseline_is_uniform_derived():
    """normal_mean/std are the uniform mean/std of the normal-operation range."""
    for m in METRICS:
        expected_mean = (m.normal_min + m.normal_max) / 2.0
        expected_std = (m.normal_max - m.normal_min) / sqrt(12.0)
        assert m.normal_mean == expected_mean
        assert m.normal_std == expected_std


def test_known_baseline_values():
    assert TEMPERATURE.normal_mean == 25.0
    assert round(TEMPERATURE.normal_std, 2) == 5.77
    assert HUMIDITY.normal_mean == 55.0
    assert PRESSURE.normal_mean == 1007.5


def test_contract_matches_simulator_ranges():
    """The drift baseline must use the exact ranges the simulator draws normal readings from.

    Guards against the producer and the detector silently disagreeing on 'normal'.
    """
    assert (TEMPERATURE.normal_min, TEMPERATURE.normal_max) == sim.TEMPERATURE_RANGE
    assert (HUMIDITY.normal_min, HUMIDITY.normal_max) == sim.HUMIDITY_RANGE
    assert (PRESSURE.normal_min, PRESSURE.normal_max) == sim.PRESSURE_RANGE


def test_normal_range_within_valid_range():
    """Normal operation must sit inside the acceptance band, or the contract is incoherent."""
    for m in METRICS:
        assert m.valid_min <= m.normal_min < m.normal_max <= m.valid_max


def test_spec_lookup():
    assert spec("temperature") is TEMPERATURE
