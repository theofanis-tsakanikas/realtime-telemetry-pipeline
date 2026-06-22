"""Unit tests for the IoT sensor simulator's data-generation logic.

`generate_sensor_data()` is already a pure, importable function whose only
randomness comes from the global `random` module, so it is tested directly
(no extraction, no Kafka, no Docker). Seeding `random` makes every assertion
deterministic; the Kafka producer/topic plumbing in `main()` stays
integration-deferred.
"""
import random

import pytest
from sensor_simulator import generate_sensor_data

# Sample size for the statistical rate checks. With a fixed seed the outcome is
# deterministic, so the tolerance bands below are stable run-to-run.
N = 5000

EXPECTED_KEYS = {"sensor_id", "temperature", "humidity", "pressure", "timestamp"}


def classify(msg: dict) -> str:
    """Map a generated message to the anomaly band it landed in (or 'normal')."""
    if msg.get("temperature") is None:
        return "null_temperature"
    if msg.get("humidity") == "N/A":
        return "na_humidity"
    if msg.get("pressure") is not None and msg["pressure"] >= 2000:
        return "pressure_outlier"
    if "timestamp" not in msg:
        return "missing_timestamp"
    return "normal"


def first_normal_message(seed: int = 0) -> dict:
    """Return the first non-anomalous message from a seeded stream."""
    random.seed(seed)
    while True:
        msg = generate_sensor_data(1)
        if classify(msg) == "normal":
            return msg


# --- schema -----------------------------------------------------------------

def test_normal_message_schema():
    msg = first_normal_message()
    assert set(msg.keys()) == EXPECTED_KEYS
    assert msg["sensor_id"] == "sensor_1"
    assert isinstance(msg["temperature"], float)
    assert isinstance(msg["humidity"], float)
    assert isinstance(msg["pressure"], float)
    assert isinstance(msg["timestamp"], str)


def test_sensor_id_uses_passed_id():
    random.seed(0)
    msg = generate_sensor_data(42)
    assert msg["sensor_id"] == "sensor_42"


def test_normal_values_are_in_simulated_ranges():
    msg = first_normal_message()
    assert 15.0 <= msg["temperature"] <= 35.0
    assert 30.0 <= msg["humidity"] <= 80.0
    assert 990.0 <= msg["pressure"] <= 1025.0


# --- anomaly rate & per-type distribution -----------------------------------

def test_anomaly_rates_within_tolerance():
    random.seed(1234)
    counts = {
        "normal": 0, "null_temperature": 0, "na_humidity": 0,
        "pressure_outlier": 0, "missing_timestamp": 0,
    }
    for _ in range(N):
        counts[classify(generate_sensor_data(1))] += 1

    # Each anomaly band is a 5% slice of [0, 1); total anomalies ~20%.
    for band in ("null_temperature", "na_humidity", "pressure_outlier", "missing_timestamp"):
        rate = counts[band] / N
        assert 0.03 <= rate <= 0.07, f"{band} rate {rate:.3f} out of tolerance"

    anomaly_total = (N - counts["normal"]) / N
    assert 0.17 <= anomaly_total <= 0.23, f"total anomaly rate {anomaly_total:.3f}"


@pytest.mark.parametrize("band", [
    "null_temperature", "na_humidity", "pressure_outlier", "missing_timestamp",
])
def test_each_anomaly_type_occurs(band):
    random.seed(1234)
    seen = {classify(generate_sensor_data(1)) for _ in range(N)}
    assert band in seen


def test_pressure_outlier_in_expected_range():
    # Locate an outlier message and confirm it lands in the 2000-3000 band.
    random.seed(7)
    for _ in range(N):
        msg = generate_sensor_data(1)
        if classify(msg) == "pressure_outlier":
            assert 2000.0 <= msg["pressure"] <= 3000.0
            return
    pytest.fail("no pressure outlier produced in sample")


# --- determinism ------------------------------------------------------------

def _strip_timestamp(msg: dict) -> dict:
    # timestamp value comes from datetime.now() (not seeded); drop it so the
    # comparison covers only the random-driven fields and anomaly decisions.
    return {k: v for k, v in msg.items() if k != "timestamp"}


def test_same_seed_produces_identical_sequence():
    random.seed(99)
    first = [_strip_timestamp(generate_sensor_data(1)) for _ in range(50)]
    random.seed(99)
    second = [_strip_timestamp(generate_sensor_data(1)) for _ in range(50)]
    assert first == second


def test_different_seeds_diverge():
    random.seed(1)
    a = [_strip_timestamp(generate_sensor_data(1)) for _ in range(50)]
    random.seed(2)
    b = [_strip_timestamp(generate_sensor_data(1)) for _ in range(50)]
    assert a != b
