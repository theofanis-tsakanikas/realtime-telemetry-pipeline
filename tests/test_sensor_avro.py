"""Tests for the Avro record normaliser and schema (scripts/sensor_avro.py)."""

import json

import pytest
from sensor_avro import AVRO_SCHEMA, to_avro_dict

EXPECTED_FIELDS = {"sensor_id", "temperature", "humidity", "pressure", "timestamp"}


def test_schema_is_valid_json_with_expected_fields():
    schema = json.loads(AVRO_SCHEMA)
    assert schema["type"] == "record"
    assert {f["name"] for f in schema["fields"]} == EXPECTED_FIELDS
    # Every field is a nullable union so the deliberate anomalies still serialise.
    for f in schema["fields"]:
        assert f["type"][0] == "null"


def test_normal_reading_normalises_humidity_to_string():
    msg = {"sensor_id": "sensor_1", "temperature": 25.0, "humidity": 60.0,
           "pressure": 1010.0, "timestamp": "2024-01-01T00:00:00+00:00"}
    out = to_avro_dict(msg)
    assert out["humidity"] == "60.0"  # schema types humidity as string
    assert out["temperature"] == 25.0
    assert set(out) == EXPECTED_FIELDS


def test_missing_timestamp_becomes_none():
    msg = {"sensor_id": "sensor_1", "temperature": 25.0, "humidity": 60.0, "pressure": 1010.0}
    out = to_avro_dict(msg)
    assert "timestamp" in out
    assert out["timestamp"] is None


def test_na_humidity_sentinel_preserved():
    out = to_avro_dict({"sensor_id": "s", "temperature": 25.0, "humidity": "N/A",
                        "pressure": 1010.0, "timestamp": "t"})
    assert out["humidity"] == "N/A"


def test_null_temperature_preserved():
    out = to_avro_dict({"sensor_id": "s", "temperature": None, "humidity": 60.0,
                        "pressure": 1010.0, "timestamp": "t"})
    assert out["temperature"] is None


@pytest.mark.parametrize("reading", [
    {"sensor_id": "s", "temperature": 25.0, "humidity": 60.0, "pressure": 1010.0, "timestamp": "t"},
    {"sensor_id": "s", "temperature": None, "humidity": 60.0, "pressure": 1010.0, "timestamp": "t"},
    {"sensor_id": "s", "temperature": 25.0, "humidity": "N/A", "pressure": 1010.0, "timestamp": "t"},
    {"sensor_id": "s", "temperature": 25.0, "humidity": 60.0, "pressure": 2500.0, "timestamp": "t"},
    {"sensor_id": "s", "temperature": 25.0, "humidity": 60.0, "pressure": 1010.0},  # missing ts
])
def test_every_variant_roundtrips_through_avro(reading):
    """Normal + each anomaly must serialise and deserialise against the schema."""
    fastavro = pytest.importorskip("fastavro")
    import io

    parsed = fastavro.parse_schema(json.loads(AVRO_SCHEMA))
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, parsed, to_avro_dict(reading))
    buf.seek(0)
    decoded = fastavro.schemaless_reader(buf, parsed)
    assert set(decoded) == EXPECTED_FIELDS
