"""Tests for parse_stream() decoding (the JSON branch; Avro needs the spark-avro JAR)."""

import json

import spark_transform as st
from spark_transform import clean_data, parse_stream

FIELDS = {"sensor_id", "temperature", "humidity", "pressure", "timestamp"}


def test_parse_stream_json_decodes_value(spark, monkeypatch):
    monkeypatch.setattr(st, "SERIALIZATION_FORMAT", "json")
    payload = json.dumps({
        "sensor_id": "sensor_1", "temperature": 25.0, "humidity": "60.0",
        "pressure": 1013.0, "timestamp": "2024-01-01T12:00:00+00:00",
    })
    raw = spark.createDataFrame([(payload,)], "value string")
    parsed = parse_stream(raw)
    assert set(parsed.columns) == FIELDS
    assert clean_data(parsed).count() == 1


def test_parse_stream_rejects_unknown_format(spark, monkeypatch):
    monkeypatch.setattr(st, "SERIALIZATION_FORMAT", "protobuf")
    raw = spark.createDataFrame([("{}",)], "value string")
    try:
        parse_stream(raw)
        raise AssertionError("expected ValueError for unsupported format")
    except ValueError as exc:
        assert "protobuf" in str(exc)
