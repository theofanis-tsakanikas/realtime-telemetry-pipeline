"""Unit tests for the Redis TimeSeries sink's pure command-building logic.

`write_row()` (and its helpers) were factored out of the `foreachPartition`
closure so they can be exercised against a mocked Redis client — no real Redis,
Spark, or Docker. The thin `foreachBatch`/`foreachPartition` wiring (connection
setup, per-row error logging) remains integration-deferred.
"""
from datetime import UTC, datetime
from unittest.mock import MagicMock, call

import pytest
import redis
from spark_transform import (
    RETENTION_MS,
    redis_key,
    row_to_metrics,
    timestamp_to_ms,
    write_row,
)

# 2024-01-01T12:00:00Z -> 1704110400 s -> 1704110400000 ms
TS = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
TS_MS = 1704110400000

ROW = {
    "sensor_id": "sensor_1",
    "temperature": 25.0,
    "humidity": 60.0,
    "pressure": 1013.0,
    "timestamp": TS,
}


# --- pure helpers -----------------------------------------------------------

def test_redis_key_format():
    assert redis_key("sensor_3", "humidity") == "sensor:sensor_3:humidity"


def test_timestamp_to_ms():
    assert timestamp_to_ms(TS) == TS_MS


def test_row_to_metrics_values_and_order():
    metrics = row_to_metrics(ROW)
    assert list(metrics.keys()) == ["temperature", "humidity", "pressure"]
    assert metrics == {"temperature": 25.0, "humidity": 60.0, "pressure": 1013.0}
    assert all(isinstance(v, float) for v in metrics.values())


def test_row_to_metrics_casts_numeric_strings():
    row = dict(ROW, humidity="45.5")
    assert row_to_metrics(row)["humidity"] == 45.5


# --- write_row against a mocked client --------------------------------------

def test_write_row_adds_each_metric_with_autocreate_options():
    r = MagicMock()
    write_row(r, ROW)

    expected = []
    for metric, value in (("temperature", 25.0), ("humidity", 60.0), ("pressure", 1013.0)):
        key = f"sensor:sensor_1:{metric}"
        expected.append(call(
            "TS.ADD", key, TS_MS, value,
            "RETENTION", RETENTION_MS,
            "ON_DUPLICATE", "LAST",
            "LABELS", "sensor_id", "sensor_1", "metric", metric,
        ))

    assert r.execute_command.call_args_list == expected
    assert r.execute_command.call_count == 3


def test_write_row_propagates_redis_errors():
    r = MagicMock()
    r.execute_command.side_effect = redis.ResponseError("WRONGTYPE something else")

    with pytest.raises(redis.ResponseError, match="WRONGTYPE"):
        write_row(r, ROW)


def test_write_row_uses_seven_day_retention():
    assert RETENTION_MS == 604800000  # 7 days in milliseconds
