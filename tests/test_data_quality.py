"""Tests for the first-class data-quality metrics."""

from unittest.mock import MagicMock

from data_quality import DQMetrics, dq_timeseries, quality_metrics, write_dq_metrics
from spark_transform import rejected_data, schema

VALID = ("sensor_1", 25.0, "60.0", 1013.0, "2024-01-01T12:00:00+00:00")


def _df(spark, rows):
    return spark.createDataFrame(rows, schema)


def test_metrics_partition_total(spark):
    rows = [
        VALID,
        ("sensor_2", None, "55.0", 1000.0, "2024-01-01T12:00:01+00:00"),  # missing_temperature
        ("sensor_3", 30.0, "N/A", 1000.0, "2024-01-01T12:00:02+00:00"),   # invalid_humidity
        ("sensor_4", 22.0, "45.5", 990.0, "2024-01-01T12:00:03+00:00"),   # valid
        ("sensor_5", 22.0, "45.5", 2500.0, "2024-01-01T12:00:04+00:00"),  # pressure_out_of_range
    ]
    m = quality_metrics(_df(spark, rows), rejected_data)
    assert m.total == 5
    assert m.valid == 2
    assert m.rejected_total == 3
    assert m.valid + m.rejected_total == m.total


def test_by_reason_breakdown(spark):
    rows = [
        ("s", 30.0, "N/A", 1000.0, "2024-01-01T12:00:00+00:00"),
        ("s", 30.0, "N/A", 1000.0, "2024-01-01T12:00:01+00:00"),
        ("s", 22.0, "45.5", 2500.0, "2024-01-01T12:00:02+00:00"),
    ]
    m = quality_metrics(_df(spark, rows), rejected_data)
    assert m.by_reason["invalid_humidity"] == 2
    assert m.by_reason["pressure_out_of_range"] == 1


def test_accept_rate(spark):
    rows = [VALID, ("s", None, "55.0", 1000.0, "2024-01-01T12:00:01+00:00")]
    m = quality_metrics(_df(spark, rows), rejected_data)
    assert m.accept_rate == 0.5
    assert m.reject_rate == 0.5


def test_empty_batch_accept_rate_is_one(spark):
    m = quality_metrics(_df(spark, []), rejected_data)
    assert m.total == 0
    assert m.accept_rate == 1.0


# --- timeseries flattening + Redis write ------------------------------------ #


def test_dq_timeseries_keys():
    m = DQMetrics(total=10, valid=8, rejected_total=2, by_reason={"invalid_humidity": 2})
    ts = dq_timeseries(m)
    assert ts["dq:total"] == 10.0
    assert ts["dq:valid"] == 8.0
    assert ts["dq:accept_rate"] == 0.8
    assert ts["dq:rejected:invalid_humidity"] == 2.0


def test_write_dq_metrics_issues_ts_add():
    r = MagicMock()
    m = DQMetrics(total=10, valid=9, rejected_total=1, by_reason={"missing_temperature": 1})
    write_dq_metrics(r, m, 1704110400000)
    cmds = [c.args[0] for c in r.execute_command.call_args_list]
    assert cmds and all(c == "TS.ADD" for c in cmds)
    # the accept-rate series is present
    keys = [c.args[1] for c in r.execute_command.call_args_list]
    assert "dq:accept_rate" in keys
    assert "dq:rejected:missing_temperature" in keys
