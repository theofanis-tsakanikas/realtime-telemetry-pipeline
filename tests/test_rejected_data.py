"""Unit tests for rejected_data() — the dead-letter branch of the stream.

rejected_data() must be the exact complement of clean_data(): every input row
lands in exactly one of the two outputs, and each rejected row carries the
first rule it violated in `rejection_reason`.
"""
import pytest

from spark_transform import clean_data, rejected_data, schema

VALID = ("sensor_1", 25.0, "60.0", 1013.0, "2024-01-01T12:00:00+00:00")


def make_df(spark, rows):
    # Use the production schema so rows containing None still type correctly.
    return spark.createDataFrame(rows, schema)


@pytest.mark.parametrize(
    ("row", "reason"),
    [
        ((None, 25.0, "60.0", 1013.0, "2024-01-01T12:00:00+00:00"), "missing_sensor_id"),
        (("sensor_1", None, "60.0", 1013.0, "2024-01-01T12:00:00+00:00"), "missing_temperature"),
        (("sensor_1", 25.0, "N/A", 1013.0, "2024-01-01T12:00:00+00:00"), "invalid_humidity"),
        (("sensor_1", 25.0, "60.0", None, "2024-01-01T12:00:00+00:00"), "missing_pressure"),
        (("sensor_1", 25.0, "60.0", 1013.0, None), "missing_timestamp"),
        (("sensor_1", 9.9, "60.0", 1013.0, "2024-01-01T12:00:00+00:00"), "temperature_out_of_range"),
        (("sensor_1", 25.0, "150.0", 1013.0, "2024-01-01T12:00:00+00:00"), "humidity_out_of_range"),
        (("sensor_1", 25.0, "60.0", 2500.0, "2024-01-01T12:00:00+00:00"), "pressure_out_of_range"),
    ],
)
def test_each_violation_gets_its_reason(spark, row, reason):
    out = rejected_data(make_df(spark, [row])).collect()
    assert len(out) == 1
    assert out[0]["rejection_reason"] == reason


def test_valid_row_is_not_rejected(spark):
    assert rejected_data(make_df(spark, [VALID])).count() == 0


def test_clean_and_rejected_partition_the_input(spark):
    rows = [
        VALID,
        ("sensor_2", None, "55.0", 1000.0, "2024-01-01T12:00:01+00:00"),  # rejected
        ("sensor_3", 30.0, "N/A", 1000.0, "2024-01-01T12:00:02+00:00"),   # rejected
        ("sensor_4", 22.0, "45.5", 990.0, "2024-01-01T12:00:03+00:00"),   # valid
        ("sensor_5", 22.0, "45.5", 2500.0, "2024-01-01T12:00:04+00:00"),  # rejected
    ]
    df = make_df(spark, rows)
    valid_count = clean_data(df).count()
    rejected_count = rejected_data(df).count()
    assert valid_count == 2
    assert rejected_count == 3
    assert valid_count + rejected_count == len(rows)
