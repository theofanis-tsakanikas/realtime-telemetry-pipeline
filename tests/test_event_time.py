"""Tests for event-time stamping of observability samples (batch_event_time_ms)."""

from spark_transform import batch_event_time_ms, clean_data, schema


def _parse(spark, rows):
    return clean_data(spark.createDataFrame(rows, schema))


def test_returns_max_event_time_ms(spark):
    rows = [
        ("sensor_1", 22.0, "55.0", 1005.0, "2024-01-01T00:00:00+00:00"),
        ("sensor_2", 24.0, "50.0", 1010.0, "2024-01-01T00:00:05+00:00"),  # newest
        ("sensor_3", 23.0, "60.0", 1008.0, "2024-01-01T00:00:02+00:00"),
    ]
    clean = _parse(spark, rows)
    got = batch_event_time_ms(clean, fallback_ms=0)
    # Max event time is 2024-01-01T00:00:05Z = 1704067205000 ms.
    assert got == 1704067205000


def test_empty_batch_uses_fallback(spark):
    # All rows are anomalous → cleaned batch is empty → fallback is used.
    rows = [
        ("sensor_1", None, "55.0", 1005.0, "2024-01-01T00:00:00+00:00"),  # null temp
        ("sensor_2", 24.0, "N/A", 1010.0, "2024-01-01T00:00:05+00:00"),   # bad humidity
    ]
    clean = _parse(spark, rows)
    assert clean.count() == 0
    assert batch_event_time_ms(clean, fallback_ms=999) == 999
