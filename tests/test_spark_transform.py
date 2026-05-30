from spark_transform import clean_data, schema

VALID_TS = "2024-01-01T12:00:00+00:00"


def make_df(spark, rows):
    return spark.createDataFrame(rows, schema)


def test_valid_row_passes_through(spark):
    rows = [("sensor_1", 25.0, "60.0", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 1


def test_null_temperature_is_dropped(spark):
    rows = [("sensor_1", None, "60.0", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_humidity_string_na_is_dropped(spark):
    rows = [("sensor_1", 25.0, "N/A", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_valid_numeric_humidity_string_is_cast(spark):
    rows = [("sensor_1", 25.0, "45.5", 1013.0, VALID_TS)]
    result = clean_data(make_df(spark, rows))
    assert result.count() == 1
    assert result.collect()[0]["humidity"] == 45.5


def test_null_timestamp_is_dropped(spark):
    rows = [("sensor_1", 25.0, "60.0", 1013.0, None)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_temperature_below_minimum_is_filtered(spark):
    rows = [("sensor_1", 9.9, "60.0", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_temperature_above_maximum_is_filtered(spark):
    rows = [("sensor_1", 45.1, "60.0", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_pressure_outlier_filtered(spark):
    # Matches the simulator's deliberate anomaly range (2000–3000 hPa)
    rows = [("sensor_1", 25.0, "60.0", 2500.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_temperature_at_boundary_values(spark):
    # between() in PySpark is inclusive on both ends
    rows = [
        ("sensor_1", 10.0, "60.0", 1013.0, VALID_TS),
        ("sensor_2", 45.0, "60.0", 1013.0, VALID_TS),
    ]
    assert clean_data(make_df(spark, rows)).count() == 2


def test_mixed_batch_correct_count(spark):
    rows = [
        ("sensor_1", 25.0, "60.0", 1013.0, VALID_TS),   # valid
        ("sensor_2", 25.0, "60.0", 1013.0, VALID_TS),   # valid
        ("sensor_3", 25.0, "60.0", 1013.0, VALID_TS),   # valid
        ("sensor_4", None, "60.0", 1013.0, VALID_TS),   # null temperature → dropped
        ("sensor_5", 25.0, "N/A",  1013.0, VALID_TS),   # invalid humidity → dropped
    ]
    assert clean_data(make_df(spark, rows)).count() == 3
