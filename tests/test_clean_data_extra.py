"""Additional `clean_data()` coverage: range edges plus the schema-enforcement
path (`from_json`) that the existing suite does not exercise.

The existing `test_spark_transform.py` builds DataFrames directly against the
typed `schema`, so it never tests how malformed JSON is coerced. These tests
parse raw JSON strings through `from_json(schema)` exactly as `main()` does,
proving that bad types and missing keys become null and are then dropped.
"""
import json

from pyspark.sql.functions import col, from_json
from spark_transform import clean_data, schema

VALID_TS = "2024-01-01T12:00:00+00:00"


def make_df(spark, rows):
    """Typed-row path: mirrors the existing suite's helper."""
    return spark.createDataFrame(rows, schema)


def parse_json(spark, payloads):
    """JSON-string path: mirrors main()'s parsed_df pipeline via from_json."""
    df = spark.createDataFrame([(p,) for p in payloads], ["json_str"])
    return (
        df.select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )


# --- clean_data() range gaps (typed-row path) -------------------------------

def test_humidity_above_range_is_filtered(spark):
    # "150.0" is numeric -> cast to 150.0 -> fails between(0, 100) -> dropped
    rows = [("sensor_1", 25.0, "150.0", 1013.0, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_humidity_boundary_values_pass(spark):
    # between() is inclusive: 0 and 100 both survive
    rows = [
        ("sensor_1", 25.0, "0",   1013.0, VALID_TS),
        ("sensor_2", 25.0, "100", 1013.0, VALID_TS),
    ]
    assert clean_data(make_df(spark, rows)).count() == 2


def test_pressure_boundary_values_pass(spark):
    # between() is inclusive: 950 and 1050 both survive
    rows = [
        ("sensor_1", 25.0, "60.0", 950.0,  VALID_TS),
        ("sensor_2", 25.0, "60.0", 1050.0, VALID_TS),
    ]
    assert clean_data(make_df(spark, rows)).count() == 2


def test_pressure_just_below_range_is_filtered(spark):
    rows = [("sensor_1", 25.0, "60.0", 949.9, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


def test_pressure_just_above_range_is_filtered(spark):
    rows = [("sensor_1", 25.0, "60.0", 1050.1, VALID_TS)]
    assert clean_data(make_df(spark, rows)).count() == 0


# --- schema / type enforcement (JSON-string path) ---------------------------

def test_valid_json_passes_through(spark):
    payload = json.dumps({
        "sensor_id": "sensor_1", "temperature": 25.0,
        "humidity": "60.0", "pressure": 1013.0, "timestamp": VALID_TS,
    })
    assert clean_data(parse_json(spark, [payload])).count() == 1


def test_bad_type_temperature_becomes_null_and_dropped(spark):
    # Non-numeric string for a DoubleType field -> from_json yields null -> dropped
    payload = json.dumps({
        "sensor_id": "sensor_1", "temperature": "not_a_number",
        "humidity": "60.0", "pressure": 1013.0, "timestamp": VALID_TS,
    })
    assert clean_data(parse_json(spark, [payload])).count() == 0


def test_missing_timestamp_key_is_dropped(spark):
    # The timestamp KEY is entirely absent (distinct from timestamp=None):
    # from_json fills it with null -> na.drop removes the row.
    payload = json.dumps({
        "sensor_id": "sensor_1", "temperature": 25.0,
        "humidity": "60.0", "pressure": 1013.0,
    })
    assert clean_data(parse_json(spark, [payload])).count() == 0


def test_missing_timestamp_key_distinct_from_present_valid(spark):
    # Sanity: identical payload WITH the key survives, isolating the key absence
    # as the sole cause of the drop above.
    with_key = json.dumps({
        "sensor_id": "sensor_1", "temperature": 25.0,
        "humidity": "60.0", "pressure": 1013.0, "timestamp": VALID_TS,
    })
    without_key = json.dumps({
        "sensor_id": "sensor_1", "temperature": 25.0,
        "humidity": "60.0", "pressure": 1013.0,
    })
    result = clean_data(parse_json(spark, [with_key, without_key]))
    assert result.count() == 1
