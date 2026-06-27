"""Avro schema + record normalisation for the sensor stream (Confluent Schema Registry).

When ``SERIALIZATION_FORMAT=avro`` the simulator serialises each reading with the Confluent
Avro serializer (which registers this schema in the Schema Registry and frames every message
with a magic byte + schema id), and Spark decodes it with ``from_avro`` after stripping that
5-byte header. The schema is the on-the-wire contract; :mod:`metrics_spec` remains the
*semantic* contract (valid ranges / drift). The two are complementary.

Design note — why every field is nullable
------------------------------------------
The simulator deliberately injects ~20% malformed readings (null temperature, ``"N/A"``
humidity, a missing timestamp) so the downstream DLQ has something to reject. A strict Avro
schema would reject those at *serialize* time and defeat the demo, so every field is a
``["null", T]`` union with a null default. ``humidity`` is typed as ``string`` (not double)
for the same reason the Spark schema reads it as a string: it must accept both numeric
readings and the dirty ``"N/A"`` sentinel, with the numeric/regex cleaning happening later.
"""

from __future__ import annotations

# Subject follows the Confluent TopicNameStrategy default: "<topic>-value".
SUBJECT_SUFFIX = "-value"

# Canonical Avro schema (JSON). Shared verbatim by the producer (writer) and Spark (reader),
# so decoding succeeds regardless of the registry-assigned schema id.
AVRO_SCHEMA = """
{
  "type": "record",
  "name": "SensorReading",
  "namespace": "iot.telemetry",
  "fields": [
    {"name": "sensor_id",   "type": ["null", "string"], "default": null},
    {"name": "temperature", "type": ["null", "double"], "default": null},
    {"name": "humidity",    "type": ["null", "string"], "default": null},
    {"name": "pressure",    "type": ["null", "double"], "default": null},
    {"name": "timestamp",   "type": ["null", "string"], "default": null}
  ]
}
"""


def to_avro_dict(msg: dict) -> dict:
    """Normalise a generated reading into a record that matches :data:`AVRO_SCHEMA`.

    * fills in every field (a *missing* timestamp anomaly becomes an explicit ``None`` —
      Avro records cannot omit a field, and null timestamp is rejected identically downstream);
    * coerces a numeric ``humidity`` to its string form (the schema types humidity as string,
      mirroring how the JSON path read it), while leaving the ``"N/A"`` sentinel untouched.
    """
    humidity = msg.get("humidity")
    if humidity is not None and not isinstance(humidity, str):
        humidity = str(humidity)
    return {
        "sensor_id": msg.get("sensor_id"),
        "temperature": msg.get("temperature"),
        "humidity": humidity,
        "pressure": msg.get("pressure"),
        "timestamp": msg.get("timestamp"),
    }
