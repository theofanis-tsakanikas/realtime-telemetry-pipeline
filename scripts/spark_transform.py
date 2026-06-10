"""Spark Structured Streaming job for the IoT sensor pipeline.

Reads raw sensor JSON from a Kafka topic, enforces a static schema, and splits
the stream in two:

* Valid rows (cleaned and range-filtered via :func:`clean_data`) are written to
  Redis TimeSeries through ``foreachBatch``. Each partition opens one pipelined
  Redis connection and issues native ``TS.ADD`` commands (auto-creating the
  series with retention + labels on first write).
* Rejected rows (the complement, via :func:`rejected_data`) are routed to a
  dead-letter Kafka topic with a ``rejection_reason`` column, so data-quality
  failures are observable instead of silently dropped.

Configuration is read from environment variables (with Docker-friendly
defaults) at import time; see the constants below.
"""
import logging
import os
import sys
from datetime import datetime

import redis
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, lit, struct, to_json, to_timestamp, when
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================
# 🔧 Configuration Section
# ============================================================
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")
DLQ_TOPIC = os.getenv("DLQ_TOPIC", "sensor_data_rejected")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/sensor_data")
DLQ_CHECKPOINT_DIR = os.getenv("DLQ_CHECKPOINT_DIR", CHECKPOINT_DIR + "_dlq")

# 7-day retention for every TimeSeries key (milliseconds)
RETENTION_MS = 604800000

# Flush the Redis pipeline every N commands to bound memory per partition.
PIPELINE_FLUSH_EVERY = 500

# Schema for incoming Kafka JSON messages
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", StringType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


def redis_key(sensor_id: str, metric_name: str) -> str:
    """Build the Redis TimeSeries key for a sensor/metric pair."""
    return f"sensor:{sensor_id}:{metric_name}"


def timestamp_to_ms(ts_obj) -> int:
    """Convert a Spark/``datetime`` timestamp to integer milliseconds."""
    return int(ts_obj.timestamp() * 1000)


def row_to_metrics(row) -> dict:
    """Extract the numeric metrics from a row as floats, preserving order."""
    return {
        "temperature": float(row["temperature"]),
        "humidity": float(row["humidity"]),
        "pressure": float(row["pressure"]),
    }


def write_row(r, row) -> None:
    """Write a single cleaned row to Redis TimeSeries.

    Pure command-building logic (key derivation, timestamp conversion, and a
    single ``TS.ADD`` per metric) factored out of the ``foreachPartition``
    wiring so it can be unit-tested against a mocked Redis client. ``r`` is any
    object exposing ``execute_command`` — a connection or a pipeline.

    ``TS.ADD`` auto-creates the series on first write, applying the retention
    and labels; ``ON_DUPLICATE LAST`` makes replayed micro-batches idempotent
    instead of erroring on duplicate timestamps.
    """
    sensor_id = row["sensor_id"]

    # Convert Spark Timestamp object to Milliseconds for RedisTimeSeries
    timestamp_ms = timestamp_to_ms(row["timestamp"])

    for metric_name, value in row_to_metrics(row).items():
        r.execute_command(
            "TS.ADD", redis_key(sensor_id, metric_name), timestamp_ms, value,
            "RETENTION", RETENTION_MS,
            "ON_DUPLICATE", "LAST",
            "LABELS", "sensor_id", sensor_id, "metric", metric_name,
        )


def _clean_columns(df: DataFrame) -> DataFrame:
    """Apply the shared type coercions used by both the valid and DLQ branches."""
    return (
        df.withColumn(
            "humidity",
            when(col("humidity").rlike("^[0-9.]+$"), col("humidity").cast("double")).otherwise(None),
        )
        .withColumn("timestamp", to_timestamp("timestamp"))
    )


def clean_data(df: DataFrame) -> DataFrame:
    """Clean and filter incoming sensor data.

    Applies the data-quality rules for the pipeline:

    1. Casts ``humidity`` to a double only when it matches ``^[0-9.]+$``;
       otherwise it becomes null (handles dirty values such as ``"N/A"``).
    2. Parses the ISO-8601 ``timestamp`` string into a Spark timestamp.
    3. Drops rows missing any critical column (sensor_id, temperature,
       humidity, pressure, timestamp).
    4. Filters readings to plausible ranges: temperature 10-45 C,
       humidity 0-100 %, pressure 950-1050 hPa.

    Args:
        df: Parsed input DataFrame with the columns defined by ``schema``.

    Returns:
        A new DataFrame containing only the valid, in-range rows.
    """
    df = _clean_columns(df)

    # Drop nulls for critical columns
    df = df.na.drop(subset=["sensor_id", "temperature", "humidity", "pressure", "timestamp"])

    # Range validation
    df = df.filter(
        (col("temperature").between(10, 45)) &
        (col("humidity").between(0, 100)) &
        (col("pressure").between(950, 1050))
    )

    return df


def rejected_data(df: DataFrame) -> DataFrame:
    """Return the complement of :func:`clean_data` with a ``rejection_reason``.

    Every row that the cleaning rules would drop is kept here and tagged with
    the first rule it violated, so the dead-letter topic carries enough context
    to debug upstream sensors.

    Args:
        df: Parsed input DataFrame with the columns defined by ``schema``.

    Returns:
        The rejected rows with an extra ``rejection_reason`` string column.
    """
    df = _clean_columns(df)

    reason = (
        when(col("sensor_id").isNull(), lit("missing_sensor_id"))
        .when(col("temperature").isNull(), lit("missing_temperature"))
        .when(col("humidity").isNull(), lit("invalid_humidity"))
        .when(col("pressure").isNull(), lit("missing_pressure"))
        .when(col("timestamp").isNull(), lit("missing_timestamp"))
        .when(~col("temperature").between(10, 45), lit("temperature_out_of_range"))
        .when(~col("humidity").between(0, 100), lit("humidity_out_of_range"))
        .when(~col("pressure").between(950, 1050), lit("pressure_out_of_range"))
    )

    return df.withColumn("rejection_reason", reason).filter(col("rejection_reason").isNotNull())


def write_to_redis(batch_df, batch_id: int) -> None:
    """Write each micro-batch to Redis TimeSeries.

    Used as the ``foreachBatch`` callback for the streaming query. Each
    partition opens its own Redis connection and batches the ``TS.ADD``
    commands through a pipeline, flushing every ``PIPELINE_FLUSH_EVERY``
    commands to bound memory.

    Args:
        batch_df: The micro-batch DataFrame for this trigger.
        batch_id: The monotonically increasing micro-batch identifier.
    """

    def send_partition(partition) -> None:
        """Write all rows in a single Spark partition to Redis.

        One connection + pipeline per partition (Spark best practice). Per-row
        errors are logged so one bad row cannot fail the batch.
        """
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)
        pipe = r.pipeline(transaction=False)
        pending = 0

        for row in partition:
            try:
                write_row(pipe, row)
                pending += 3  # one TS.ADD per metric
                if pending >= PIPELINE_FLUSH_EVERY:
                    pipe.execute()
                    pending = 0
            except Exception:
                logger.exception("Error queueing row for Redis")

        if pending:
            try:
                pipe.execute()
            except Exception:
                logger.exception("Error flushing Redis pipeline")

    batch_df.foreachPartition(send_partition)
    logger.info("Batch %s processed at %s", batch_id, datetime.now().isoformat())


def main() -> None:
    """Main Spark Streaming entry point.

    Builds the local SparkSession (with the Kafka connector package), reads the
    sensor topic as a stream, parses the JSON payloads, and starts two sinks:
    valid rows to Redis TimeSeries, rejected rows to the dead-letter Kafka
    topic. Blocks until either query terminates.
    """
    spark = (
        SparkSession.builder
        .appName("SensorDataTransformer")
        .master("local[*]")
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark Streaming job initialized. Waiting for Kafka data...")

    # 1. Read from Kafka
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2. Parse JSON
    parsed_df = (
        raw_df.selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
    )

    # 3a. Valid branch → RedisTimeSeries
    redis_query = (
        clean_data(parsed_df).writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("update")
        .start()
    )

    # 3b. Rejected branch → dead-letter Kafka topic (with rejection_reason)
    dlq_query = (
        rejected_data(parsed_df)
        .select(to_json(struct("*")).alias("value"))
        .writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("topic", DLQ_TOPIC)
        .option("checkpointLocation", DLQ_CHECKPOINT_DIR)
        .start()
    )

    logger.info(
        "Streaming queries started: valid → Redis, rejected → Kafka topic '%s'", DLQ_TOPIC
    )
    spark.streams.awaitAnyTermination()
    # Surface which query died so the container log explains the exit.
    for q in (redis_query, dlq_query):
        if q.exception() is not None:
            raise q.exception()


if __name__ == "__main__":
    main()
