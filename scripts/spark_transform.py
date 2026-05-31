"""Spark Structured Streaming job for the IoT sensor pipeline.

Reads raw sensor JSON from a Kafka topic, enforces a static schema, cleans and
range-filters the readings via :func:`clean_data`, and writes the surviving
metrics to Redis TimeSeries. The Redis sink runs through ``foreachBatch`` and
opens one connection per Spark partition (a Spark best practice) before issuing
native ``TS.CREATE`` / ``TS.ADD`` commands.

Configuration is read from environment variables (with Docker-friendly
defaults) at import time; see the constants below.
"""
import os
import redis
from datetime import datetime
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, from_json, when, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ============================================================
# 🔧 Configuration Section
# ============================================================
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/sensor_data")

# Schema for incoming Kafka JSON messages
schema = StructType([
    StructField("sensor_id", StringType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", StringType(), True),
    StructField("pressure", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])


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
    # Convert humidity to double if it's a valid number string
    df = df.withColumn(
        "humidity",
        when(col("humidity").rlike("^[0-9.]+$"), col("humidity").cast("double")).otherwise(None)
    )

    df = df.withColumn("timestamp", to_timestamp("timestamp"))

    # Drop nulls for critical columns
    df = df.na.drop(subset=["sensor_id", "temperature", "humidity", "pressure", "timestamp"])

    # Range validation
    df = df.filter(
        (col("temperature").between(10, 45)) &
        (col("humidity").between(0, 100)) &
        (col("pressure").between(950, 1050))
    )

    return df


def write_to_redis(batch_df, batch_id: int) -> None:
    """Write each micro-batch to Redis TimeSeries.

    Used as the ``foreachBatch`` callback for the streaming query. Each
    partition opens its own Redis connection, lazily creates a labelled
    TimeSeries per ``sensor:{id}:{metric}`` key, and appends the reading.

    Args:
        batch_df: The micro-batch DataFrame for this trigger (type hint omitted
            to avoid adding a Spark ``DataFrame`` import).
        batch_id: The monotonically increasing micro-batch identifier.
    """

    def send_partition(partition) -> None:
        """Write all rows in a single Spark partition to Redis.

        Opens one Redis connection for the partition and, for every row,
        ensures the TimeSeries keys exist before adding the sample. Per-row
        errors are caught and logged so one bad row cannot fail the batch.
        """
        # Establish connection per partition (best practice in Spark)
        r = redis.StrictRedis(host=REDIS_HOST, port=REDIS_PORT, db=0)

        for row in partition:
            try:
                sensor_id = row["sensor_id"]
                
                # Convert Spark Timestamp object to Milliseconds for RedisTimeSeries
                ts_obj = row["timestamp"]
                timestamp_ms = int(ts_obj.timestamp() * 1000)

                metrics = {
                    "temperature": float(row["temperature"]),
                    "humidity": float(row["humidity"]),
                    "pressure": float(row["pressure"])
                }

                for metric_name, value in metrics.items():
                    key = f"sensor:{sensor_id}:{metric_name}"

                    # Attempt to create TimeSeries (ignore if already exists)
                    try:
                        r.execute_command(
                            "TS.CREATE", key,
                            "RETENTION", 604800000,  # 7 days retention
                            "LABELS", "sensor_id", sensor_id, "metric", metric_name
                        )
                    except redis.ResponseError as e:
                        if "already exists" not in str(e):
                            raise e

                    # Add sample to RedisTimeSeries
                    r.execute_command("TS.ADD", key, timestamp_ms, value)

            except Exception as e:
                print(f"❌ Error processing row in Redis: {e}")

    # Process Rows directly without JSON overhead!
    batch_df.foreachPartition(send_partition)
    print(f"✅ Batch {batch_id} processed at {datetime.now().isoformat()}")


def main() -> None:
    """Main Spark Streaming entry point.

    Builds the local SparkSession (with the Kafka connector package), reads the
    sensor topic as a stream, parses and cleans the JSON payloads, and starts
    the Redis sink query, blocking on ``awaitTermination``.
    """
    spark = (
        SparkSession.builder
        .appName("SensorDataTransformer")
        .master("local[*]") 
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    print("🚀 Spark Streaming Job Initialized. Waiting for Kafka data...")

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

    # 3. Transform & Clean
    cleaned_df = clean_data(parsed_df)

    # 4. Sink to RedisTimeSeries
    query = (
        cleaned_df.writeStream
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", CHECKPOINT_DIR)
        .outputMode("update")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    main()