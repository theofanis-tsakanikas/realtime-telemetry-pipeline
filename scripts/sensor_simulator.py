"""IoT sensor simulator: a Confluent Kafka producer.

Generates synthetic readings (temperature, humidity, pressure, timestamp) for a
configurable number of sensors and publishes them to a Kafka topic roughly once
per second per tick. About 20% of messages contain deliberate data-quality
anomalies (nulls, wrong types, extreme outliers, missing keys) so the
downstream Spark cleaning logic has something to filter.

Configuration is read from environment variables (loaded from ``.env``) with
sensible defaults; see the constants below. Handles SIGINT/SIGTERM for a
graceful, buffer-flushing shutdown.
"""
import json
import logging
import os
import random
import signal
import sys
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)

# ============================================================
# 🔧 Configuration Section
# ============================================================
load_dotenv()  # Load environment variables from .env file

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "sensor_data")
SENSOR_COUNT = int(os.getenv("SENSOR_COUNT", 5))
DELAY_SECONDS = float(os.getenv("DELAY_SECONDS", 1))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 10))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", 3))


# ============================================================
# ⚙️ Kafka Setup Functions
# ============================================================
def create_kafka_topic() -> None:
    """
    Creates the Kafka topic if it does not exist using the AdminClient.
    Uses futures to wait for the topic creation result.
    """
    try:
        admin_client = AdminClient({"bootstrap.servers": KAFKA_BROKER})
        topic = NewTopic(KAFKA_TOPIC, num_partitions=1, replication_factor=1)

        # Call create_topics, which returns a dictionary of futures
        fs = admin_client.create_topics([topic])

        for topic_name, f in fs.items():
            try:
                f.result()  # Wait for the operation to finish
                logger.info("Topic '%s' created successfully.", KAFKA_TOPIC)
            except Exception as e:
                # Handle the case where the topic already exists
                if "exists" in str(e).lower():
                    logger.info("Topic '%s' already exists.", KAFKA_TOPIC)
                else:
                    logger.warning("Could not create topic: %s", e)
    except Exception as e:
        logger.warning("Admin client error: %s", e)


def connect_producer_with_retry() -> Producer:
    """
    Initializes the Confluent Kafka Producer and verifies broker connectivity.

    The Producer constructor never contacts the broker (librdkafka connects
    lazily), so a metadata request (``list_topics``) is used as the actual
    connectivity probe — retried while the broker finishes leader election.
    """
    producer = Producer({
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'sensor_simulator_producer'
    })

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            producer.list_topics(timeout=5)
            logger.info("Connected to Kafka broker on attempt %d.", attempt)
            return producer
        except Exception as e:
            logger.warning(
                "Kafka broker not available (attempt %d/%d). Retrying in %ds... Error: %s",
                attempt, MAX_RETRIES, RETRY_DELAY, e,
            )
            time.sleep(RETRY_DELAY)

    raise ConnectionError(f"Failed to connect to Kafka after {MAX_RETRIES} attempts.")


def delivery_report(err, msg) -> None:
    """Per-message delivery callback: log failures instead of dropping them silently."""
    if err is not None:
        logger.error("Delivery failed for key=%s: %s", msg.key(), err)


# ============================================================
# 🌡️ Sensor Data Simulation
# ============================================================
def generate_sensor_data(sensor_id: int) -> dict:
    """
    Generates simulated IoT sensor data.
    Introduces ~20% random data quality anomalies for Spark testing.
    """
    data = {
        "sensor_id": f"sensor_{sensor_id}",
        "temperature": round(random.uniform(15.0, 35.0), 2),
        "humidity": round(random.uniform(30.0, 80.0), 2),
        "pressure": round(random.uniform(990.0, 1025.0), 2),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    # Introduce data quality anomalies
    error_chance = random.random()
    if error_chance < 0.05:
        data["temperature"] = None  # Missing value
    elif error_chance < 0.10:
        data["humidity"] = "N/A"  # Data type inconsistency
    elif error_chance < 0.15:
        data["pressure"] = round(random.uniform(2000.0, 3000.0), 2)  # Extreme outlier
    elif error_chance < 0.20:
        del data["timestamp"]  # Missing key

    return data


# ============================================================
# 🚀 Main Application Logic
# ============================================================
def main() -> None:
    """
    Main entry point for the sensor simulator.
    Ensures topic exists, connects producer, and pushes messages indefinitely.
    Sets up signal handlers for graceful shutdown.
    """
    create_kafka_topic()
    producer = connect_producer_with_retry()

    logger.info("Starting sensor simulator... Producing to topic '%s'", KAFKA_TOPIC)

    def handle_exit(*_) -> None:
        """
        Gracefully handles SIGTERM and SIGINT for clean shutdowns.
        Flushes pending messages in buffer and closes the producer.
        """
        logger.info("Stopping simulator gracefully...")
        producer.flush()  # Push any buffered messages to Kafka before exiting
        sys.exit(0)

    # Register OS signals for graceful shutdown
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    try:
        while True:
            sensor_id = random.randint(1, SENSOR_COUNT)
            data = generate_sensor_data(sensor_id)

            producer.produce(
                KAFKA_TOPIC,
                key=f"sensor_{sensor_id}",
                value=json.dumps(data).encode("utf-8"),
                callback=delivery_report,
            )

            # Serve delivery callbacks for previously produced messages
            producer.poll(0)

            logger.info("Produced: %s", data)
            time.sleep(DELAY_SECONDS)

    except Exception:
        logger.exception("Error in simulator loop")
    finally:
        handle_exit()


if __name__ == "__main__":
    main()
