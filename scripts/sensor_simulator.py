import os
import json
import time
import random
import signal
import sys
from dotenv import load_dotenv
from datetime import datetime, timezone
from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Initialize Faker for any potential fake data generation
fake = Faker()

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
def create_kafka_topic():
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
                print(f"✅ Topic '{KAFKA_TOPIC}' created successfully.")
            except Exception as e:
                # Handle the case where the topic already exists
                if "exists" in str(e).lower():
                    print(f"ℹ️ Topic '{KAFKA_TOPIC}' already exists.")
                else:
                    print(f"⚠️ Could not create topic: {e}")
    except Exception as e:
        print(f"⚠️ Admin client error: {e}")


def connect_producer_with_retry():
    """
    Attempts to initialize the Confluent Kafka Producer.
    Retries connection multiple times in case the broker is temporarily unavailable.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Confluent Kafka uses a dictionary for configuration
            producer = Producer({
                'bootstrap.servers': KAFKA_BROKER,
                'client.id': 'sensor_simulator_producer'
            })
            print(f"✅ Connected to Kafka broker on attempt {attempt}.")
            return producer
        except Exception as e:
            print(f"⚠️ Kafka broker not available (attempt {attempt}/{MAX_RETRIES}). Retrying in {RETRY_DELAY}s... Error: {e}")
            time.sleep(RETRY_DELAY)
            
    raise ConnectionError(f"❌ Failed to connect to Kafka after {MAX_RETRIES} attempts.")


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
def main():
    """
    Main entry point for the sensor simulator.
    Ensures topic exists, connects producer, and pushes messages indefinitely.
    Sets up signal handlers for graceful shutdown.
    """
    create_kafka_topic()
    producer = connect_producer_with_retry()

    print(f"🚀 Starting sensor simulator... Producing to topic '{KAFKA_TOPIC}'")

    def handle_exit(*_):
        """
        Gracefully handles SIGTERM and SIGINT for clean shutdowns.
        Flushes pending messages in buffer and closes the producer.
        """
        print("\n🛑 Stopping simulator gracefully...")
        producer.flush()  # Push any buffered messages to Kafka before exiting
        sys.exit(0)

    # Register OS signals for graceful shutdown
    signal.signal(signal.SIGTERM, handle_exit)
    signal.signal(signal.SIGINT, handle_exit)

    try:
        while True:
            sensor_id = random.randint(1, SENSOR_COUNT)
            data = generate_sensor_data(sensor_id)
            
            # Confluent Kafka uses 'produce' method instead of 'send'
            producer.produce(
                KAFKA_TOPIC, 
                key=f"sensor_{sensor_id}", 
                value=json.dumps(data).encode("utf-8")
            )
            
            # Immediately trigger the delivery of the message
            producer.poll(0) 
            
            print(f"[{datetime.now(timezone.utc).isoformat()}] Produced: {data}")
            time.sleep(DELAY_SECONDS)
            
    except Exception as e:
        print(f"❌ Error in simulator loop: {e}")
    finally:
        handle_exit()


if __name__ == "__main__":
    main()