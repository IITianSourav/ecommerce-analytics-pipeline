from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime
import os

# Kafka setup
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_transactions")
SLEEP_INTERVAL = int(os.getenv("PRODUCER_SLEEP", 2))  # seconds

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "product": random.choice(["phone", "laptop", "tablet", "watch"]),
        "price": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

try:
    while True:
        event = generate_order()
        print("Sending event:", event)
        producer.send(TOPIC_NAME, event)
        time.sleep(SLEEP_INTERVAL)
except KeyboardInterrupt:
    print("Producer stopped.")
except Exception as e:
    print(f"Error sending data to Kafka: {e}")
finally:
    producer.close()
