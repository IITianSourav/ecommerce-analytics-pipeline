from kafka import KafkaProducer
import json
import time
import random
from datetime import datetime

# Kafka setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Generate fake order event
def generate_order():
    return {
        "order_id": random.randint(1000, 9999),
        "user_id": random.randint(1, 100),
        "product": random.choice(["phone", "laptop", "tablet", "watch"]),
        "price": round(random.uniform(100, 1000), 2),
        "timestamp": datetime.utcnow().isoformat()
    }

# Send event every 2 seconds
while True:
    event = generate_order()
    print("Sending event:", event)
    producer.send('ecommerce_orders', event)
    time.sleep(2)
