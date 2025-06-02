import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

TOPIC_NAME = os.getenv("KAFKA_TOPIC", "ecommerce_transactions")

admin_client = KafkaAdminClient(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "localhost:9092"),
    client_id='ecom-data-engineer'
)

topic_list = [NewTopic(name=TOPIC_NAME, num_partitions=1, replication_factor=1)]

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print(f"Topic '{TOPIC_NAME}' created successfully.")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC_NAME}' already exists.")
except Exception as e:
    print(f"Error creating topic: {e}")
