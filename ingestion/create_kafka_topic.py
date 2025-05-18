from kafka.admin import KafkaAdminClient, NewTopic

admin_client = KafkaAdminClient(
    bootstrap_servers="localhost:9092",
    client_id='ecom-data-engineer'
)

topic_list = []
topic_list.append(NewTopic(name="ecommerce_transactions", num_partitions=1, replication_factor=1))

try:
    admin_client.create_topics(new_topics=topic_list, validate_only=False)
    print("Topic 'ecommerce_transactions' created successfully.")
except Exception as e:
    print(f"Error creating topic: {e}")
