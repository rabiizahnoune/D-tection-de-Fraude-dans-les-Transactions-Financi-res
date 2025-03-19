import requests
import json
from kafka import KafkaProducer
import time

# Configuration Kafka
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "external_data_topic"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    response = requests.get("http://localhost:5002/generate/externaldata")
    if response.status_code == 200:
        external_data = response.json()
        producer.send(KAFKA_TOPIC, value=external_data)
        print("Sent external data update")
    else:
        print(f"Failed to fetch external data: {response.status_code}")
    time.sleep(60)  # Récupérer toutes les minutes