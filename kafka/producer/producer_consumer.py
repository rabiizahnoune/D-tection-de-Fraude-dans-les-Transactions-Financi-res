import requests
import json
from kafka import KafkaProducer
import time

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "customers_topic"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    response = requests.get("http://api-customers:5001/generate/customer")
    if response.status_code == 200:
        customer = response.json()
        producer.send(KAFKA_TOPIC, value=customer)
        print(f"Sent customer: {customer['customer_id']}")
    else:
        print(f"Failed to fetch customer: {response.status_code}")
    time.sleep(5)