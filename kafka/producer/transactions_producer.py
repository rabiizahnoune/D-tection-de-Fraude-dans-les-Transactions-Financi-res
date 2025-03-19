import requests
import json
from kafka import KafkaProducer
import time

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "transactions_topic"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    response = requests.get("http://transactions-api-transactions-1:5000/generate/transaction")
    if response.status_code == 200:
        transaction = response.json()
        producer.send(KAFKA_TOPIC, value=transaction)
        print(f"Sent transaction: {transaction['transaction_id']}")
    else:
        print(f"Failed to fetch transaction: {response.status_code}")
    time.sleep(1)  # Une transaction par seconde