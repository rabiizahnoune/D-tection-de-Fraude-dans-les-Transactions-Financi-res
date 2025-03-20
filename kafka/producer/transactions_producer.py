import requests
import json
from kafka import KafkaProducer
import time
from dotenv import load_dotenv
import os

# Charger le fichier .env
load_dotenv()

# Accéder aux variables
PORT = os.getenv("PORT")
API = os.getenv("API", "http://api-transactions:5000/generate/transaction")

KAFKA_BROKER = f"kafka:{PORT}"
KAFKA_TOPIC = "transactions_topic"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

while True:
    response = requests.get(API)
    if response.status_code == 200:
        transaction = response.json()
        producer.send(KAFKA_TOPIC, value=transaction)
        print(f"Sent transaction: {transaction['transaction_id']}")
    else:
        print(f"Failed to fetch transaction: {response.status_code}")
    time.sleep(1)  # Une transaction par seconde