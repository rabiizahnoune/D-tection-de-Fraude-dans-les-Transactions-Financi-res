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
API = os.getenv("API")

KAFKA_BROKER = f"kafka:{PORT}"
KAFKA_TOPIC = "transactions_topic"
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Limiter le nombre de messages produits
MAX_MESSAGES = 601
message_count = 0

while message_count < MAX_MESSAGES:
    response = requests.get(API)
    if response.status_code == 200:
        transaction = response.json()
        producer.send(KAFKA_TOPIC, value=transaction)
        print(f"Sent transaction: {transaction['transaction_id']}")
        message_count += 1
    else:
        print(f"Failed to fetch transaction: {response.status_code}")
    time.sleep(1)  # Une transaction par seconde

# S'assurer que tous les messages sont envoyés avant de quitter
producer.flush()
print(f"Produit {message_count} messages. Arrêt du producteur.")