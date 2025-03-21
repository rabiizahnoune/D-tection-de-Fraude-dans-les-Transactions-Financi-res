from kafka import KafkaConsumer
import pandas as pd
import json
from datetime import datetime
import os
from dotenv import load_dotenv


# Charger le fichier .env
load_dotenv()

# Accéder aux variables
PORT = os.getenv("PORT")
consumer = KafkaConsumer(
    'transactions_topic',
    bootstrap_servers=[F'kafka:{PORT}'],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='transaction-consumer-group'
)

transactions = []
batch_size = 10
batch_count = 0

print("Starting consumer...")
for i, message in enumerate(consumer):
    # Décoder le message Kafka (chaîne JSON)
    transaction_json = message.value.decode('utf-8')
    print(f"Received transaction({i}): {transaction_json}")
    
    # Parser le JSON
    transaction_data = json.loads(transaction_json)

    # Extraire les champs du JSON
    transactions.append({
        'transaction_id': transaction_data['transaction_id'],
        'date_time': transaction_data['date_time'],
        'amount': transaction_data['amount'],
        'currency': transaction_data['currency'],
        'merchant_details': transaction_data['merchant_details'],
        'customer_id': transaction_data['customer_id'],
        'transaction_type': transaction_data['transaction_type'],
        'location': transaction_data['location']
    })

    # Écrire un batch toutes les 20 transactions
    if len(transactions) >= batch_size:
        batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        df = pd.DataFrame(transactions)
        output_path = f"/data/transactions/transactions_batch_{batch_id}.parquet"
        df.to_parquet(output_path, index=False)
        print(f"Wrote {output_path} to shared volume")
        
        transactions = []
        batch_count += 1

# Écrire les transactions restantes
if transactions:
    batch_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    df = pd.DataFrame(transactions)
    output_path = f"/data/transactions/transactions_batch_{batch_id}.parquet"
    df.to_parquet(output_path, index=False)
    print(f"Wrote {output_path} to shared volume")