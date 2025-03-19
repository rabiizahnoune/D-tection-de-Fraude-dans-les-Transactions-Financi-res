from flask import Flask, jsonify
import random
from datetime import datetime, timedelta
import time

app = Flask(__name__)

def random_date(start, end):
    return start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    
def generate_transaction():
    return {
        "transaction_id": f"T{random.randint(10000, 99999)}",
        "date_time": datetime.now().isoformat(),  # Temps réel
        "amount": random.uniform(10, 1000) * (10 if random.random() < 0.4 else 1),
        "currency": random.choice(["USD", "EUR", "GBP"]),
        "merchant_details": f"Merchant{random.randint(1, 20)}",
        "customer_id": f"C{random.randint(0, 99):03}",
        "transaction_type": random.choice(["purchase", "withdrawal"]),
        "location": f"City{random.randint(1, 10)}"
    }

@app.route('/generate/transaction', methods=['GET'])
def get_transaction():
    transaction = generate_transaction()
    time.sleep(1)  # Simule un délai de 1 seconde pour la génération
    return jsonify(transaction)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)