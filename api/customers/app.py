from flask import Flask, jsonify
import random
import time

app = Flask(__name__)

def generate_customer():
    customer_id = f"C{random.randint(0, 999):03}"
    return {
        "customer_id": customer_id,
        "account_history": [],
        "demographics": {"age": random.randint(18, 70), "location": f"City{random.randint(1, 10)}"},
        "behavioral_patterns": {"avg_transaction_value": random.uniform(50, 500)}
    }

@app.route('/generate/customer', methods=['GET'])
def get_customer():
    customer = generate_customer()
    time.sleep(5)  # Générer un client toutes les 5 secondes
    return jsonify(customer)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)