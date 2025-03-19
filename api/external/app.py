from flask import Flask, jsonify
import random
import time

app = Flask(__name__)

def generate_external_data():
    return {
        "blacklist_info": [f"Merchant{random.randint(21, 30)}" for _ in range(10)],
        "credit_scores": {f"C{random.randint(0, 99):03}": random.randint(300, 850) for _ in range(10)},
        "fraud_reports": {f"C{random.randint(0, 99):03}": random.randint(0, 5) for _ in range(10)}
    }

@app.route('/generate/externaldata', methods=['GET'])
def get_external_data():
    external_data = generate_external_data()
    time.sleep(60)  # Mise à jour toutes les minutes
    return jsonify(external_data)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5002)