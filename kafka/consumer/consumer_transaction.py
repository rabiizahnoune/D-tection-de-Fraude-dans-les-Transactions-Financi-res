from kafka import KafkaConsumer
import pandas as pd
import json
import os
import time

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "transactions_topic"
OUTPUT_DIR = "/data/transactions"  # Volume partagé

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='latest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

if not os.path.exists(OUTPUT_DIR):
    try:
        os.makedirs(OUTPUT_DIR)
    except PermissionError:
        print(f"Permission denied for {OUTPUT_DIR}. Ensure directory exists and is writable.")
        exit(1)

data = []
file_counter = 0
while True:
    i = 0
    for message in consumer:
        data.append(message.value)
        print(f"Received transaction({i}): {message.value['transaction_id']}")
        i += 1
        if len(data) >= 2:
            df = pd.DataFrame(data)
            output_file = f"{OUTPUT_DIR}/transactions_batch_{file_counter}.parquet"
            df.to_parquet(output_file)
            print(f"Wrote {output_file} to shared volume")
            data = []
            file_counter += 1
    time.sleep(1)