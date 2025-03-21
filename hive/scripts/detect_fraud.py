from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys
import os

# Récupérer le batch_id depuis les arguments
batch_id = sys.argv[1] if len(sys.argv) > 1 else "batch_0"

# Initialiser la session Spark
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("spark://localhost:7077") \
    .config("spark.driver.memory", "1g") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.executor.instances", "1") \
    .getOrCreate()

try:
    print(f"Spark version: {spark.version}")
    # Lire les données depuis HDFS
    df_transactions = spark.read.parquet(f"hdfs://localhost:9000/data/transactions/transactions_{batch_id}/*.parquet")

    # Filtrer les transactions frauduleuses et ajouter la colonne batch_id
    df_fraud = df_transactions.filter(df_transactions.amount > 1000) \
        .select(
            "transaction_id",
            "date_time",
            "amount",
            "customer_id",
            "location",
            F.lit("High Amount").alias("fraud_reason"),
            F.lit(batch_id).alias("batch_id")  # Ajouter la colonne batch_id
        )

    # Écrire dans HDFS avec partitionnement par batch_id
    df_fraud.write.partitionBy("batch_id").mode("append").parquet("hdfs://localhost:9000/data/fraud_detections")
except Exception as e:
    print(f"Error: {str(e)}")
    raise
finally:
    spark.stop()