from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import sys

batch_id = sys.argv[1] if len(sys.argv) > 1 else "batch_0"

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("spark://localhost:7077") \
    .getOrCreate()

try:
    print(f"Spark version: {spark.version}")
    df_transactions = spark.read.parquet(f"/data/transactions/transactions_{batch_id}/*.parquet")

    # Filtrer les transactions frauduleuses
    df_fraud = df_transactions.filter(df_transactions.amount > 1000) \
        .select(
            "transaction_id",
            "date_time",
            "amount",
            "customer_id",
            "location",
            F.lit("High Amount").alias("fraud_reason")
        )

    # Écrire dans HDFS
    df_fraud.write.mode("overwrite").parquet("/data/fraud_detections")
except Exception as e:
    print(f"Error: {str(e)}")
    raise
finally:
    spark.stop()