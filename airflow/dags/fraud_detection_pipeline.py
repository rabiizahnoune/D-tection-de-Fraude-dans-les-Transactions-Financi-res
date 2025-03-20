from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transactions_pipeline',
    default_args=default_args,
    description='Process transactions every 5 minutes',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2025, 3, 19),
    catchup=False,
)

def get_latest_batch_id():
    try:
        result = subprocess.run(
            ["docker", "exec", "-i", "kafka", "bash", "-c", "ls -t /data/transactions/transactions_batch_*.parquet"],
            capture_output=True,
            text=True
        )
        print(f"Command stdout: '{result.stdout}'")
        print(f"Command stderr: '{result.stderr}'")
        
        if result.stderr and "No such file" in result.stderr:
            print("No batch files found in /data/transactions")
            return "batch_0"

        files = result.stdout.strip().split('\n')
        if not files or files[0] == '':
            print("No batch files found in /data/transactions")
            return "batch_0"

        latest_file = files[0].strip()
        batch_id = latest_file.split('transactions_batch_')[1].replace('.parquet', '')
        if not batch_id:
            print("Could not extract batch ID from filename")
            return "batch_0"
        
        print(f"Latest file: {latest_file}")
        print(f"Latest batch ID: batch_{batch_id}")
        return f"batch_{batch_id}"
    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e.stderr}")
        raise
    except Exception as e:
        print(f"Unexpected error: {str(e)}")
        raise

get_batch_id = PythonOperator(
    task_id='get_batch_id',
    python_callable=get_latest_batch_id,
    do_xcom_push=True,
    dag=dag,
)

copy_to_hdfs = BashOperator(
    task_id='copy_to_hdfs',
    bash_command="docker exec hive hdfs dfs -mkdir -p /data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }} && docker exec hive hdfs dfs -put -f /data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}.parquet /data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}/",
    dag=dag,
)

create_table = BashOperator(
    task_id='create_hive_table',
    bash_command="""
    docker exec hive beeline -u "jdbc:hive2://localhost:10000" --hiveconf hive.exec.dynamic.partition.mode=nonstrict -e "
    CREATE EXTERNAL TABLE IF NOT EXISTS transactions (
        transaction_id STRING,
        date_time STRING,
        amount DOUBLE,
        currency STRING,
        merchant_details STRING,
        customer_id STRING,
        transaction_type STRING,
        location STRING
    )
    PARTITIONED BY (batch_id STRING)
    STORED AS PARQUET;
    ALTER TABLE transactions ADD IF NOT EXISTS PARTITION (batch_id='{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}') LOCATION '/data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}';"
    """,
    dag=dag,
)

create_fraud_table = BashOperator(
    task_id='create_fraud_table',
    bash_command="""
    docker exec hive beeline -u "jdbc:hive2://localhost:10000" -e "
    CREATE EXTERNAL TABLE IF NOT EXISTS fraud_detections (
        transaction_id STRING,
        date_time STRING,
        amount DOUBLE,
        customer_id STRING,
        location STRING,
        fraud_reason STRING
    )
    STORED AS PARQUET
    LOCATION '/data/fraud_detections';"
    """,
    dag=dag,
)

detect_fraud = BashOperator(
    task_id='detect_fraud',
    bash_command="""
    docker exec hive spark-submit --master spark://localhost:7077 /hive/scripts/detect_fraud.py "{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}"
    """,
    dag=dag,
)

get_batch_id >> copy_to_hdfs >> create_table >> create_fraud_table >> detect_fraud