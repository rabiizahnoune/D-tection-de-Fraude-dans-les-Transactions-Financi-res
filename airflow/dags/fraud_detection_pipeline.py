from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import subprocess
import logging
import time

# Configurer le logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Fonction pour envoyer une alerte en cas d'échec
def on_failure_callback(context):
    dag_run = context.get('dag_run')
    task_instance = context.get('task_instance')
    subject = f"Airflow Alert: Task {task_instance.task_id} Failed in DAG {dag_run.dag_id}"
    body = f"""
    Task {task_instance.task_id} failed in DAG {dag_run.dag_id}.
    Execution Date: {dag_run.execution_date}
    Log URL: {task_instance.log_url}
    """
    # Envoyer un email (assure-toi que le SMTP est configuré dans airflow.cfg)
    send_email(
        to=Variable.get("alert_email", default_var="rabiizahnoune7@gmail.com"),
        subject=subject,
        html_content=body
    )
    logger.error(f"Task {task_instance.task_id} failed: {context.get('exception')}")

# Définir les arguments par défaut avec des callbacks pour les alertes
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,  # Ajouter des retries en cas d'échec
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': on_failure_callback,  # Callback pour les échecs
    'email_on_failure': True,  # Activer les emails en cas d'échec
    'email': Variable.get("alert_email", default_var="admin@example.com"),
    'sla': timedelta(minutes=45),  # SLA pour s'assurer que le DAG termine en moins de 45 minutes
}

# Définir le DAG avec un SLA
with DAG(
    'transactions_pipeline',
    default_args=default_args,
    description='Process transactions every 5 minutes with professional practices',
    schedule_interval=timedelta(minutes=50),
    start_date=datetime(2025, 3, 19),
    catchup=False,
    tags=['transactions', 'fraud_detection'],  # Ajouter des tags pour organiser les DAGs
    sla_miss_callback=on_failure_callback,  # Callback pour les SLA manqués
) as dag:

    # Utiliser des Airflow Variables pour les chemins et configurations
    TRANSACTIONS_PATH = Variable.get("transactions_path", default_var="/data/transactions")
    FRAUD_THRESHOLD = float(Variable.get("fraud_amount_threshold", default_var=1000))

    # Fonction pour récupérer le dernier batch_id avec un mécanisme d'attente
    def get_latest_batch_id(**context):
        max_wait_time = 3600  # Attendre jusqu'à 1 heure (en secondes)
        poke_interval = 60  # Vérifier toutes les 60 secondes
        elapsed_time = 0

        while elapsed_time < max_wait_time:
            try:
                logger.info(f"Listing files in {TRANSACTIONS_PATH}")
                result = subprocess.run(
                    ["docker", "exec", "-i", "kafka", "bash", "-c", f"ls -t {TRANSACTIONS_PATH}/transactions_batch_*.parquet"],
                    capture_output=True,
                    text=True,
                    check=True
                )
                logger.info(f"Command stdout: '{result.stdout}'")
                logger.info(f"Command stderr: '{result.stderr}'")
                
                if not result.stdout or result.stdout.strip() == '':
                    logger.warning(f"No batch files found in {TRANSACTIONS_PATH}, waiting {poke_interval} seconds...")
                    time.sleep(poke_interval)
                    elapsed_time += poke_interval
                    continue

                files = result.stdout.strip().split('\n')
                latest_file = files[0].strip()
                batch_id = latest_file.split('transactions_batch_')[1].replace('.parquet', '')
                if not batch_id:
                    logger.error("Could not extract batch ID from filename")
                    raise ValueError("Could not extract batch ID")
                
                logger.info(f"Latest file: {latest_file}")
                logger.info(f"Latest batch ID: batch_{batch_id}")
                return f"batch_{batch_id}"
            except subprocess.CalledProcessError as e:
                logger.error(f"Error executing command: {e.stderr}")
                raise
            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                raise

        # Si aucun fichier n'est trouvé après le temps d'attente
        logger.error(f"No batch files found in {TRANSACTIONS_PATH} after waiting {max_wait_time} seconds")
        raise TimeoutError(f"No batch files found after waiting {max_wait_time} seconds")

    # Tâche pour récupérer le batch_id
    get_batch_id = PythonOperator(
        task_id='get_batch_id',
        python_callable=get_latest_batch_id,
        provide_context=True,
        do_xcom_push=True,
    )

    # Tâche pour copier les fichiers dans HDFS
    copy_to_hdfs = BashOperator(
        task_id='copy_to_hdfs',
        bash_command="""
        docker exec hive hdfs dfs -mkdir -p /data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }} && \
        docker exec hive hdfs dfs -put -f {{ params.transactions_path }}/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}.parquet /data/transactions/transactions_{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}/
        """,
        params={'transactions_path': TRANSACTIONS_PATH},
    )

    # Tâche pour créer la table Hive et ajouter une partition
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
    )

    # Tâche pour créer la table des fraudes
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
    )

    # Tâche pour détecter les fraudes
    detect_fraud = BashOperator(
        task_id='detect_fraud',
        bash_command="""
        docker exec hive spark-submit --master spark://localhost:7077 /hive/scripts/detect_fraud.py "{{ ti.xcom_pull(task_ids='get_batch_id') or 'batch_0' }}"
        """,
    )

    # Définir les dépendances
    get_batch_id >> copy_to_hdfs >> create_table >> create_fraud_table >> detect_fraud