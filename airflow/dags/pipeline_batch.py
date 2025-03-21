from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import json
import logging
import subprocess

# Configurer le logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL du webhook Discord
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1352669441019740230/j7ez0ADpYLvH4F1Kc2bm26eAAv7CIfyinEtoe52OBerIM9fPExFg4alWGdx4ZSHQLP12"

# Fonction pour envoyer un message à Discord
def send_discord_message(message, color="info"):
    colors = {
        "info": 0x3498db,    # Bleu
        "success": 0x2ecc71, # Vert
        "error": 0xe74c3c    # Rouge
    }
    embed_color = colors.get(color, colors["info"])

    # Raccourcir le message si nécessaire (limite Discord : 2000 caractères pour la description)
    if len(message) > 1500:
        message = message[:1500] + "... (message truncated)"

    payload = {
        "embeds": [
            {
                "title": "Airflow Notification",
                "description": message,
                "color": embed_color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Airflow DAG: external_customer_pipeline"}
            }
        ]
    }

    try:
        response = requests.post(DISCORD_WEBHOOK_URL, json=payload)
        if response.status_code != 204:
            logger.error(f"Failed to send Discord message: {response.status_code} - {response.text}")
        else:
            logger.info("Discord message sent successfully")
    except Exception as e:
        logger.error(f"Error sending Discord message: {str(e)}")

# Fonction de callback pour les succès
def on_success_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    message = f"✅ **Success**: Task `{task_id}` in DAG `{dag_id}` completed successfully on {execution_date}."
    send_discord_message(message, color="success")

# Fonction de callback pour les échecs
def on_failure_callback(context):
    dag_id = context['dag'].dag_id
    task_id = context['task_instance'].task_id
    execution_date = context['execution_date']
    exception = context.get('exception', 'Unknown error')
    message = f"❌ **Failure**: Task `{task_id}` in DAG `{dag_id}` failed on {execution_date}. Error: {exception}"
    send_discord_message(message, color="error")

# Définir les arguments par défaut
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'on_success_callback': on_success_callback,
    'on_failure_callback': on_failure_callback,
}

# Définir le DAG
with DAG(
    'external_customer_pipeline',
    default_args=default_args,
    description='Fetch data from external_data and customer APIs, store in HDFS, and load into Hive using XCom',
    schedule_interval=None,
    start_date=datetime(2025, 3, 20),  # Changé pour une date dans le passé
    catchup=False,
    tags=['external_data', 'customer', 'hdfs', 'hive'],
) as dag:

    # Chemins HDFS pour stocker les données
    HDFS_EXTERNAL_DATA_PATH = "/data/external_data"
    HDFS_CUSTOMER_DATA_PATH = "/data/customer"

    # URLs des API
    CUSTOMER_API_URL = "http://api-customers:5001/generate/customer"
    EXTERNAL_DATA_API_URL = "http://api-externaldata:5002/generate/externaldata"

    # Tâche pour créer les répertoires dans HDFS et ajuster les permissions
    create_hdfs_dirs = BashOperator(
        task_id='create_hdfs_dirs',
        bash_command=f"""
        docker exec hive hdfs dfs -mkdir -p {HDFS_EXTERNAL_DATA_PATH} && \
        docker exec hive hdfs dfs -mkdir -p {HDFS_CUSTOMER_DATA_PATH} && \
        docker exec hive hdfs dfs -chmod -R 777 {HDFS_EXTERNAL_DATA_PATH} && \
        docker exec hive hdfs dfs -chmod -R 777 {HDFS_CUSTOMER_DATA_PATH} && \
        docker exec hive hdfs dfs -test -d {HDFS_EXTERNAL_DATA_PATH} || (echo "Failed to create {HDFS_EXTERNAL_DATA_PATH}" && exit 1) && \
        docker exec hive hdfs dfs -test -d {HDFS_CUSTOMER_DATA_PATH} || (echo "Failed to create {HDFS_CUSTOMER_DATA_PATH}" && exit 1)
        """,
    )

    # Fonction pour récupérer les données de l'API external_data et les passer via XCom
    def fetch_external_data(**context):
        batch_id = context['execution_date'].strftime('%Y%m%d_%H%M%S')
        try:
            response = requests.get(EXTERNAL_DATA_API_URL, timeout=15)
            response.raise_for_status()
            data = response.json()

            # Convertir les données en JSON pour les passer via XCom
            data_json = json.dumps(data)
            context['ti'].xcom_push(key='external_data_json', value=data_json)
            context['ti'].xcom_push(key='batch_id', value=batch_id)

            message = f"📊 **External Data Fetched**: Batch `{batch_id}` data fetched successfully."
            send_discord_message(message, color="info")
        except Exception as e:
            message = f"❌ **Error Fetching External Data (Batch {batch_id})**: {str(e)}"
            send_discord_message(message, color="error")
            raise

    # Fonction pour récupérer les données de l'API customer et les passer via XCom
    def fetch_customer_data(**context):
        batch_id = context['execution_date'].strftime('%Y%m%d_%H%M%S')
        try:
            response = requests.get(CUSTOMER_API_URL, timeout=15)
            response.raise_for_status()
            data = response.json()

            # Convertir les données en JSON pour les passer via XCom
            data_json = json.dumps(data)
            context['ti'].xcom_push(key='customer_data_json', value=data_json)
            context['ti'].xcom_push(key='batch_id', value=batch_id)

            message = f"👤 **Customer Data Fetched**: Batch `{batch_id}` data fetched successfully."
            send_discord_message(message, color="info")
        except Exception as e:
            message = f"❌ **Error Fetching Customer Data (Batch {batch_id})**: {str(e)}"
            send_discord_message(message, color="error")
            raise

    # Fonction pour stocker les données external_data dans HDFS et les charger dans Hive
    def store_external_data_to_hdfs_and_hive(**context):
        ti = context['ti']
        batch_id = ti.xcom_pull(key='batch_id', task_ids='fetch_external_data')
        data_json = ti.xcom_pull(key='external_data_json', task_ids='fetch_external_data')

        # Créer un répertoire pour la partition
        partition_dir = f"{HDFS_EXTERNAL_DATA_PATH}/batch_id={batch_id}"
        hdfs_file_path = f"{partition_dir}/data.json"

        try:
            # Créer le répertoire de la partition dans HDFS
            try:
                subprocess.run(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-mkdir", "-p", partition_dir],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                subprocess.run(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-chmod", "-R", "777", partition_dir],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                error_message = f"Failed to create partition directory in HDFS: {e.stderr}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            # Écrire les données JSON dans HDFS dans le répertoire de la partition
            try:
                process = subprocess.Popen(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-put", "-", hdfs_file_path],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = process.communicate(input=data_json)
                if process.returncode != 0:
                    error_message = f"HDFS put command failed: {stderr}"
                    logger.error(error_message)
                    raise RuntimeError(error_message)
                logger.info(f"HDFS put command output: {stdout}")
            except Exception as e:
                error_message = f"Error writing to HDFS: {str(e)}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            # Créer la table Hive et ajouter une partition
            create_table_cmd = f"""
            docker exec hive beeline -u "jdbc:hive2://localhost:10000" --hiveconf hive.exec.dynamic.partition.mode=nonstrict -e "
            CREATE EXTERNAL TABLE IF NOT EXISTS external_data (
                blacklist_info ARRAY<STRING>,
                credit_scores MAP<STRING, INT>,
                fraud_reports MAP<STRING, INT>
            )
            PARTITIONED BY (batch_id STRING)
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION '{HDFS_EXTERNAL_DATA_PATH}';
            ALTER TABLE external_data ADD IF NOT EXISTS PARTITION (batch_id='{batch_id}');
            "
            """
            try:
                result = subprocess.run(
                    create_table_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                logger.info(f"Hive command output: {result.stdout}")
            except subprocess.CalledProcessError as e:
                error_message = f"Hive command failed: {e.stderr}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            message = f"📊 **External Data Processed**: Batch `{batch_id}` stored in HDFS at {hdfs_file_path} and loaded into Hive table `external_data`."
            send_discord_message(message, color="info")
        except Exception as e:
            message = f"❌ **Error Processing External Data (Batch {batch_id})**: {str(e)}"
            send_discord_message(message, color="error")
            raise

    # Fonction pour stocker les données customer dans HDFS et les charger dans Hive
    def store_customer_data_to_hdfs_and_hive(**context):
        ti = context['ti']
        batch_id = ti.xcom_pull(key='batch_id', task_ids='fetch_customer_data')
        data_json = ti.xcom_pull(key='customer_data_json', task_ids='fetch_customer_data')

        # Créer un répertoire pour la partition
        partition_dir = f"{HDFS_CUSTOMER_DATA_PATH}/batch_id={batch_id}"
        hdfs_file_path = f"{partition_dir}/data.json"

        try:
            # Créer le répertoire de la partition dans HDFS
            try:
                subprocess.run(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-mkdir", "-p", partition_dir],
                    check=True,
                    capture_output=True,
                    text=True,
                )
                subprocess.run(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-chmod", "-R", "777", partition_dir],
                    check=True,
                    capture_output=True,
                    text=True,
                )
            except subprocess.CalledProcessError as e:
                error_message = f"Failed to create partition directory in HDFS: {e.stderr}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            # Écrire les données JSON dans HDFS dans le répertoire de la partition
            try:
                process = subprocess.Popen(
                    ["docker", "exec", "hive", "hdfs", "dfs", "-put", "-", hdfs_file_path],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, stderr = process.communicate(input=data_json)
                if process.returncode != 0:
                    error_message = f"HDFS put command failed: {stderr}"
                    logger.error(error_message)
                    raise RuntimeError(error_message)
                logger.info(f"HDFS put command output: {stdout}")
            except Exception as e:
                error_message = f"Error writing to HDFS: {str(e)}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            # Créer la table Hive et ajouter une partition
            create_table_cmd = f"""
            docker exec hive beeline -u "jdbc:hive2://localhost:10000" --hiveconf hive.exec.dynamic.partition.mode=nonstrict -e "
            CREATE EXTERNAL TABLE IF NOT EXISTS customers (
                customer_id STRING,
                account_history ARRAY<STRING>,
                demographics STRUCT<age: INT, location: STRING>,
                behavioral_patterns STRUCT<avg_transaction_value: DOUBLE>
            )
            PARTITIONED BY (batch_id STRING)
            ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
            STORED AS TEXTFILE
            LOCATION '{HDFS_CUSTOMER_DATA_PATH}';
            ALTER TABLE customers ADD IF NOT EXISTS PARTITION (batch_id='{batch_id}');
            "
            """
            try:
                result = subprocess.run(
                    create_table_cmd,
                    shell=True,
                    check=True,
                    capture_output=True,
                    text=True,
                )
                logger.info(f"Hive command output: {result.stdout}")
            except subprocess.CalledProcessError as e:
                error_message = f"Hive command failed: {e.stderr}"
                logger.error(error_message)
                raise RuntimeError(error_message)

            message = f"👤 **Customer Data Processed**: Batch `{batch_id}` stored in HDFS at {hdfs_file_path} and loaded into Hive table `customers`."
            send_discord_message(message, color="info")
        except Exception as e:
            message = f"❌ **Error Processing Customer Data (Batch {batch_id})**: {str(e)}"
            send_discord_message(message, color="error")
            raise

    # Tâches pour récupérer et stocker les données
    fetch_external_data_task = PythonOperator(
        task_id='fetch_external_data',
        python_callable=fetch_external_data,
        provide_context=True,
    )

    fetch_customer_data_task = PythonOperator(
        task_id='fetch_customer_data',
        python_callable=fetch_customer_data,
        provide_context=True,
    )

    store_external_data_task = PythonOperator(
        task_id='store_external_data_to_hdfs_and_hive',
        python_callable=store_external_data_to_hdfs_and_hive,
        provide_context=True,
    )

    store_customer_data_task = PythonOperator(
        task_id='store_customer_data_to_hdfs_and_hive',
        python_callable=store_customer_data_to_hdfs_and_hive,
        provide_context=True,
    )

    # Définir les dépendances
    create_hdfs_dirs >> fetch_external_data_task >> store_external_data_task
    create_hdfs_dirs >> fetch_customer_data_task >> store_customer_data_task