from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import subprocess
import logging
import time
import os
import requests  # Importer requests pour envoyer des messages à Discord

# Configurer le logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# URL du webhook Discord (remplace par ton URL de webhook)
DISCORD_WEBHOOK_URL = "https://discord.com/api/webhooks/1352669441019740230/j7ez0ADpYLvH4F1Kc2bm26eAAv7CIfyinEtoe52OBerIM9fPExFg4alWGdx4ZSHQLP12"  # Remplace par ton URL

# Fonction pour envoyer un message à Discord
def send_discord_message(message, color="info"):
    """
    Envoie un message à un canal Discord via un webhook.
    :param message: Le message à envoyer
    :param color: Couleur de l'embed ("info" pour bleu, "success" pour vert, "error" pour rouge)
    """
    # Définir la couleur de l'embed
    colors = {
        "info": 0x3498db,    # Bleu
        "success": 0x2ecc71, # Vert
        "error": 0xe74c3c    # Rouge
    }
    embed_color = colors.get(color, colors["info"])  # Par défaut, bleu si la couleur n'est pas reconnue

    # Créer le payload pour Discord
    payload = {
        "embeds": [
            {
                "title": "Airflow Notification",
                "description": message,
                "color": embed_color,
                "timestamp": datetime.utcnow().isoformat(),
                "footer": {"text": "Airflow DAG: transactions_pipeline"}
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
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'on_success_callback': on_success_callback,
    'on_failure_callback': on_failure_callback,
}

# Définir le DAG
with DAG(
    'transactions_pipeline',
    default_args=default_args,
    description='Process transactions dynamically when new files are detected',
    schedule_interval=timedelta(minutes=8),
    start_date=datetime(2025, 3, 21),
    catchup=False,
    tags=['transactions', 'fraud_detection'],
) as dag:

    # Utiliser des Airflow Variables pour les chemins
    TRANSACTIONS_PATH = Variable.get("transactions_path", default_var="/data/transactions")
    PROCESSED_PATH = f"{TRANSACTIONS_PATH}/processed"
    FRAUD_THRESHOLD = float(Variable.get("fraud_amount_threshold", default_var=1000))

    # Tâche pour créer le répertoire des transactions et des fichiers traités
    create_transactions_dir = BashOperator(
        task_id='create_transactions_dir',
        bash_command=f"""
        docker exec kafka bash -c 'mkdir -p {TRANSACTIONS_PATH} && mkdir -p {PROCESSED_PATH}' && \
        docker exec kafka bash -c '[ -d {TRANSACTIONS_PATH} ] || (echo "Failed to create {TRANSACTIONS_PATH}" && exit 1)'
        """,
    )

    # Fonction pour lister les fichiers non traités
    def list_unprocessed_files(**context):
        max_wait_time = 60  # Attendre jusqu'à 1 minute
        poke_interval = 30  # Vérifier toutes les 30 secondes
        elapsed_time = 0  # Corrigé : initialiser à 0

        # Vérifier d'abord si le répertoire existe
        try:
            result = subprocess.run(
                ["docker", "exec", "-i", "kafka", "bash", "-c", f"[ -d {TRANSACTIONS_PATH} ]"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                message = f"❌ **Error**: Directory {TRANSACTIONS_PATH} does not exist in kafka container."
                send_discord_message(message, color="error")
                raise RuntimeError(f"Directory {TRANSACTIONS_PATH} does not exist in kafka container.")
        except Exception as e:
            message = f"❌ **Error**: Failed to check directory {TRANSACTIONS_PATH}: {str(e)}"
            send_discord_message(message, color="error")
            raise

        while elapsed_time < max_wait_time:
            try:
                logger.info(f"Listing files in {TRANSACTIONS_PATH}")
                result = subprocess.run(
                    ["docker", "exec", "-i", "kafka", "bash", "-c", f"ls {TRANSACTIONS_PATH}/transactions_batch_*.parquet 2>/dev/null"],
                    capture_output=True,
                    text=True,
                )
                logger.info(f"Command stdout: '{result.stdout}'")
                logger.info(f"Command stderr: '{result.stderr}'")
                logger.info(f"Command return code: {result.returncode}")

                if result.returncode != 0:
                    logger.warning(f"Command failed with return code {result.returncode}. Treating as no files found.")
                    time.sleep(poke_interval)
                    elapsed_time += poke_interval
                    continue

                if not result.stdout or result.stdout.strip() == '':
                    logger.warning(f"No unprocessed batch files found in {TRANSACTIONS_PATH}, waiting {poke_interval} seconds...")
                    time.sleep(poke_interval)
                    elapsed_time += poke_interval
                    continue

                files = result.stdout.strip().split('\n')
                files = [f.strip() for f in files if f.strip()]
                if not files:
                    logger.warning(f"No valid batch files found in {TRANSACTIONS_PATH}, waiting {poke_interval} seconds...")
                    time.sleep(poke_interval)
                    elapsed_time += poke_interval
                    continue

                # Fichiers trouvés : envoyer une notification Discord
                message = f"📂 **Files Found**: {len(files)} unprocessed files found in {TRANSACTIONS_PATH}: {files}"
                send_discord_message(message, color="info")
                logger.info(f"Found unprocessed files: {files}")
                return files  # Retourner la liste des fichiers

            except Exception as e:
                logger.error(f"Unexpected error: {str(e)}")
                time.sleep(poke_interval)
                elapsed_time += poke_interval
                continue

        # Aucun fichier trouvé après max_wait_time : envoyer une notification Discord
        message = f"ℹ️ **No Files**: No batch files found in {TRANSACTIONS_PATH} after waiting {max_wait_time} seconds."
        send_discord_message(message, color="info")
        logger.info(f"No batch files found in {TRANSACTIONS_PATH} after waiting {max_wait_time} seconds, skipping...")
        return []  # Retourner une liste vide si aucun fichier n'est trouvé

    # Tâche pour lister les fichiers non traités
    list_files = PythonOperator(
        task_id='list_unprocessed_files',
        python_callable=list_unprocessed_files,
        provide_context=True,
        do_xcom_push=True,
    )

    # Fonction pour créer les tâches pour chaque fichier
    def create_tasks_for_file(file_path, dag):
        batch_id = os.path.basename(file_path).split('transactions_batch_')[1].replace('.parquet', '')
        batch_id = f"batch_{batch_id}"

        copy_to_hdfs = BashOperator(
            task_id=f'copy_to_hdfs_{batch_id}',
            bash_command=f"""
            docker exec hive hdfs dfs -mkdir -p /data/transactions/transactions_{batch_id} && \
            docker exec hive hdfs dfs -put -f {file_path} /data/transactions/transactions_{batch_id}/
            """,
            dag=dag,
        )

        create_table = BashOperator(
            task_id=f'create_hive_table_{batch_id}',
            bash_command=f"""
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
            ALTER TABLE transactions ADD IF NOT EXISTS PARTITION (batch_id='{batch_id}') LOCATION '/data/transactions/transactions_{batch_id}';"
            """,
            dag=dag,
        )

        create_fraud_table = BashOperator(
            task_id=f'create_fraud_table_{batch_id}',
            bash_command=f"""
            docker exec hive beeline -u "jdbc:hive2://localhost:10000" --hiveconf hive.exec.dynamic.partition.mode=nonstrict -e "
            CREATE EXTERNAL TABLE IF NOT EXISTS fraud_detections (
                transaction_id STRING,
                date_time STRING,
                amount DOUBLE,
                customer_id STRING,
                location STRING,
                fraud_reason STRING
            )
            PARTITIONED BY (batch_id STRING)
            STORED AS PARQUET
            LOCATION '/data/fraud_detections';"
            """,
            dag=dag,
        )

        detect_fraud = BashOperator(
            task_id=f'detect_fraud_{batch_id}',
            bash_command=f"""
            docker exec hive spark-submit \
              --master spark://localhost:7077 \
              --driver-memory 1g \
              --executor-memory 1g \
              --num-executors 1 \
              --executor-cores 1 \
              /hive/scripts/detect_fraud.py "{batch_id}"
            """,
            dag=dag,
        )

        add_fraud_partition = BashOperator(
            task_id=f'add_fraud_partition_{batch_id}',
            bash_command=f"""
            docker exec hive beeline -u "jdbc:hive2://localhost:10000" --hiveconf hive.exec.dynamic.partition.mode=nonstrict -e "
            ALTER TABLE fraud_detections ADD IF NOT EXISTS PARTITION (batch_id='{batch_id}') LOCATION '/data/fraud_detections/batch_id={batch_id}';"
            """,
            dag=dag,
        )

        move_processed_file = BashOperator(
            task_id=f'move_processed_file_{batch_id}',
            bash_command=f"""
            docker exec kafka bash -c "mv {file_path} {PROCESSED_PATH}/" && \
            echo "File {file_path} moved to {PROCESSED_PATH}"
            """,
            on_success_callback=lambda context: send_discord_message(
                f"📦 **File Processed**: File `{file_path}` has been processed and moved to `{PROCESSED_PATH}`.",
                color="success"
            ),
            dag=dag,
        )

        list_files >> copy_to_hdfs >> create_table >> create_fraud_table >> detect_fraud >> add_fraud_partition >> move_processed_file

    # Fonction pour générer les tâches dynamiquement au moment de la construction du DAG
    def generate_tasks():
        try:
            result = subprocess.run(
                ["docker", "exec", "-i", "kafka", "bash", "-c", f"ls {TRANSACTIONS_PATH}/transactions_batch_*.parquet 2>/dev/null"],
                capture_output=True,
                text=True,
            )
            if result.returncode != 0:
                logger.warning(f"Command failed with return code {result.returncode} during DAG construction. Treating as no files found.")
                files = []
            elif result.stdout and result.stdout.strip():
                files = result.stdout.strip().split('\n')
                files = [f.strip() for f in files if f.strip()]
                logger.info(f"Found files at DAG construction time: {files}")
            else:
                files = []
                logger.info("No files found during DAG construction, no tasks will be generated.")
        except subprocess.CalledProcessError as e:
            logger.error(f"Error listing files at DAG construction time: {e.stderr}")
            files = []

        if files:
            for file_path in files:
                create_tasks_for_file(file_path, dag)
        else:
            logger.info("No files to process, DAG will complete successfully without generating tasks.")

    # Appeler la fonction pour générer les tâches au moment de la construction du DAG
    generate_tasks()

    # Définir les dépendances globales
    create_transactions_dir >> list_files