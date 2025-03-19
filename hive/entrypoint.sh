#!/bin/bash
set -e  # Arrête le script si une commande échoue

# Fonction pour arrêter les processus proprement
cleanup() {
    echo "Stopping Hadoop and Hive services..."
    kill $NAMENODE_PID $DATANODE_PID $HIVESERVER2_PID 2>/dev/null
    wait $NAMENODE_PID $DATANODE_PID $HIVESERVER2_PID 2>/dev/null
    echo "All services stopped."
    exit 0
}
trap cleanup SIGINT SIGTERM

# Vérifier et créer les répertoires locaux
echo "Creating local directories..."
mkdir -p /data/namenode /data/datanode /data/transactions /data/warehouse
chmod -R 777 /data
chown -R hive:hive /data  # Assurer que l'utilisateur hive peut y accéder

# Vérifier si HDFS est déjà formaté
if [ ! -d "/data/namenode/current" ]; then
    echo "Formatting HDFS NameNode..."
    /opt/hadoop/bin/hdfs namenode -format -force
fi

# Démarrer le NameNode
echo "Starting NameNode..."
/opt/hadoop/bin/hdfs namenode &
NAMENODE_PID=$!
sleep 10
if ! ps -p $NAMENODE_PID > /dev/null; then
    echo "NameNode failed to start. Check logs..."
    ls -lh /opt/hadoop/logs/  # Lister les fichiers pour trouver les logs
    cat /opt/hadoop/logs/hadoop-*namenode*.log || echo "No NameNode logs found"
    exit 1
fi

# Démarrer le DataNode
echo "Starting DataNode..."
rm -rf /data/datanode/*  # Nettoyer les anciennes données pour éviter les corruptions
/opt/hadoop/bin/hdfs datanode &
DATANODE_PID=$!
sleep 10
if ! ps -p $DATANODE_PID > /dev/null; then
    echo "DataNode failed to start. Check logs..."
    ls -lh /opt/hadoop/logs/  # Lister les fichiers pour trouver les logs
    cat /opt/hadoop/logs/hadoop-*datanode*.log || echo "No DataNode logs found"
    exit 1
fi

# Vérifier l'état de HDFS
echo "Checking HDFS status..."
if ! hdfs dfsadmin -report; then
    echo "HDFS failed to initialize properly. Check logs..."
    cat /opt/hadoop/logs/hadoop-*.log || echo "No logs found"
    exit 1
fi

# Créer les répertoires dans HDFS
echo "Creating directories in HDFS..."
hdfs dfs -mkdir -p /data/transactions /data/fraud_detections /tmp/hadoop /tmp/hive /user/hive/warehouse
hdfs dfs -chmod -R 777 /data/transactions /data/fraud_detections /tmp/hadoop /tmp/hive /user/hive/warehouse
hdfs dfs -chown -R hive:hive /data /tmp/hadoop /tmp/hive /user/hive/warehouse

# Initialiser le schéma Hive si nécessaire
if [ ! -d "/tmp/metastore_db" ]; then
    echo "Initializing Hive schema..."
    /opt/hive/bin/schematool -dbType derby -initSchema
fi

# Démarrer HiveServer2
echo "Starting HiveServer2..."
/opt/hive/bin/hive --service hiveserver2 --hiveconf hive.server2.enable.doAs=false --hiveconf hive.server2.thrift.bind.host=0.0.0.0 &
HIVESERVER2_PID=$!
sleep 5
if ! ps -p $HIVESERVER2_PID > /dev/null; then
    echo "HiveServer2 failed to start. Check logs..."
    cat /opt/hive/logs/hive.log || echo "No Hive logs found"
    exit 1
fi

# Attendre que les services restent en cours d'exécution
wait $NAMENODE_PID $DATANODE_PID $HIVESERVER2_PID