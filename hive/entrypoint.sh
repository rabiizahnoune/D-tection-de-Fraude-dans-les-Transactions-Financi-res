#!/bin/bash
rm -rf /tmp/metastore_db
hdfs namenode -format -force
/opt/hadoop/bin/hdfs namenode &
/opt/hadoop/bin/hdfs datanode &
sleep 10
/opt/hive/bin/schematool -dbType derby -initSchema
hdfs dfs -mkdir -p /data
hdfs dfs -put /data/*.parquet /data/
# Créer le répertoire /data/transactions dans HDFS s'il n'existe pas
echo "Creating /data/transactions in HDFS..."
hdfs dfs -mkdir -p /data/transactions
hdfs dfs -chmod -R 777 /data/transactions
exec /opt/hive/bin/hive --service hiveserver2 --hiveconf hive.server2.enable.doAs=false --hiveconf hive.server2.thrift.bind.host=0.0.0.0