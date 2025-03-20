FROM apache/hive:3.1.3

USER root

# Installer les dépendances
RUN apt-get update && apt-get install -y python3 python3-pip wget curl

# Installer les bibliothèques Python nécessaires
RUN pip3 install pyhive thrift python-dotenv

# Créer les répertoires nécessaires pour HDFS
RUN mkdir -p /data/namenode /data/datanode /data/transactions /data/warehouse && \
    chmod -R 777 /data

# Configurer Hadoop (HDFS)
RUN echo '<?xml version="1.0"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>fs.defaultFS</name>\n\
        <value>hdfs://localhost:9000</value>\n\
    </property>\n\
</configuration>' > /opt/hadoop/etc/hadoop/core-site.xml

RUN echo '<?xml version="1.0"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>dfs.replication</name>\n\
        <value>1</value>\n\
    </property>\n\
    <property>\n\
        <name>dfs.namenode.name.dir</name>\n\
        <value>file:///data/namenode</value>\n\
    </property>\n\
    <property>\n\
        <name>dfs.datanode.data.dir</name>\n\
        <value>file:///data/datanode</value>\n\
    </property>\n\
</configuration>' > /opt/hadoop/etc/hadoop/hdfs-site.xml

# Télécharger et installer Spark
RUN curl -O https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz && \
    tar -xvzf spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    rm spark-3.5.0-bin-hadoop3.tgz


# Configurer Hive pour utiliser Spark
RUN echo '<?xml version="1.0"?>\n\
<configuration>\n\
    <property>\n\
        <name>hive.execution.engine</name>\n\
        <value>spark</value>\n\
    </property>\n\
    <property>\n\
        <name>hive.metastore.warehouse.dir</name>\n\
        <value>/data/warehouse</value>\n\
    </property>\n\
    <property>\n\
        <name>spark.master</name>\n\
        <value>spark://hive:7077</value>\n\
    </property>\n\
    <property>\n\
        <name>spark.sql.catalogImplementation</name>\n\
        <value>hive</value>\n\
    </property>\n\
    <property>\n\
        <name>spark.home</name>\n\
        <value>/opt/spark-3.5.0-bin-hadoop3</value>\n\
    </property>\n\
    <property>\n\
        <name>spark.driver.memory</name>\n\
        <value>2g</value>\n\
    </property>\n\
    <property>\n\
        <name>spark.executor.memory</name>\n\
        <value>2g</value>\n\
    </property>\n\
</configuration>' > /opt/hive/conf/hive-site.xml

# Configurer Spark (facultatif, pour les defaults)
RUN echo "spark.master                     spark://localhost:7077" >> /opt/spark-3.5.0-bin-hadoop3/conf/spark-defaults.conf && \
    echo "spark.sql.warehouse.dir         /data/warehouse" >> /opt/spark-3.5.0-bin-hadoop3/conf/spark-defaults.conf && \
    echo "spark.hadoop.fs.defaultFS        hdfs://localhost:9000" >> /opt/spark-3.5.0-bin-hadoop3/conf/spark-defaults.conf


# Définir les variables d'environnement
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark-3.5.0-bin-hadoop3
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin


# Copier les scripts et entrypoint
COPY scripts/ /hive/scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Commande d'entrée
CMD ["/entrypoint.sh"]