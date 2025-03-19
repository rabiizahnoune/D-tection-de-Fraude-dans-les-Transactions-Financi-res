FROM apache/hive:3.1.3

USER root

# Installer les dépendances
RUN apt-get update && apt-get install -y python3 python3-pip wget curl

# Installer les bibliothèques Python nécessaires
RUN pip3 install pyhive thrift

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

# Configurer MapReduce avec YARN
RUN echo '<?xml version="1.0"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>mapreduce.framework.name</name>\n\
        <value>yarn</value>\n\
    </property>\n\
    <property>\n\
        <name>mapreduce.map.memory.mb</name>\n\
        <value>1024</value>\n\
    </property>\n\
    <property>\n\
        <name>mapreduce.reduce.memory.mb</name>\n\
        <value>1024</value>\n\
    </property>\n\
    <property>\n\
        <name>yarn.app.mapreduce.am.resource.mb</name>\n\
        <value>1024</value>\n\
    </property>\n\
</configuration>' > /opt/hadoop/etc/hadoop/mapred-site.xml

# Configurer YARN
RUN echo '<?xml version="1.0"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>yarn.resourcemanager.hostname</name>\n\
        <value>localhost</value>\n\
    </property>\n\
    <property>\n\
        <name>yarn.nodemanager.aux-services</name>\n\
        <value>mapreduce_shuffle</value>\n\
    </property>\n\
    <property>\n\
        <name>yarn.nodemanager.resource.memory-mb</name>\n\
        <value>2048</value>\n\
    </property>\n\
    <property>\n\
        <name>yarn.scheduler.maximum-allocation-mb</name>\n\
        <value>2048</value>\n\
    </property>\n\
</configuration>' > /opt/hadoop/etc/hadoop/yarn-site.xml

# Configurer Hive pour utiliser YARN
RUN echo '<?xml version="1.0"?>\n\
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>\n\
<configuration>\n\
    <property>\n\
        <name>hive.execution.engine</name>\n\
        <value>mr</value>\n\
    </property>\n\
    <property>\n\
        <name>hive.metastore.warehouse.dir</name>\n\
        <value>/data/warehouse</value>\n\
    </property>\n\
</configuration>' > /opt/hive/conf/hive-site.xml

# Définir les variables d'environnement
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Copier les scripts et entrypoint
COPY scripts/ /hive/scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Commande d'entrée
CMD ["/entrypoint.sh"]