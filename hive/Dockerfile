FROM apache/hive:3.1.3

USER root

# Installer les dépendances
RUN apt-get update && apt-get install -y python3 python3-pip wget
RUN pip3 install pyhive thrift

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
    <property>\n\
        <name>dfs.data.dir</name>\n\
        <value>file:///data/transactions</value>\n\
    </property>\n\
</configuration>' > /opt/hadoop/etc/hadoop/hdfs-site.xml

# Configurer Hive
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

# Copier les scripts et entrypoint
COPY scripts/ /hive/scripts/
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Définir le répertoire de travail et les variables d'environnement
WORKDIR /opt/hive/bin
ENV HADOOP_HOME=/opt/hadoop
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

# Commande d'entrée
CMD ["/entrypoint.sh"]