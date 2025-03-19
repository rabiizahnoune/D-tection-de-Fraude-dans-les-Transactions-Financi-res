FROM confluentinc/cp-kafka:7.3.0

USER root

# Installer wget si nécessaire, sans toucher à Python
RUN microdnf update -y && \
    microdnf install -y wget && \
    microdnf clean all

# Copier et installer les dépendances Python avec le pip existant
COPY requirements.txt .
RUN /usr/bin/pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /data/transactions && \
    chown appuser:appuser /data/transactions
# Revenir à l'utilisateur par défaut
USER appuser

# Commande par défaut pour démarrer Kafka
CMD ["bash", "-c", "/etc/confluent/docker/run"]