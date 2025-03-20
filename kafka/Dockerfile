FROM confluentinc/cp-kafka:7.3.0

USER root

# Installer wget et les locales
RUN microdnf update -y && \
    microdnf install -y wget glibc-langpack-en && \
    microdnf clean all

# Configurer la locale UTF-8
ENV LANG=en_US.UTF-8
ENV LC_ALL=en_US.UTF-8

# Copier et installer les dépendances Python avec le pip existant
COPY requirements.txt .
RUN /usr/bin/pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /data/transactions && \
    chown appuser:appuser /data/transactions
# Revenir à l'utilisateur par défaut
USER appuser

# Commande par défaut pour démarrer Kafka
CMD ["bash", "-c", "/etc/confluent/docker/run"]