FROM apache/airflow:2.8.1

USER root

# Instalar dependencias del sistema
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    iputils-ping \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar dependencias de Python
RUN pip install --no-cache-dir \
    kafka-python \
    psycopg2-binary \
    requests \
    pandas \
    sqlalchemy