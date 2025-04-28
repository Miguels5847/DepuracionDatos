FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    curl \
    iputils-ping \
    dnsutils \
    net-tools \
    procps \
    openssl \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias de Python
RUN pip install --no-cache-dir requests==2.28.1

# Crear directorio temporal
RUN mkdir -p /tmp && chmod 777 /tmp

# Variables de entorno
ENV PYTHONUNBUFFERED=1
ENV PYTHONHTTPSVERIFY=0

# Exponer puerto
EXPOSE 5000

# Copiar el código al final para aprovechar el caché de capas
COPY proxy.py /app/proxy.py

# Comando de inicio
CMD ["python", "proxy.py"]