"""
Script para configurar las conexiones necesarias en Airflow.
Ejecuta este script dentro del contenedor de Airflow:
docker exec -it airflow_centrosur python /opt/airflow/scripts/setup_connections.py
"""

import os
import sys
import logging
from airflow.models import Connection
from airflow.utils.session import create_session

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Definición de conexiones
CONNECTIONS = [
    {
        "conn_id": "postgres_centrosur",
        "conn_type": "postgres",
        "host": "postgres_centrosur",
        "schema": "centrosur",
        "login": "postgres",
        "password": "1150040812",
        "port": 5432,
        "description": "Conexión a PostgreSQL principal"
    },
    {
        "conn_id": "postgres_base_prueba",
        "conn_type": "postgres",
        "host": "postgres_centrosur",
        "schema": "base_prueba",
        "login": "postgres",
        "password": "1150040812",
        "port": 5432,
        "description": "Conexión a base de prueba"
    },
    {
        "conn_id": "nifi_api",
        "conn_type": "http",
        "host": "nifi_centrosur",
        "schema": "https",
        "port": 8443,
        "login": "admin",
        "password": "centrosur123",
        "extra": {
            "verify": False
        },
        "description": "Conexión a la API de NiFi"
    },
    {
        "conn_id": "kafka_centrosur",
        "conn_type": "kafka",
        "host": "kafka_centrosur",
        "port": 9092,
        "description": "Conexión a Kafka"
    }
]

def setup_connections():
    """Configura todas las conexiones definidas en CONNECTIONS"""
    with create_session() as session:
        for conn_info in CONNECTIONS:
            conn_id = conn_info.pop("conn_id")
            
            # Verificar si la conexión ya existe
            existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
            
            if existing_conn:
                logger.info(f"La conexión {conn_id} ya existe. Actualizando...")
                # Si existe, actualizar sus propiedades
                for key, value in conn_info.items():
                    if key != "extra":  # extra es un caso especial
                        setattr(existing_conn, key, value)
                
                # Manejar extra como un caso especial si es necesario
                if "extra" in conn_info:
                    existing_conn.set_extra(conn_info["extra"])
                
            else:
                logger.info(f"Creando nueva conexión: {conn_id}")
                # Crear nueva conexión
                new_conn = Connection(conn_id=conn_id, **conn_info)
                session.add(new_conn)
            
            # Guardar los cambios
            session.commit()
    
    logger.info("Configuración de conexiones completada")

if __name__ == "__main__":
    logger.info("Iniciando configuración de conexiones en Airflow")
    setup_connections()
    logger.info("Configuración finalizada")