"""
Script para monitorear el flujo completo de datos desde NiFi a Kafka y luego a la base de datos.
Este script se puede ejecutar como parte de un DAG de Airflow o de forma independiente.
"""

import json
import time
import logging
import requests
from kafka import KafkaConsumer, KafkaAdminClient, admin
import psycopg2
from datetime import datetime

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración
NIFI_ENDPOINT = "https://nifi_centrosur:8443/nifi-api"
KAFKA_BROKER = "kafka_centrosur:9092"
KAFKA_TOPIC = "potencia-data"
DB_CONFIG = {
    "host": "postgres_centrosur",
    "port": 5432,
    "database": "base_prueba",
    "user": "postgres",
    "password": "1150040812"
}
NIFI_CREDENTIALS = {
    "username": "admin",
    "password": "centrosur123"
}

# IDs importantes de NiFi
QUERY_DB_PROCESSOR_ID = "43f8b29f-0195-1000-995e-a0393bd40439"
CONSUME_KAFKA_PROCESSOR_ID = "3a91dca5-9c8d-3264-93a4-589f623cc61b"
PUT_DB_PROCESSOR_ID = "f5b11fb1-c240-3c6b-b32d-08b7e58d79a9"

def get_nifi_token():
    """Obtiene un token de autenticación para la API de NiFi"""
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
    }
    
    response = requests.post(
        f"{NIFI_ENDPOINT}/access/token",
        headers=headers,
        data=NIFI_CREDENTIALS,
        verify=False  # En producción, usa certificados válidos
    )
    
    if response.status_code == 201:
        return response.text
    else:
        logger.error(f"Error al obtener token: {response.status_code} - {response.text}")
        return None

def get_processor_status(processor_id, token):
    """Obtiene el estado actual de un procesador en NiFi"""
    headers = {
        'Authorization': f'Bearer {token}'
    }
    
    response = requests.get(
        f"{NIFI_ENDPOINT}/processors/{processor_id}",
        headers=headers,
        verify=False
    )
    
    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"Error al obtener estado: {response.status_code} - {response.text}")
        return None

def check_kafka_topic():
    """Verifica si el tema de Kafka existe y muestra detalles básicos"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BROKER,
            client_id='monitor_script'
        )
        
        topics = admin_client.list_topics()
        
        if KAFKA_TOPIC in topics:
            logger.info(f"Tema {KAFKA_TOPIC} encontrado en Kafka")
            
            # Obtener detalles del tema
            topic_details = admin_client.describe_topics([KAFKA_TOPIC])
            logger.info(f"Detalles del tema: {json.dumps(topic_details, indent=2)}")
            
            # Obtener los grupos de consumidores
            consumer_groups = admin_client.list_consumer_groups()
            logger.info(f"Grupos de consumidores disponibles: {consumer_groups}")
            
            admin_client.close()
            return True
        else:
            logger.error(f"Tema {KAFKA_TOPIC} no encontrado en Kafka")
            admin_client.close()
            return False
            
    except Exception as e:
        logger.error(f"Error al verificar el tema de Kafka: {str(e)}")
        return False

def sample_kafka_messages():
    """Lee algunas muestras de mensajes del tema de Kafka"""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='monitor_script_consumer',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # 10 segundos de timeout
        )
        
        logger.info("Esperando mensajes de Kafka (10 segundos)...")
        
        messages = []
        for message in consumer:
            messages.append(message.value)
            logger.info(f"Mensaje recibido: {message.value}")
            if len(messages) >= 5:  # Limitar a 5 mensajes
                break
                
        consumer.close()
        
        return messages
        
    except Exception as e:
        logger.error(f"Error al consumir mensajes de Kafka: {str(e)}")
        return []

def check_db_records():
    """Verifica los registros en la base de datos de destino"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Verificar tabla potencia_dep (origen)
        cursor.execute("SELECT COUNT(*) FROM potencia_dep")
        source_count = cursor.fetchone()[0]
        logger.info(f"Registros en potencia_dep (origen): {source_count}")
        
        # Verificar tabla potencia_dep_processed (destino)
        cursor.execute("SELECT COUNT(*) FROM potencia_dep_processed")
        dest_count = cursor.fetchone()[0]
        logger.info(f"Registros en potencia_dep_processed (destino): {dest_count}")
        
        # Obtener algunos ejemplos
        cursor.execute("SELECT * FROM potencia_dep_processed LIMIT 3")
        samples = cursor.fetchall()
        logger.info(f"Muestras de registros: {samples}")
        
        conn.close()
        
        return {
            "source_count": source_count,
            "dest_count": dest_count,
            "samples": samples
        }
        
    except Exception as e:
        logger.error(f"Error al verificar registros en base de datos: {str(e)}")
        return None

def run_monitor():
    """Ejecuta la monitorización completa del flujo"""
    logger.info("=== Iniciando monitoreo del flujo NiFi-Kafka-PostgreSQL ===")
    
    # Paso 1: Verificar NiFi
    token = get_nifi_token()
    if not token:
        logger.error("No se pudo obtener el token de NiFi. Abortando.")
        return False
    
    # Verificar procesadores principales
    processors_to_check = [
        ("QueryDatabaseTable (Productor)", QUERY_DB_PROCESSOR_ID),
        ("ConsumeKafka (Consumidor)", CONSUME_KAFKA_PROCESSOR_ID),
        ("PutDatabaseRecord (Inserción)", PUT_DB_PROCESSOR_ID)
    ]
    
    for name, pid in processors_to_check:
        processor_info = get_processor_status(pid, token)
        if processor_info:
            state = processor_info.get('component', {}).get('state')
            run_status = processor_info.get('component', {}).get('runStatus')
            logger.info(f"Procesador {name}: Estado={state}, RunStatus={run_status}")
        else:
            logger.error(f"No se pudo obtener información del procesador {name}")
    
    # Paso 2: Verificar Kafka
    if not check_kafka_topic():
        logger.error("Verificación de tema Kafka falló. Continuando con la monitorización...")
    
    # Paso 3: Muestrear mensajes de Kafka
    messages = sample_kafka_messages()
    if not messages:
        logger.warning("No se encontraron mensajes recientes en Kafka")
    
    # Paso 4: Verificar base de datos
    db_info = check_db_records()
    if not db_info:
        logger.error("Error al verificar la base de datos")
    
    # Resumen
    logger.info("=== Resumen de monitoreo ===")
    logger.info(f"Timestamp: {datetime.now().isoformat()}")
    if db_info:
        logger.info(f"Registros origen: {db_info['source_count']}")
        logger.info(f"Registros destino: {db_info['dest_count']}")
        
        if db_info['source_count'] > 0 and db_info['dest_count'] > 0:
            ratio = (db_info['dest_count'] / db_info['source_count']) * 100
            logger.info(f"Ratio de transferencia: {ratio:.2f}%")
    
    logger.info("=== Fin del monitoreo ===")
    return True

if __name__ == "__main__":
    run_monitor()