"""
Script de prueba para verificar la resolución dinámica de IDs de procesadores NiFi
"""
import logging
import sys
import os

# Añadir el directorio padre al PATH para importaciones absolutas
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Ahora importar usando ruta absoluta
from modules.fabrica_nifi import crear_instancia_nifi

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

logger = logging.getLogger(__name__)

def test_fabrica_nifi():
    """Prueba la fábrica de NiFi y la resolución dinámica de IDs"""
    logger.info("Iniciando prueba de fábrica NiFi...")
    
    # Crear instancia NiFi
    nifi = crear_instancia_nifi(proxy_url="http://localhost:5001")

    if nifi:
        logger.info("✅ Instancia NiFi creada correctamente")
        logger.info(f"ID Grupo Productor: {nifi.producer_group_id}")
        logger.info(f"ID Grupo Consumidor: {nifi.consumer_group_id}")
        
        # Verificar algunos IDs de procesadores
        procesadores_a_verificar = [
            "query_db_table_processor_id",
            "consume_kafka_processor_id",
            "publish_kafka_processor_id"
        ]
        
        for proc_attr in procesadores_a_verificar:
            if hasattr(nifi, proc_attr):
                proc_id = getattr(nifi, proc_attr)
                if proc_id:
                    logger.info(f"✅ {proc_attr}: {proc_id}")
                else:
                    logger.warning(f"⚠️ {proc_attr} es None")
            else:
                logger.warning(f"⚠️ {proc_attr} no existe en la instancia")
    else:
        logger.error("❌ No se pudo crear la instancia NiFi")

if __name__ == "__main__":
    test_fabrica_nifi()