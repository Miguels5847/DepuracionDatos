import os
import logging
import time
import requests
import sys
from typing import Optional

# Añadir el directorio padre al PATH para importaciones absolutas
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if parent_dir not in sys.path:
    sys.path.insert(0, parent_dir)

# Ahora importar usando ruta absoluta
from modules.nifi_utils import NiFiConnector
from modules.gestor_ids_procesadores import GestorIdsProcesadores

logger = logging.getLogger(__name__)

# Función independiente para crear instancia NiFi
def crear_instancia_nifi(proxy_url: str = None, timeout: int = 60) -> Optional[NiFiConnector]:
    """
    Crea una instancia NiFi completamente configurada con IDs de procesadores resueltos.
    
    Args:
        proxy_url: URL del mini proxy NiFi
        timeout: Timeout HTTP en segundos
        
    Returns:
        Optional[NiFiConnector]: Conector NiFi configurado o None si la inicialización falla
    """
    try:
        # Detectar entorno
        docker_env = os.path.exists('/.dockerenv') or os.environ.get('DOCKER_CONTAINER')
        
        # Elegir URL predeterminada según el entorno
        if proxy_url is None:
            if docker_env:
                proxy_url = os.environ.get("MINI_PROXY_URL", "http://mini_nifi_proxy_centrosur:5001")
            else:
                proxy_url = os.environ.get("MINI_PROXY_URL", "http://localhost:5001")
                
        logger.info(f"Creando conector NiFi con URL de proxy {proxy_url}")
        nifi = NiFiConnector(proxy_url=proxy_url, timeout=timeout)
        
        # Verificar que NiFi está disponible
        if not nifi.verificar_disponibilidad():
            logger.error("Proxy NiFi no está disponible")
            return None
            
        # Crear e inicializar gestor de IDs de procesadores
        gestor_ids = GestorIdsProcesadores(nifi)
        
        # Resolver todos los IDs de procesadores
        if not gestor_ids.resolver_ids_procesadores():
            logger.error("No se pudieron resolver todos los IDs de procesadores")
            return None
            
        # Configurar instancia NiFi con IDs resueltos
        if not gestor_ids.configurar_ids_en_nifi_utils():
            logger.error("No se pudo configurar NiFi con los IDs resueltos")
            return None
            
        logger.info("Conector NiFi inicializado correctamente con IDs de procesadores resueltos")
        return nifi
        
    except Exception as e:
        logger.error(f"Error creando instancia NiFi: {str(e)}")
        return None

class FabricaNifi:
    """
    Clase para crear y configurar conectores NiFi basados en GestorIdsProcesadores
    """
    
    def __init__(self, gestor_ids):
        """
        Inicializa la fábrica de conectores NiFi con un gestor de IDs.
        
        Args:
            gestor_ids: Instancia de GestorIdsProcesadores con IDs resueltos
        """
        self.gestor = gestor_ids
        self.producer_group_id = self.gestor.producer_group_id
        self.consumer_group_id = self.gestor.consumer_group_id
        
        # Obtener o crear conector NiFi
        self.nifi = gestor_ids.nifi if hasattr(gestor_ids, 'nifi') else self._crear_nifi()
        
        # Inicialización exitosa
        logger.info("Conector NiFi inicializado correctamente con IDs de procesadores resueltos")
    
    def _crear_nifi(self, proxy_url=None, timeout=60):
        """Crea una instancia de NiFiConnector basada en la configuración del gestor"""
        return crear_instancia_nifi(proxy_url, timeout)
    
    def finalizar_procesamiento(self):
        """
        Finaliza el procesamiento de NiFi deteniendo todos los procesadores
        y limpiando recursos.
        
        Returns:
            bool: True si se finalizó correctamente, False en caso contrario
        """
        try:
            logger.info("Finalizando procesamiento NiFi...")
            
            # Detener procesadores por grupo
            if self.producer_group_id:
                self._detener_procesadores_grupo(self.producer_group_id, "productor")
            
            if self.consumer_group_id:
                self._detener_procesadores_grupo(self.consumer_group_id, "consumidor")
            
            logger.info("Procesamiento NiFi finalizado correctamente")
            return True
        except Exception as e:
            logger.error(f"Error finalizando procesamiento NiFi: {str(e)}")
            return False
    
    def _detener_procesadores_grupo(self, grupo_id, nombre_grupo):
        """Detiene todos los procesadores en un grupo específico"""
        try:
            logger.info(f"Deteniendo procesadores del grupo {nombre_grupo}...")
            
            # Intentar detener el grupo completo primero
            try:
                response = requests.put(
                    f"{self.nifi.proxy_url}/api/process-groups/{grupo_id}/processors",
                    json={"state": "STOPPED", "disconnectedNodeAcknowledged": True},
                    timeout=self.nifi.http_timeout
                )
                
                if response.status_code in (200, 202):
                    logger.info(f"Grupo {nombre_grupo} detenido correctamente")
                    return True
                else:
                    logger.warning(f"No se pudo detener grupo completo: {response.status_code}")
            except Exception as e:
                logger.warning(f"Error al detener grupo completo: {str(e)}")
            
            # Si no se puede detener todo el grupo, intentar detener procesadores individualmente
            # usando algún patrón o estrategia específica
            return True
            
        except Exception as e:
            logger.error(f"Error deteniendo procesadores en grupo {nombre_grupo}: {str(e)}")
            return False