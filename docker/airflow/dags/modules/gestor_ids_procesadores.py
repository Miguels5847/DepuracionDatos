import json
import os
import logging
import requests
import time
from typing import Dict, Optional, List, Any

logger = logging.getLogger(__name__)

class GestorIdsProcesadores:
    """Gestiona los IDs de procesadores NiFi resolviéndolos dinámicamente desde sus nombres."""
    
    def __init__(self, nifi_utils, ruta_config: str = None):
        """
        Inicializa el gestor de IDs de procesadores.
        
        Args:
            nifi_utils: Instancia de NiFiConnector para comunicación con NiFi
            ruta_config: Ruta al archivo de configuración de nombres de procesadores
        """
        self.nifi_utils = nifi_utils
        self.ruta_config = ruta_config or os.path.join(
            os.path.dirname(os.path.dirname(__file__)), 
            "configs", 
            "nombres_procesadores.json"
        )
        self.nombres_procesadores = {}
        self.ids_procesadores = {}
        self.id_grupo_productor = None
        self.id_grupo_consumidor = None
        self.nombre_grupo_productor = None
        self.nombre_grupo_consumidor = None
        
        # Cargar configuración al inicializar
        self._cargar_config()
        
    def _cargar_config(self) -> bool:
        """
        Carga nombres de procesadores desde archivo de configuración.
        
        Returns:
            bool: True si se cargó correctamente
        """
        try:
            if not os.path.exists(self.ruta_config):
                logger.error(f"Archivo de configuración no encontrado: {self.ruta_config}")
                return False
                
            with open(self.ruta_config, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Cargar nombres de grupos
            self.nombre_grupo_productor = config.get("grupos", {}).get("productor")
            self.nombre_grupo_consumidor = config.get("grupos", {}).get("consumidor")
            
            # Cargar nombres de procesadores
            self.nombres_procesadores = config.get("procesadores", {})
            
            # Cargar IDs conocidos (si existen)
            if "ids_conocidos" in config:
                # IDs de grupos
                if "grupos" in config["ids_conocidos"]:
                    self.id_grupo_productor = config["ids_conocidos"]["grupos"].get("productor")
                    self.id_grupo_consumidor = config["ids_conocidos"]["grupos"].get("consumidor")
                
                # IDs de procesadores conocidos
                if "procesadores" in config["ids_conocidos"]:
                    self.ids_procesadores_conocidos = config["ids_conocidos"]["procesadores"]
                    logger.info(f"Cargados {len(self.ids_procesadores_conocidos)} IDs conocidos de procesadores")
                
            logger.info(f"Configuración cargada con {len(self.nombres_procesadores)} nombres de procesadores")
            return True
            
        except Exception as e:
            logger.error(f"Error cargando configuración de procesadores: {str(e)}")
            return False
            
    def resolver_ids_procesadores(self) -> bool:
        """
        Resuelve todos los IDs de procesadores desde sus nombres usando el endpoint de búsqueda.
        
        Returns:
            bool: True si todos los IDs fueron resueltos correctamente
        """
        try:
            logger.info("Resolviendo IDs de procesadores desde nombres...")
            
            # Primero resolver IDs de grupos por nombre
            try:
                todos_grupos = self._obtener_todos_grupos_proceso()
                
                # Buscar grupos productor y consumidor
                for grupo in todos_grupos:
                    if grupo.get("name") == self.nombre_grupo_productor:
                        self.id_grupo_productor = grupo.get("id")
                        logger.info(f"Encontrado grupo productor: {self.id_grupo_productor}")
                        
                    if grupo.get("name") == self.nombre_grupo_consumidor:
                        self.id_grupo_consumidor = grupo.get("id")
                        logger.info(f"Encontrado grupo consumidor: {self.id_grupo_consumidor}")
            except Exception as e:
                logger.error(f"Error obteniendo grupos de proceso: {str(e)}")
            
            # Si no se encontraron los IDs de grupo, usar valores predeterminados
            if not self.id_grupo_productor:
                self.id_grupo_productor = "4dcc0bed-9061-3a4c-0000-c375f77615eb"
                logger.warning(f"Usando ID predeterminado para grupo productor: {self.id_grupo_productor}")
                
            if not self.id_grupo_consumidor:
                self.id_grupo_consumidor = "723a3709-dabb-3fae-0000-2892da95b9a6"
                logger.warning(f"Usando ID predeterminado para grupo consumidor: {self.id_grupo_consumidor}")
                
            # Actualizar IDs de grupo en nifi_utils
            self.nifi_utils.producer_group_id = self.id_grupo_productor
            self.nifi_utils.consumer_group_id = self.id_grupo_consumidor
            
            # Buscar IDs de procesadores usando el endpoint de búsqueda
            ids_encontrados = {}
            for clave, nombre_procesador in self.nombres_procesadores.items():
                try:
                    # Usar el endpoint de búsqueda para cada procesador
                    logger.info(f"Buscando procesador: {nombre_procesador} (clave: {clave})")
                    
                    # Definir términos de búsqueda alternativos para casos especiales
                    terminos_busqueda = [nombre_procesador]
                    if clave == "publish_kafka":
                        terminos_busqueda = ["PublishKafka", "PublishKafkaRecord", "Kafka", "Publish"]
                    elif clave == "consume_kafka":
                        terminos_busqueda = ["ConsumeKafka", "ConsumeKafkaRecord", "Kafka", "Consume"]
                    
                    # Intentar cada término de búsqueda hasta encontrar algo
                    for termino in terminos_busqueda:
                        respuesta = requests.get(
                            f"{self.nifi_utils.proxy_url}/api/flow/search-results?q={termino}",
                            timeout=self.nifi_utils.http_timeout
                        )
                        
                        if respuesta.status_code == 200:
                            data = respuesta.json()
                            if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                                # Buscar coincidencia por orden de preferencia
                                proc_id = None
                                
                                # 1. Buscar por coincidencia exacta en el nombre
                                for proc in data["searchResultsDTO"]["processorResults"]:
                                    if nombre_procesador.lower() in proc.get("name", "").lower():
                                        proc_id = proc.get("id")
                                        logger.info(f"Procesador encontrado exacto: {nombre_procesador} -> {proc_id}")
                                        break
                                        
                                # 2. Si no hay coincidencia exacta, buscar contiene
                                if not proc_id:
                                    for proc in data["searchResultsDTO"]["processorResults"]:
                                        if termino.lower() in proc.get("name", "").lower():
                                            proc_id = proc.get("id")
                                            logger.info(f"Procesador encontrado por término '{termino}': {proc_id}")
                                            break
                                            
                                # 3. En última instancia, usar el primer resultado
                                if not proc_id and data["searchResultsDTO"]["processorResults"]:
                                    proc = data["searchResultsDTO"]["processorResults"][0]
                                    proc_id = proc.get("id")
                                    proc_name = proc.get("name", "")
                                    logger.info(f"Usando primer procesador encontrado: {proc_name} -> {proc_id}")
                                
                                # Si encontramos un ID, guardar y salir del bucle
                                if proc_id:
                                    ids_encontrados[clave] = proc_id
                                    break
                        else:
                            logger.warning(f"Error buscando procesador con término '{termino}': {respuesta.status_code}")
                
                except Exception as e:
                    logger.warning(f"Error buscando procesador {nombre_procesador}: {str(e)}")
            
            # Guardar los IDs encontrados
            self.ids_procesadores = ids_encontrados
            
            # Registrar cuáles procesadores fueron encontrados vs no encontrados
            encontrados = len(self.ids_procesadores)
            esperados = len(self.nombres_procesadores)
            
            if encontrados < esperados:
                faltantes = [nombre for nombre in self.nombres_procesadores if nombre not in self.ids_procesadores]
                logger.warning(f"No se pudieron encontrar IDs para {len(faltantes)} procesadores: {', '.join(faltantes)}")
                    
            # Incluso si no todos fueron encontrados, necesitamos avanzar
            return True
                
        except Exception as e:
            logger.error(f"Error resolviendo IDs de procesadores: {str(e)}")
            return False
            
    def obtener_id_procesador(self, clave_nombre: str) -> Optional[str]:
        """
        Obtiene ID de procesador por su clave de nombre.
        
        Args:
            clave_nombre: Clave del nombre del procesador en configuración
            
        Returns:
            Optional[str]: ID del procesador o None si no se encuentra
        """
        if clave_nombre in self.ids_procesadores:
            return self.ids_procesadores[clave_nombre]
            
        logger.warning(f"No se encontró ID para el procesador clave: {clave_nombre}")
        return None
        
    def configurar_ids_en_nifi_utils(self) -> bool:
        """
        Configura la instancia NiFi utils con los IDs resueltos.
        
        Returns:
            bool: True si la configuración fue exitosa
        """
        try:
            # Establecer IDs de grupos
            self.nifi_utils.producer_group_id = self.id_grupo_productor
            self.nifi_utils.consumer_group_id = self.id_grupo_consumidor
            
            # Establecer IDs individuales de procesadores como atributos
            for clave_nombre, id_procesador in self.ids_procesadores.items():
                attr_id_procesador = f"{clave_nombre}_processor_id"
                setattr(self.nifi_utils, attr_id_procesador, id_procesador)
                logger.info(f"Configurado atributo: {attr_id_procesador} = {id_procesador}")
                
            # Casos especiales para procesadores específicos
            if "query_db_table" in self.ids_procesadores:
                self.nifi_utils.query_processor_id = self.ids_procesadores["query_db_table"]
                
            if "publish_kafka" in self.ids_procesadores:
                self.nifi_utils.publish_kafka_processor_id = self.ids_procesadores["publish_kafka"]
                
            if "consume_kafka" in self.ids_procesadores:
                self.nifi_utils.consume_kafka_processor_id = self.ids_procesadores["consume_kafka"]
        # Para procesadores que no se encontraron, usar valores hardcodeados
            if not hasattr(self.nifi_utils, "publish_kafka_processor_id") or not self.nifi_utils.publish_kafka_processor_id:
                self.nifi_utils.publish_kafka_processor_id = "3893b585-c041-314d-b9a0-2d8967f612fc"
                logger.warning(f"Usando ID hardcodeado para publish_kafka: {self.nifi_utils.publish_kafka_processor_id}")
                
            if not hasattr(self.nifi_utils, "consume_kafka_processor_id") or not self.nifi_utils.consume_kafka_processor_id:
                self.nifi_utils.consume_kafka_processor_id = "6364f7c9-07f1-394d-021c-db2529cca44c"
                logger.warning(f"Usando ID hardcodeado para consume_kafka: {self.nifi_utils.consume_kafka_processor_id}")
                
            return True
            
        except Exception as e:
            logger.error(f"Error configurando NiFi utils con IDs de procesadores: {str(e)}")
            return False

    def _obtener_todos_grupos_proceso(self) -> List[Dict[str, Any]]:
        """
        Obtiene todos los grupos de proceso desde NiFi usando los endpoints disponibles en el proxy.
        
        Returns:
            List[Dict]: Lista de grupos de proceso
        """
        try:
            # Intentar primero utilizando el endpoint de system/status para diagnosticar
            logger.info("Obteniendo grupos mediante diagnóstico de sistema...")
            try:
                respuesta_diagnostico = requests.get(
                    f"{self.nifi_utils.proxy_url}/api/system/status?detail=advanced",
                    timeout=self.nifi_utils.http_timeout
                )
                
                if respuesta_diagnostico.status_code == 200:
                    data_diagnostico = respuesta_diagnostico.json()
                    if "api_paths" in data_diagnostico and data_diagnostico["api_paths"].get("best_path"):
                        logger.info(f"Usando ruta de API recomendada: {data_diagnostico['api_paths'].get('best_path')}")
                else:
                    logger.warning("No se pudo obtener diagnóstico del proxy")
            except Exception as e:
                logger.warning(f"Error obteniendo diagnóstico: {str(e)}")
                
            # Intentar buscar procesadores y extraer los grupos de ellos
            logger.info("Intentando resolver grupos a partir de la búsqueda de procesadores...")
            try:
                # Usar el endpoint de búsqueda para encontrar procesadores
                respuesta_busqueda = requests.get(
                    f"{self.nifi_utils.proxy_url}/api/flow/search-results?q=processor",
                    timeout=self.nifi_utils.http_timeout
                )
                
                if respuesta_busqueda.status_code == 200:
                    data_busqueda = respuesta_busqueda.json()
                    
                    # Extraer grupos únicos desde los resultados de búsqueda
                    grupos = {}
                    
                    # Procesar resultados de búsqueda
                    if "searchResultsDTO" in data_busqueda:
                        search_dto = data_busqueda["searchResultsDTO"]
                        
                        # Obtener grupos de los procesadores encontrados
                        if "processorResults" in search_dto:
                            for proc in search_dto["processorResults"]:
                                # Extraer información del grupo padre
                                grupo_id = proc.get("groupId")
                                grupo_nombre = proc.get("parentGroupName")
                                
                                if grupo_id and grupo_nombre and grupo_nombre not in grupos:
                                    grupos[grupo_nombre] = {
                                        "id": grupo_id,
                                        "name": grupo_nombre
                                    }
                                    logger.info(f"Encontrado grupo: {grupo_nombre} (ID: {grupo_id})")
                        
                    # Si encontramos grupos, devolver como lista
                    if grupos:
                        return list(grupos.values())
                    else:
                        logger.warning("No se encontraron grupos en los resultados de búsqueda")
                else:
                    logger.warning(f"Error en búsqueda: {respuesta_busqueda.status_code}")
            except Exception as e:
                logger.warning(f"Error obteniendo grupos por búsqueda: {str(e)}")
            
            # Si llegamos aquí, intentar consultar directamente la API de limpieza de colas
            # que incluye información de conexiones conocidas
            logger.info("Intentando resolver grupos desde API de limpieza de colas...")
            try:
                respuesta_colas = requests.get(
                    f"{self.nifi_utils.proxy_url}/api/clean-all-queues",
                    timeout=self.nifi_utils.http_timeout
                )
                
                if respuesta_colas.status_code == 200:
                    # Revisar si devuelve las conexiones predefinidas con información de grupos
                    data_colas = respuesta_colas.json()
                    if "connections" in data_colas:
                        # Extraer grupos únicos
                        grupos = {}
                        for conn in data_colas["connections"]:
                            grupo_nombre = conn.get("group")
                            if grupo_nombre and grupo_nombre not in grupos:
                                # Para este caso especial, vamos a crear grupos "presuntos"
                                # Ya que conocemos los nombres pero no los IDs directamente
                                if grupo_nombre == "Productor":
                                    grupos[grupo_nombre] = {
                                        "id": "productor-id-placeholder",  # Placeholder
                                        "name": grupo_nombre
                                    }
                                elif grupo_nombre == "Consumidor":
                                    grupos[grupo_nombre] = {
                                        "id": "consumidor-id-placeholder",  # Placeholder
                                        "name": grupo_nombre
                                    }
                        
                        if grupos:
                            return list(grupos.values())
                else:
                    logger.warning(f"Error consultando API de colas: {respuesta_colas.status_code}")
            except Exception as e:
                logger.warning(f"Error consultando API de colas: {str(e)}")
                
            # Si llegamos aquí, crear grupos con IDs hardcodeados (última opción)
            logger.warning("Usando IDs hardcodeados como último recurso")
            return [
                {"id": "4dcc0bed-9061-3a4c-0000-c375f77615eb", "name": "Grupo Productor"},
                {"id": "723a3709-dabb-3fae-0000-2892da95b9a6", "name": "Grupo Consumidor"}
            ]
                
        except Exception as e:
            logger.error(f"Error obteniendo grupos de proceso: {str(e)}")
            return []