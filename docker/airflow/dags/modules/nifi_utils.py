"""
Utilidades para interactuar con Apache NiFi a través de API REST.
Este módulo contiene funciones para controlar procesadores, gestionar estado, 
configurar propiedades y monitorear el funcionamiento de NiFi.
"""
import time
import requests
import logging
from typing import Dict, List, Union, Optional, Any

# Configuración de logging
logger = logging.getLogger(__name__)

class NiFiConnector:
    """
    Clase para gestionar conexiones y operaciones con NiFi.
    Proporciona una interfaz unificada para interactuar con NiFi a través
    del mini-proxy o directamente.
    """
    
    def __init__(self, proxy_url: str, timeout: int = 60, 
                 producer_group_id: str = None, consumer_group_id: str = None):
        self.logger = logging.getLogger(__name__)
        """
        Inicializa el conector NiFi con las configuraciones básicas.
        
        Args:
            proxy_url: URL del proxy NiFi
            timeout: Tiempo de espera para peticiones HTTP
            producer_group_id: ID del grupo productor
            consumer_group_id: ID del grupo consumidor
        """

        self.proxy_url = proxy_url
        self.http_timeout = timeout
        self.producer_group_id = producer_group_id
        self.consumer_group_id = consumer_group_id
        self.processor_cache = {}
        
    def verificar_disponibilidad(self, max_intentos: int = 5) -> bool:
        """
        Verifica que el proxy NiFi esté disponible.
        
        Args:
            max_intentos: Número máximo de intentos de conexión
            
        Returns:
            bool: True si está disponible, False en caso contrario
        """
        for intento in range(max_intentos):
            try:
                logger.info(f"Verificando disponibilidad del proxy NiFi (intento {intento+1}/{max_intentos})...")
                
                # Usar el endpoint /health en lugar de /api/flow/about
                response = requests.get(
                    f"{self.proxy_url}/health", 
                    timeout=self.http_timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    # Verificar si el proxy reporta un estado saludable
                    if data.get("status") == "healthy" or data.get("auth_working"):
                        logger.info(f"✅ Proxy NiFi disponible y reporta estado saludable")
                        logger.info(f"Detalles: {data}")
                        return True
                    else:
                        logger.warning(f"⚠️ Proxy disponible pero reporta estado: {data.get('status')}")
                        # Verificar información de autenticación
                        if "auth_working" in data and data.get("auth_working"):
                            logger.info("✅ Autenticación funcionando correctamente")
                            return True
                else:
                    logger.warning(f"⚠️ NiFi respondió con código {response.status_code} (intento {intento+1})")
                    
            except requests.exceptions.ConnectionError as e:
                logger.warning(f"⚠️ Error de conexión al proxy (intento {intento+1}): {str(e)}")
                
            except requests.exceptions.Timeout as e:
                logger.warning(f"⚠️ Timeout al conectar con el proxy (intento {intento+1}): {str(e)}")
                
            except Exception as e:
                logger.error(f"❌ Error al verificar proxy (intento {intento+1}): {str(e)}")

            # Esperar antes de reintentar
            if intento < max_intentos - 1:
                logger.info("Esperando 5 segundos antes de reintentar...")
                time.sleep(5)
                    
        logger.error("❌ NiFi no disponible después de múltiples intentos")
        return False

    def buscar_procesadores_por_tipo(self, tipo_procesador: str, grupo_id: str) -> List[Dict]:
        """
        Busca procesadores por tipo dentro de un grupo específico.
        
        Args:
            tipo_procesador: Tipo de procesador a buscar (ej: 'QueryDatabaseTable')
            grupo_id: ID del grupo de procesadores
            
        Returns:
            List[Dict]: Lista de procesadores encontrados
        """
        procesadores = []
        logger.info(f"Buscando procesadores de tipo '{tipo_procesador}' en grupo {grupo_id}")
        
        try:
            # ESTRATEGIA 1: Buscar por nombre siguiendo el patrón de los procesadores
            # Usar la información de PRODUCER_PROCESSORS y CONSUMER_PROCESSORS de potencia_config.py
            from configs.potencia_config import PRODUCER_PROCESSORS, CONSUMER_PROCESSORS
            
            # Determinar si es grupo productor o consumidor
            is_producer = grupo_id == self.producer_group_id
            
            # Buscar el patrón de nombre para este tipo de procesador
            processor_pattern = None
            
            if is_producer:
                for proc in PRODUCER_PROCESSORS:
                    if proc["type"] == tipo_procesador:
                        processor_pattern = proc["name"]
                        break
            else:
                for proc in CONSUMER_PROCESSORS:
                    if proc["type"] == tipo_procesador:
                        processor_pattern = proc["name"]
                        break
            
            if processor_pattern:
                logger.info(f"Buscando procesador con patrón de nombre: {processor_pattern}")
                
                # Buscar por el patrón específico
                response = requests.get(
                    f"{self.proxy_url}/api/flow/search-results?q={processor_pattern}",
                    timeout=self.http_timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                        for proc in data["searchResultsDTO"]["processorResults"]:
                            if proc.get("groupId") == grupo_id:
                                nuevo_proc = {
                                    "id": proc.get("id"),
                                    "name": proc.get("name", ""),
                                    "type": tipo_procesador
                                }
                                procesadores.append(nuevo_proc)
                                logger.info(f"Encontrado por patrón de nombre: {nuevo_proc['name']} ({nuevo_proc['id']})")
            
            # ESTRATEGIA 2: Búsqueda por tipo/clase de procesador
            if not procesadores:
                response = requests.get(
                    f"{self.proxy_url}/api/flow/search-results?q={tipo_procesador}",
                    timeout=self.http_timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                        for proc in data["searchResultsDTO"]["processorResults"]:
                            if proc.get("groupId") == grupo_id:
                                nuevo_proc = {
                                    "id": proc.get("id"),
                                    "name": proc.get("name", ""),
                                    "type": tipo_procesador
                                }
                                procesadores.append(nuevo_proc)
                                logger.info(f"Encontrado por tipo: {nuevo_proc['name']} ({nuevo_proc['id']})")
            
            # ESTRATEGIA 3: Buscar por prefijo (P o C) y posición (1-5)
            if not procesadores:
                prefijo = "P" if is_producer else "C"
                
                # Obtener la posición (order) para este tipo de procesador
                orden = 0
                if is_producer:
                    for proc in PRODUCER_PROCESSORS:
                        if proc["type"] == tipo_procesador:
                            orden = proc["order"]
                            break
                else:
                    for proc in CONSUMER_PROCESSORS:
                        if proc["type"] == tipo_procesador:
                            orden = proc["order"]
                            break
                
                if orden > 0:
                    patron_busqueda = f"{prefijo}.{orden}"
                    
                    logger.info(f"Buscando por patrón de orden: {patron_busqueda}")
                    
                    response = requests.get(
                        f"{self.proxy_url}/api/flow/search-results?q={patron_busqueda}",
                        timeout=self.http_timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                            for proc in data["searchResultsDTO"]["processorResults"]:
                                if proc.get("groupId") == grupo_id:
                                    nuevo_proc = {
                                        "id": proc.get("id"),
                                        "name": proc.get("name", ""),
                                        "type": tipo_procesador
                                    }
                                    procesadores.append(nuevo_proc)
                                    logger.info(f"Encontrado por orden: {nuevo_proc['name']} ({nuevo_proc['id']})")
            
            # ESTRATEGIA 4: Listar todos los procesadores del grupo y filtrar por tipo o palabras clave
            if not procesadores:
                try:
                    logger.info(f"Listando todos los procesadores del grupo {grupo_id}")
                    
                    response = requests.get(
                        f"{self.proxy_url}/api/process-groups/{grupo_id}/processors",
                        timeout=self.http_timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        if "processors" in data:
                            # Palabras clave asociadas a cada tipo de procesador
                            keywords = {
                                "QueryDatabaseTable": ["query", "database", "table", "sql"],
                                "ConvertRecord": ["convert", "record"],
                                "SplitJson": ["split", "json"],
                                "MergeContent": ["merge", "content"],
                                "PublishKafka": ["publish", "kafka"],
                                "ConsumeKafka": ["consume", "kafka"],
                                "EvaluateJsonPath": ["evaluate", "json", "path"],
                                "ReplaceText": ["replace", "text"],
                                "PutDatabaseRecord": ["put", "database", "record"]
                            }
                            
                            keywords_for_type = keywords.get(tipo_procesador, [tipo_procesador.lower()])
                            
                            for proc in data["processors"]:
                                comp = proc.get("component", {})
                                proc_type = comp.get("type", "")
                                proc_name = comp.get("name", "").lower()
                                
                                # Verificar si el tipo contiene la cadena buscada o si el nombre contiene palabras clave
                                if (tipo_procesador.lower() in proc_type.lower() or 
                                    any(keyword in proc_name for keyword in keywords_for_type)):
                                    
                                    nuevo_proc = {
                                        "id": proc.get("id"),
                                        "name": comp.get("name", ""),
                                        "type": tipo_procesador
                                    }
                                    procesadores.append(nuevo_proc)
                                    logger.info(f"Encontrado por listado: {nuevo_proc['name']} ({nuevo_proc['id']})")
                except Exception as e:
                    logger.warning(f"Error listando procesadores: {str(e)}")
            
            if procesadores:
                logger.info(f"Total de procesadores encontrados de tipo '{tipo_procesador}': {len(procesadores)}")
            else:
                logger.warning(f"No se encontraron procesadores de tipo '{tipo_procesador}' en el grupo {grupo_id}")
                
            return procesadores
        
        except Exception as e:
            logger.error(f"Error en buscar_procesadores_por_tipo: {str(e)}")
            return []
    def obtener_estado_procesador(self, processor_id):
        """
        Obtiene el estado y configuración de un procesador por su ID
        
        Args:
            processor_id: ID del procesador
            
        Returns:
            dict: Datos del procesador o None si no se pudo obtener
        """
        import requests
        
        logger.info(f"Obteniendo estado del procesador {processor_id}")
        
        # Intentar varios endpoints posibles
        urls_to_try = [
            f"{self.proxy_url}/nifi-api/processors/{processor_id}",
            f"{self.proxy_url}/api/processors/{processor_id}"
        ]
        
        for url in urls_to_try:
            try:
                response = requests.get(
                    url,
                    timeout=self.http_timeout
                )
                
                if response.status_code == 200:
                    logger.info(f"Estado obtenido correctamente usando {url}")
                    return response.json()
                else:
                    logger.warning(f"Error obteniendo estado con {url}: {response.status_code}")
                    
            except Exception as e:
                logger.warning(f"Excepción al obtener estado con {url}: {str(e)}")
        
        logger.error(f"No se pudo obtener estado del procesador {processor_id}")
        return None
    def get_processor_status(self, processor_id: str) -> Dict:
        """
        Obtiene el estado de un procesador.
        
        Args:
            processor_id: ID del procesador
            
        Returns:
            Dict: Estado del procesador o diccionario vacío si hay error
        """
        try:
            response = requests.get(
                f"{self.proxy_url}/api/processors/{processor_id}",
                timeout=self.http_timeout
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                logger.warning(f"Error obteniendo estado del procesador {processor_id}: {response.status_code}")
                return {}
                
        except Exception as e:
            logger.error(f"Error en get_processor_status: {str(e)}")
            return {}

    def stop_processor(self, processor_id: str) -> str:
        """
        Detiene un procesador.
        
        Args:
            processor_id: ID del procesador
            
        Returns:
            str: Resultado de la operación ('SUCCESS', 'ERROR')
        """
        try:
            # Primero obtenemos el estado actual y revision
            processor_info = self.get_processor_status(processor_id)
            if not processor_info:
                return "ERROR"
                
            # Si ya está detenido, retornamos éxito
            if processor_info.get("component", {}).get("state") == "STOPPED":
                return "SUCCESS"
                
            # Preparamos el payload para detener el procesador
            revision = processor_info.get("revision", {})
            stop_payload = {
                "revision": revision,
                "component": {
                    "id": processor_id,
                    "state": "STOPPED"
                }
            }
            
            # Ejecutamos la petición
            response = requests.put(
                f"{self.proxy_url}/api/processors/{processor_id}/run-status",
                json=stop_payload,
                timeout=self.http_timeout
            )
            
            if response.status_code in [200, 202]:
                return "SUCCESS"
            else:
                logger.warning(f"Error deteniendo procesador {processor_id}: {response.status_code}")
                return "ERROR"
                
        except Exception as e:
            logger.error(f"Error en stop_processor: {str(e)}")
            return "ERROR"
        
    def start_processor(self, processor_id: str, processor_name: str = None, max_retries: int = 3) -> str:
        """
        Inicia un procesador con reintentos en caso de fallo.
        
        Args:
            processor_id: ID del procesador
            processor_name: Nombre del procesador (para logs)
            max_retries: Número máximo de reintentos
            
        Returns:
            str: Resultado de la operación ('SUCCESS', 'ERROR')
        """
        nombre = processor_name or processor_id
        
        for intento in range(max_retries):
            try:
                # Primero obtenemos el estado actual y revision
                processor_info = self.get_processor_status(processor_id)
                if not processor_info:
                    logger.warning(f"No se pudo obtener información del procesador {nombre}")
                    time.sleep(2)
                    continue
                    
                # Si ya está en ejecución, retornamos éxito
                if processor_info.get("component", {}).get("state") == "RUNNING":
                    logger.info(f"El procesador {nombre} ya está en ejecución")
                    return "SUCCESS"
                    
                # Preparamos el payload para iniciar el procesador
                revision = processor_info.get("revision", {})
                start_payload = {
                    "revision": revision,
                    "component": {
                        "id": processor_id,
                        "state": "RUNNING"
                    }
                }
                
                # Ejecutamos la petición
                response = requests.put(
                    f"{self.proxy_url}/api/processors/{processor_id}/run-status",
                    json=start_payload,
                    timeout=self.http_timeout
                )
                
                if response.status_code in [200, 202]:
                    logger.info(f"✅ Procesador {nombre} iniciado correctamente")
                    return "SUCCESS"
                else:
                    logger.warning(f"Error iniciando procesador {nombre}: {response.status_code}")
                    
            except Exception as e:
                logger.error(f"Error en start_processor ({nombre}): {str(e)}")
                
            # Esperar antes de reintentar
            if intento < max_retries - 1:
                time.sleep(2)
        
        logger.error(f"❌ No se pudo iniciar el procesador {nombre} después de {max_retries} intentos")
        return "ERROR"
        
    def iniciar_grupo(self, group_id):
        """
        Inicia todos los procesadores en un grupo
        
        Args:
            group_id: ID del grupo de procesadores
            
        Returns:
            bool: True si se inició correctamente, False en caso contrario
        """
        try:
            # Primero iniciar el grupo entero
            response = requests.put(
                f"{self.proxy_url}/nifi-api/flow/process-groups/{group_id}",  # Cambiar /api/ por /nifi-api/
                json={"id": group_id, "state": "RUNNING", "disconnectedNodeAcknowledged": True},
                timeout=self.http_timeout
            )
            
            # Si no se puede iniciar el grupo completo, intentar iniciar procesadores individuales
            if response.status_code not in (200, 202):
                logging.warning(f"No se pudo iniciar el grupo {group_id} completo")
                
                # Listar procesadores del grupo
                processors_resp = requests.get(
                    f"{self.proxy_url}/nifi-api/process-groups/{group_id}/processors",  # Cambiar la URL
                    timeout=self.http_timeout
                )
                
                if processors_resp.status_code != 200:
                    logging.error(f"No se pudo obtener lista de procesadores del grupo {group_id}")
                    return False
                    
                processors = processors_resp.json().get("processors", [])
                
                # Iniciar cada procesador individualmente
                success = True
                success_count = 0
                for proc in processors:
                    proc_id = proc.get("id")
                    if not proc_id:
                        continue
                        
                    start_resp = requests.put(
                        f"{self.proxy_url}/nifi-api/processors/{proc_id}/run-status",  # Cambiar la URL
                        json={"state": "RUNNING", "disconnectedNodeAcknowledged": True},
                        timeout=self.http_timeout
                    )
                    
                    if start_resp.status_code in (200, 202):
                        success_count += 1
                    else:
                        logging.warning(f"No se pudo iniciar el procesador {proc_id}")
                        success = False
                
                logging.info(f"Iniciados {success_count}/{len(processors)} procesadores en el grupo {group_id}")
                return success_count > 0
            
            logging.info(f"Grupo {group_id} iniciado correctamente")
            return True
            
        except Exception as e:
            logging.error(f"Error iniciando grupo {group_id}: {str(e)}")
            return False

    def actualizar_propiedades_procesador(self, processor_id, properties):
        """
        Actualiza las propiedades de un procesador
        
        Args:
            processor_id: ID del procesador
            properties: Diccionario con propiedades a actualizar
            
        Returns:
            bool: True si la actualización fue exitosa, False en caso contrario
        """
        try:
            # Primero obtener el estado actual
            response = requests.get(
                f"{self.proxy_url}/api/processors/{processor_id}",
                timeout=self.http_timeout
            )
            
            if response.status_code != 200:
                return False
                
            processor_data = response.json()
            
            # Detener el procesador si está en ejecución
            current_state = processor_data.get("component", {}).get("state", "")
            if current_state == "RUNNING":
                stop_resp = requests.put(
                    f"{self.proxy_url}/api/processors/{processor_id}/run-status",
                    json={"state": "STOPPED", "disconnectedNodeAcknowledged": True},
                    timeout=self.http_timeout
                )
                if stop_resp.status_code not in (200, 202):
                    return False
            
            # Actualizar las propiedades
            component = processor_data.get("component", {})
            config = component.get("config", {})
            
            # Asegurarse que existe la sección de properties
            if "properties" not in config:
                config["properties"] = {}
                
            # Actualizar las propiedades especificadas
            for key, value in properties.items():
                config["properties"][key] = value
                
            # Crear el payload para la actualización
            processor_data["component"]["config"] = config
            
            # Enviar la actualización
            update_resp = requests.put(
                f"{self.proxy_url}/api/processors/{processor_id}",
                json=processor_data,
                timeout=self.http_timeout
            )
            
            return update_resp.status_code in (200, 202)
            
        except Exception as e:
            self.logger.error(f"Error actualizando propiedades: {str(e)}")
            return False
            
    def detener_grupo(self, group_id):
        """
        Detiene todos los procesadores en un grupo
        
        Args:
            group_id: ID del grupo de procesadores
            
        Returns:
            bool: True si se detuvo correctamente, False en caso contrario
        """
        try:
            # Primero detener el grupo entero
            response = requests.put(
                f"{self.proxy_url}/api/flow/process-groups/{group_id}",
                json={"id": group_id, "state": "STOPPED", "disconnectedNodeAcknowledged": True},
                timeout=self.http_timeout
            )
            
            # Si no se puede detener el grupo completo, intentar detener procesadores individuales
            if response.status_code not in (200, 202):
                self.logger.warning(f"No se pudo detener el grupo {group_id} completo")
                
                # Listar procesadores del grupo
                processors_resp = requests.get(
                    f"{self.proxy_url}/api/flow/process-groups/{group_id}/processors",
                    timeout=self.http_timeout
                )
                
                if processors_resp.status_code != 200:
                    return False
                    
                processors = processors_resp.json().get("processors", [])
                
                # Detener cada procesador individualmente
                success = True
                for proc in processors:
                    proc_id = proc.get("id")
                    if not proc_id:
                        continue
                        
                    stop_resp = requests.put(
                        f"{self.proxy_url}/api/processors/{proc_id}/run-status",
                        json={"state": "STOPPED", "disconnectedNodeAcknowledged": True},
                        timeout=self.http_timeout
                    )
                    
                    if stop_resp.status_code not in (200, 202):
                        self.logger.warning(f"No se pudo detener el procesador {proc_id}")
                        success = False
                
                return success
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error deteniendo grupo {group_id}: {str(e)}")
            return False

    def update_query_processor(self, processor_id: str, sql_query: str) -> bool:
        """
        Actualiza la consulta SQL en un procesador QueryDatabaseTable.
        
        Args:
            processor_id: ID del procesador
            sql_query: Consulta SQL a configurar
            
        Returns:
            bool: True si se actualizó correctamente, False en caso contrario
        """
        try:
            # Primero obtenemos el estado actual y revision
            processor_info = self.get_processor_status(processor_id)
            if not processor_info:
                return False
                
            # Preparamos el payload para actualizar la consulta
            revision = processor_info.get("revision", {})
            component = processor_info.get("component", {})
            
            # Modificar la propiedad SQL query
            if "properties" not in component:
                component["properties"] = {}
                
            component["properties"]["sql-query"] = sql_query
            
            # Creamos el payload completo
            update_payload = {
                "revision": revision,
                "component": component
            }
            
            # Ejecutamos la petición
            response = requests.put(
                f"{self.proxy_url}/api/processors/{processor_id}",
                json=update_payload,
                timeout=self.http_timeout
            )
            
            if response.status_code in [200, 202]:
                logger.info(f"✅ Consulta SQL actualizada correctamente")
                return True
            else:
                logger.warning(f"Error actualizando consulta SQL: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error en update_query_processor: {str(e)}")
            return False
            
    def limpiar_estado_procesador(self, processor_id: str) -> bool:
        """
        Limpia el estado de un procesador (útil para QueryDatabaseTable y ConsumeKafka).
        
        Args:
            processor_id: ID del procesador
            
        Returns:
            bool: True si se limpió correctamente, False en caso contrario
        """
        try:
            # Intentamos obtener el estado del procesador
            response = requests.get(
                f"{self.proxy_url}/api/processors/{processor_id}/state",
                timeout=self.http_timeout
            )
            
            if response.status_code != 200:
                logger.warning(f"Error obteniendo estado local del procesador: {response.status_code}")
                return False
                
            # Realizamos una petición para limpiar el estado
            clear_response = requests.post(
                f"{self.proxy_url}/api/processors/{processor_id}/state/clear-requests",
                json={},
                timeout=self.http_timeout
            )
            
            if clear_response.status_code in [200, 202]:
                logger.info(f"✅ Estado del procesador limpiado correctamente")
                return True
            else:
                logger.warning(f"Error limpiando estado del procesador: {clear_response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error en limpiar_estado_procesador: {str(e)}")
            return False
            
    def buscar_procesadores(self) -> Dict:
        """
        Busca los procesadores necesarios para el flujo de potencia.
        
        Returns:
            Dict: Diccionario con los procesadores encontrados
        """
        result = {
            "productor": {},
            "consumidor": {}
        }
        
        # Primero, verificar los IDs de grupo y buscar los correctos si es necesario
        if not self._verificar_y_actualizar_grupos():
            logger.error("No se pudieron encontrar los grupos de procesadores correctos")
            return result
        
        # Tipos de procesadores a buscar
        tipos_productor = ["QueryDatabaseTable", "ConvertRecord", "SplitJson", "MergeContent", "PublishKafka"]
        tipos_consumidor = ["ConsumeKafka", "EvaluateJsonPath", "ReplaceText", "PutDatabaseRecord"]
        
        # Buscar procesadores del grupo productor
        logger.info(f"Buscando procesadores en grupo productor (ID: {self.nifi.producer_group_id})")
        for tipo in tipos_productor:
            procesadores = self.nifi.buscar_procesadores_por_tipo(tipo, self.nifi.producer_group_id)
            if procesadores:
                result["productor"][tipo] = procesadores[0]  # Tomamos el primer procesador encontrado
                logger.info(f"✅ Encontrado procesador {tipo}: {procesadores[0]['name']} ({procesadores[0]['id']})")
            else:
                logger.warning(f"No se encontró procesador de tipo {tipo} en el grupo productor")
        
        # Buscar procesadores del grupo consumidor
        logger.info(f"Buscando procesadores en grupo consumidor (ID: {self.nifi.consumer_group_id})")
        for tipo in tipos_consumidor:
            procesadores = self.nifi.buscar_procesadores_por_tipo(tipo, self.nifi.consumer_group_id)
            if procesadores:
                result["consumidor"][tipo] = procesadores[0]  # Tomamos el primer procesador encontrado
                logger.info(f"✅ Encontrado procesador {tipo}: {procesadores[0]['name']} ({procesadores[0]['id']})")
            else:
                logger.warning(f"No se encontró procesador de tipo {tipo} en el grupo consumidor")
                
        return result

    def _verificar_y_actualizar_grupos(self):
        """
        Verifica que los IDs de grupo son correctos, y los actualiza si es necesario.
        
        Returns:
            bool: True si los grupos se encontraron, False si hay error
        """
        try:
            # Intentar obtener todos los grupos de procesos
            response = requests.get(
                f"{self.nifi.proxy_url}/api/flow/process-groups/root",
                timeout=self.nifi.http_timeout
            )
            
            if response.status_code != 200:
                logger.error(f"Error obteniendo grupos de procesos: {response.status_code}")
                return False
            
            data = response.json()
            
            # Buscar grupos con nombres "Producer" y "Consumer"
            if "processGroupFlow" in data and "flow" in data["processGroupFlow"] and "processGroups" in data["processGroupFlow"]["flow"]:
                process_groups = data["processGroupFlow"]["flow"]["processGroups"]
                
                producer_group = None
                consumer_group = None
                
                # Primero buscar por ID configurado
                for group in process_groups:
                    if group.get("id") == self.nifi.producer_group_id:
                        producer_group = group
                        logger.info(f"✅ Grupo productor encontrado por ID: {group.get('name')} ({group.get('id')})")
                    
                    if group.get("id") == self.nifi.consumer_group_id:
                        consumer_group = group
                        logger.info(f"✅ Grupo consumidor encontrado por ID: {group.get('name')} ({group.get('id')})")
                
                # Si no se encuentran por ID, buscar por nombre o palabras clave
                if not producer_group or not consumer_group:
                    logger.warning("Buscando grupos por nombre en lugar de ID...")
                    
                    for group in process_groups:
                        name = group.get("component", {}).get("name", "").lower()
                        
                        # Buscar palabras clave relacionadas con productores
                        if not producer_group and ("productor" in name or "producer" in name or "publishers" in name):
                            producer_group = group
                            logger.info(f"✅ Grupo productor encontrado por nombre: {name} ({group.get('id')})")
                        
                        # Buscar palabras clave relacionadas con consumidores
                        if not consumer_group and ("consumidor" in name or "consumer" in name or "subscribers" in name):
                            consumer_group = group
                            logger.info(f"✅ Grupo consumidor encontrado por nombre: {name} ({group.get('id')})")
                
                # Actualizar IDs si se encontraron
                if producer_group:
                    self.nifi.producer_group_id = producer_group.get("id")
                
                if consumer_group:
                    self.nifi.consumer_group_id = consumer_group.get("id")
                
                # Si se encontraron ambos grupos, retornar True
                if producer_group and consumer_group:
                    return True
                else:
                    logger.error("No se pudieron encontrar ambos grupos de procesamiento")
                    return False
            else:
                logger.error("La respuesta no tiene la estructura esperada")
                return False
                
        except Exception as e:
            logger.error(f"Error verificando grupos: {str(e)}")
            return False   

    def configurar_tabla_destino(self, processor_id: str, tabla_destino: str) -> bool:
        """
        Configura la tabla de destino en un procesador PutDatabaseRecord.
        
        Args:
            processor_id: ID del procesador
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            bool: True si se configuró correctamente, False en caso contrario
        """
        try:
            # Primero obtenemos el estado actual y revision
            processor_info = self.get_processor_status(processor_id)
            if not processor_info:
                return False
                
            # Preparamos el payload para actualizar la propiedad
            revision = processor_info.get("revision", {})
            component = processor_info.get("component", {})
            
            # Modificar la propiedad de tabla
            if "properties" not in component:
                component["properties"] = {}
                
            component["properties"]["table-name"] = tabla_destino
            
            # Creamos el payload completo
            update_payload = {
                "revision": revision,
                "component": component
            }
            
            # Ejecutamos la petición
            response = requests.put(
                f"{self.proxy_url}/api/processors/{processor_id}",
                json=update_payload,
                timeout=self.http_timeout
            )
            
            if response.status_code in [200, 202]:
                logger.info(f"✅ Tabla destino actualizada correctamente a: {tabla_destino}")
                return True
            else:
                logger.warning(f"Error actualizando tabla destino: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error en configurar_tabla_destino: {str(e)}")
            return False
            
    def reparar_put_database_record(self, processor_id: str, processor_name: str = None) -> bool:
        """
        Configura y repara un procesador PutDatabaseRecord para asegurar su funcionamiento.
        
        Args:
            processor_id: ID del procesador
            processor_name: Nombre del procesador (para logs)
            
        Returns:
            bool: True si se reparó correctamente, False en caso contrario
        """
        try:
            nombre = processor_name or processor_id
            logger.info(f"Reparando procesador PutDatabaseRecord: {nombre}")
            
            # Primero obtenemos el estado actual y revision
            processor_info = self.get_processor_status(processor_id)
            if not processor_info:
                logger.error(f"No se pudo obtener información del procesador {nombre}")
                return False
                
            revision = processor_info.get("revision", {})
            component = processor_info.get("component", {})
            
            # Aplicar configuraciones recomendadas
            if "properties" not in component:
                component["properties"] = {}
                
            # Configuraciones clave para evitar problemas comunes
            # 1. Batch Size - no demasiado grande
            component["properties"]["Batch Size"] = "100"
            
            # 2. Handling Strategy - adecuado para el contexto
            component["properties"]["Statement Type"] = "INSERT"
            
            # 3. Connection Pool - usar el controlador correcto
            component["properties"]["Maximum-value Columns"] = ""
            
            # Creamos el payload completo
            update_payload = {
                "revision": revision,
                "component": component
            }
            
            # Ejecutamos la petición
            response = requests.put(
                f"{self.proxy_url}/api/processors/{processor_id}",
                json=update_payload,
                timeout=self.http_timeout
            )
            
            if response.status_code in [200, 202]:
                logger.info(f"✅ Procesador {nombre} reparado correctamente")
                return True
            else:
                logger.warning(f"Error reparando procesador {nombre}: {response.status_code}")
                return False
                
        except Exception as e:
            logger.error(f"Error en reparar_put_database_record: {str(e)}")
            return False

# Función para crear instancia fácilmente
def crear_conector_nifi(proxy_url="http://mini_nifi_proxy_centrosur:5001", timeout=60):
    """
    Crea y devuelve una instancia del conector NiFi.
    
    Args:
        proxy_url: URL del proxy NiFi
        timeout: Tiempo de espera para peticiones HTTP
        
    Returns:
        NiFiConnector: Instancia del conector o None si no está disponible
    """
    try:
        conector = NiFiConnector(proxy_url=proxy_url, timeout=timeout)
        if conector.verificar_disponibilidad():
            return conector
        return None
    except Exception as e:
        logger.error(f"Error creando conector NiFi: {str(e)}")
        return None