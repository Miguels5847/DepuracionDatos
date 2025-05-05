import requests
import time
import json
import logging

class GestorNiFi:
    """
    Clase para gestionar la interacción con NiFi de manera dinámica,
    resolviendo IDs de procesadores por nombre y agrupándolos por función.
    """
    
    def __init__(self, proxy_url="http://mini_nifi_proxy_centrosur:5001", timeout=10):
        self.proxy_url = proxy_url
        self.timeout = timeout
        self.logger = logging.getLogger(__name__)
        
        # Definir IDs de grupos conocidos (valores por defecto que se actualizarán)
        self.producer_group_id = "4dcc0bed-9061-3a4c-0000-c375f77615eb"  # Grupo productor
        self.consumer_group_id = "723a3709-dabb-3fae-0000-2892da95b9a6"  # Grupo consumidor
        
        # Mapeo de nombres de procesadores a sus IDs (se llenarán dinámicamente)
        self.processor_ids = {}
        
        # Mapeo de nombres amigables a posibles nombres reales de procesadores
        # Añadimos variantes de nombres para mayor flexibilidad
        self.processor_mapping = {
            # Grupo productor
            "query_db_table": ["QueryDatabaseTable", "P.1-QueryDatabaseTable", "QueryDB"],
            "convert_record": ["ConvertRecord", "P.2-ConvertRecord"], 
            "split_json": ["SplitJson", "P.3-SplitJson"],
            "merge_content": ["MergeContent", "P.4-MergeContent"],
            "publish_kafka": ["PublishKafka", "PublishKafkaRecord", "P5-PublishKafka"],
            "put_database_record": ["PutDatabaseRecord", "P.6-PutDatabaseRecord"],
            
            # Grupo consumidor
            "consume_kafka": ["ConsumeKafka", "ConsumeKafkaRecord", "C.1-ConsumeKafka"],
            "evaluate_json_path": ["EvaluateJsonPath", "C.2-EvaluateJsonPath"],
            "replace_text": ["ReplaceText", "C.3-ReplaceText"],
            "put_db": ["PutDatabaseRecord", "C.4-PutDatabaseRecord"]
        }
        
        # Asegurar que todos los atributos necesarios existan, aunque sea con valores por defecto
        self._init_default_processor_ids()
        
        # Verificar disponibilidad y resolver IDs
        self.verificar_disponibilidad()
        self.resolver_ids_procesadores()
    
    def _init_default_processor_ids(self):
        """Inicializa atributos con valores por defecto para evitar AttributeError"""
        # Valores por defecto (se reemplazarán si se encuentran los procesadores)
        defaults = {
            "query_db_table_processor_id": "a6ee2f4e-edbc-38f9-a152-b0a19044d4a9",
            "convert_record_processor_id": "47082c97-43ab-3005-31b7-3a45868f9d4f",
            "split_json_processor_id": "610f8ec8-efbb-393a-67d7-2e1f6d72cdd2",
            "merge_content_processor_id": "c78203d6-ff06-3657-6bc9-e78f115b5426",
            "publish_kafka_processor_id": "5f56daf3-3075-3d6a-53b0-4f1106a9e790",
            "put_database_record_processor_id": "10d89865-0196-1000-3eaf-c624cd671d97",
            "consume_kafka_processor_id": "e70f09af-1320-30d9-d640-9b0cb7e39f8a",
            "evaluate_json_path_processor_id": "ef0e8535-8d18-3ee2-e0a7-38281ba8fe2a",
            "replace_text_processor_id": "398ab46d-50e6-3101-c4dc-fd605138661c",
            "put_db_processor_id": "db428504-4b50-3703-22eb-367ccbf3b295"
        }
        
        # Asignar valores por defecto para evitar AttributeError
        for attr_name, default_value in defaults.items():
            setattr(self, attr_name, default_value)
    
    def verificar_disponibilidad(self):
        """Verifica la disponibilidad del proxy NiFi."""
        max_intentos = 5
        for intento in range(1, max_intentos + 1):
            self.logger.info(f"Verificando disponibilidad del proxy NiFi (intento {intento}/{max_intentos})...")
            try:
                response = requests.get(f"{self.proxy_url}/health", timeout=self.timeout)
                if response.status_code == 200:
                    datos = response.json()
                    if datos.get("status") == "healthy":
                        self.logger.info("✅ Proxy NiFi disponible y reporta estado saludable")
                        self.logger.info(f"Detalles: {datos}")
                        return True
                self.logger.warning(f"Proxy no disponible: {response.status_code}")
                time.sleep(2)
            except Exception as e:
                self.logger.warning(f"Error verificando disponibilidad: {str(e)}")
                time.sleep(2)
        
        self.logger.error("❌ No se pudo conectar al proxy NiFi después de múltiples intentos")
        return False
    
    def resolver_ids_procesadores(self):
        """Resuelve IDs de procesadores usando múltiples estrategias."""
        self.logger.info("Resolviendo IDs de procesadores por nombre...")
        
        # Primero resolver los grupos
        self.resolver_ids_grupos()
        
        # Intentar primero por tipos (más confiable)
        self.buscar_procesadores_por_tipo()
        
        # Si no se encontraron todos, intentar por nombre
        if len(self.processor_ids) < len(self.processor_mapping):
            # Luego resolver los procesadores con mejor manejo de múltiples nombres posibles
            for clave, nombres_posibles in self.processor_mapping.items():
                # Si ya tenemos este procesador, omitir
                if clave in self.processor_ids:
                    continue
                    
                # Convertir a lista si es string (para compatibilidad)
                if isinstance(nombres_posibles, str):
                    nombres_posibles = [nombres_posibles]
                
                # Intentar con cada nombre posible hasta encontrar uno que funcione
                processor_id = None
                for nombre in nombres_posibles:
                    self.logger.info(f"Buscando procesador: {nombre} (clave: {clave})")
                    processor_id = self.buscar_procesador_por_nombre(nombre)
                    if processor_id:
                        break
                
                if processor_id:
                    self.processor_ids[clave] = processor_id
                    self.logger.info(f"✅ ID resuelto para {clave}: {processor_id}")
                    
                    # Configurar atributos dinámicamente para acceso más fácil
                    attr_name = f"{clave}_processor_id"
                    setattr(self, attr_name, processor_id)
                    self.logger.info(f"Configurado atributo: {attr_name} = {processor_id}")
                else:
                    self.logger.warning(f"⚠️ No se pudo encontrar ID para {clave}")
                    
                    # Mantener los valores por defecto ya inicializados
                    # No hacer nada aquí, pues ya asignamos valores por defecto en _init_default_processor_ids()
        
        # Verificar procesadores que siguen faltando
        faltantes = [k for k in self.processor_mapping.keys() if k not in self.processor_ids]
        if faltantes:
            self.logger.warning(f"⚠️ No se pudieron encontrar {len(faltantes)} procesadores: {faltantes}")
        
        return self.processor_ids
    
    def buscar_procesador_por_tipo(self):
        """
        Busca procesadores por su tipo en lugar de por nombre.
        Esta es una alternativa cuando los nombres específicos no coinciden.
        """
        try:
            # Mapeo de claves a tipos de procesadores
            tipos_procesadores = {
                "query_db_table": "org.apache.nifi.processors.standard.QueryDatabaseTable",
                "convert_record": "org.apache.nifi.processors.standard.ConvertRecord",
                "split_json": "org.apache.nifi.processors.standard.SplitJson",
                "merge_content": "org.apache.nifi.processors.standard.MergeContent",
                "publish_kafka": "org.apache.nifi.processors.kafka.PublishKafkaRecord",
                "put_database_record": "org.apache.nifi.processors.standard.PutDatabaseRecord",
                "consume_kafka": "org.apache.nifi.processors.kafka.ConsumeKafkaRecord",
                "evaluate_json_path": "org.apache.nifi.processors.standard.EvaluateJsonPath",
                "replace_text": "org.apache.nifi.processors.standard.ReplaceText"
            }
            
            resultados = {}
            
            # Para cada tipo, intenta encontrar un procesador correspondiente
            for clave, tipo in tipos_procesadores.items():
                # Este endpoint no existe directamente, pero podemos usar search para aproximarnos
                response = requests.get(
                    f"{self.proxy_url}/nifi-api/flow/search-results?q={tipo.split('.')[-1]}",
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                        for proc in data["searchResultsDTO"]["processorResults"]:
                            proc_name = proc.get("name", "")
                            proc_id = proc.get("id")
                            
                            if proc_id:
                                self.logger.info(f"Encontrado procesador por tipo {tipo}: {proc_name} -> {proc_id}")
                                resultados[clave] = proc_id
                                break
            
            return resultados
                        
        except Exception as e:
            self.logger.error(f"Error buscando procesadores por tipo: {str(e)}")
            return {}

    def resolver_ids_grupos(self):
        """
        Resuelve IDs de grupos de procesadores buscando en todos los grupos disponibles.
        Busca grupos que podrían contener procesadores productores o consumidores.
        """
        try:
            self.logger.info("Resolviendo IDs de grupos de procesadores...")
            
            # Obtener todos los grupos desde el nivel raíz
            response = requests.get(
                f"{self.proxy_url}/nifi-api/flow/process-groups/root",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Error obteniendo grupos: {response.status_code}")
                return
                
            data = response.json()
            
            # Buscar grupos que contengan palabras clave
            grupos_encontrados = []
            if "processGroupFlow" in data and "flow" in data["processGroupFlow"] and "processGroups" in data["processGroupFlow"]["flow"]:
                grupos = data["processGroupFlow"]["flow"]["processGroups"]
                
                for grupo in grupos:
                    nombre = grupo.get("component", {}).get("name", "").lower()
                    grupo_id = grupo.get("component", {}).get("id")
                    
                    # Distinguir entre grupos productores y consumidores
                    if "producer" in nombre or "productor" in nombre or "extract" in nombre or "query" in nombre:
                        self.logger.info(f"✅ Encontrado grupo productor: {nombre} -> {grupo_id}")
                        self.producer_group_id = grupo_id
                        grupos_encontrados.append(("producer", nombre, grupo_id))
                        
                    if "consumer" in nombre or "consumidor" in nombre or "process" in nombre or "insert" in nombre:
                        self.logger.info(f"✅ Encontrado grupo consumidor: {nombre} -> {grupo_id}")
                        self.consumer_group_id = grupo_id
                        grupos_encontrados.append(("consumer", nombre, grupo_id))
                        
                if grupos_encontrados:
                    self.logger.info(f"✅ Encontrados {len(grupos_encontrados)} grupos relevantes")
                else:
                    # Si no encontramos por palabras clave, tomar los dos primeros grupos
                    if len(grupos) >= 2:
                        self.logger.info("⚠️ No se encontraron grupos con nombres específicos, usando los dos primeros por defecto")
                        self.producer_group_id = grupos[0].get("component", {}).get("id")
                        self.consumer_group_id = grupos[1].get("component", {}).get("id")
                        
                        self.logger.info(f"Grupo productor (presumido): {grupos[0].get('component', {}).get('name')} -> {self.producer_group_id}")
                        self.logger.info(f"Grupo consumidor (presumido): {grupos[1].get('component', {}).get('name')} -> {self.consumer_group_id}")
                        
                    elif len(grupos) == 1:
                        # Si solo hay un grupo, usarlo para ambos
                        self.logger.info("⚠️ Solo se encontró un grupo, usándolo tanto para productor como consumidor")
                        self.producer_group_id = self.consumer_group_id = grupos[0].get("component", {}).get("id")
                    else:
                        self.logger.warning("❌ No se encontraron grupos de procesadores")
        
        except Exception as e:
            self.logger.error(f"Error resolviendo IDs de grupos: {str(e)}")
    
    def buscar_procesadores_por_tipo(self):
        """
        Busca procesadores por su tipo en ambos grupos (productor y consumidor).
        Esto es más efectivo que buscar por nombre cuando los nombres no coinciden.
        """
        try:
            self.logger.info("Buscando procesadores por tipo...")
            
            # Mapeo de tipos a claves de procesadores
            tipos_procesadores = {
                "org.apache.nifi.processors.standard.QueryDatabaseTable": "query_db_table",
                "org.apache.nifi.processors.standard.ConvertRecord": "convert_record", 
                "org.apache.nifi.processors.standard.SplitJson": "split_json",
                "org.apache.nifi.processors.standard.MergeContent": "merge_content",
                "org.apache.nifi.processors.kafka.PublishKafkaRecord": "publish_kafka",
                "org.apache.nifi.processors.standard.PutDatabaseRecord": "put_database_record",
                "org.apache.nifi.processors.kafka.ConsumeKafkaRecord": "consume_kafka",
                "org.apache.nifi.processors.standard.EvaluateJsonPath": "evaluate_json_path",
                "org.apache.nifi.processors.standard.ReplaceText": "replace_text"
            }
            
            # Obtener todos los procesadores de ambos grupos
            for grupo_id, grupo_nombre in [
                (self.producer_group_id, "Productor"), 
                (self.consumer_group_id, "Consumidor")
            ]:
                if not grupo_id:
                    self.logger.warning(f"ID de grupo {grupo_nombre} no disponible")
                    continue
                    
                response = requests.get(
                    f"{self.proxy_url}/nifi-api/flow/process-groups/{grupo_id}/processors",
                    timeout=self.timeout
                )
                
                if response.status_code != 200:
                    self.logger.warning(f"Error obteniendo procesadores de grupo {grupo_nombre}: {response.status_code}")
                    continue
                    
                data = response.json()
                
                if "processors" not in data:
                    self.logger.warning(f"No se encontraron procesadores en grupo {grupo_nombre}")
                    continue
                    
                processors = data["processors"]
                self.logger.info(f"Encontrados {len(processors)} procesadores en grupo {grupo_nombre}")
                
                # Buscar procesadores por tipo
                for proc in processors:
                    proc_type = proc.get("component", {}).get("type", "")
                    proc_name = proc.get("component", {}).get("name", "")
                    proc_id = proc.get("id")
                    
                    # Buscar por tipo exacto
                    for tipo, clave in tipos_procesadores.items():
                        if tipo == proc_type or tipo.split(".")[-1] == proc_type:
                            self.logger.info(f"✅ Encontrado procesador por tipo: {proc_name} ({proc_type}) -> {proc_id}")
                            self.processor_ids[clave] = proc_id
                            
                            # Configurar atributos dinámicamente
                            attr_name = f"{clave}_processor_id"
                            setattr(self, attr_name, proc_id)
                            
                            # Marcar como encontrado para no seguir buscando
                            tipos_procesadores.pop(tipo, None)
                            break
            
            # Mostrar resumen de procesadores encontrados
            self.logger.info(f"✅ Encontrados {len(self.processor_ids)} procesadores por tipo")
            for clave, proc_id in self.processor_ids.items():
                self.logger.info(f"  - {clave}: {proc_id}")
                
            return self.processor_ids
                
        except Exception as e:
            self.logger.error(f"Error buscando procesadores por tipo: {str(e)}")
            return {}

    def buscar_procesador_por_nombre(self, nombre):
        """
        Busca un procesador utilizando el endpoint de búsqueda avanzada de NiFi
        con soporte para búsqueda por expresiones regulares.
        """
        try:
            # Usar el endpoint de búsqueda correcto del mini-proxy
            self.logger.info(f"Buscando procesador utilizando endpoint search: {nombre}")
            
            response = requests.get(
                f"{self.proxy_url}/api/flow/search-results?q={nombre}",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Error en búsqueda: {response.status_code}")
                self.logger.warning(f"Respuesta: {response.text[:200]}")
                return None
            
            data = response.json()
            
            if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                processors = data["searchResultsDTO"]["processorResults"]
                
                # Buscar coincidencia exacta
                for proc in processors:
                    proc_name = proc.get("name", "").lower()
                    if proc_name == nombre.lower():
                        self.logger.info(f"✓ Coincidencia exacta: {proc.get('name')} -> {proc.get('id')}")
                        return proc.get("id")
                
                # Si no hay coincidencia exacta, buscar coincidencia parcial
                for proc in processors:
                    proc_name = proc.get("name", "").lower()
                    if nombre.lower() in proc_name:
                        self.logger.info(f"✓ Coincidencia parcial: {proc.get('name')} -> {proc.get('id')}")
                        return proc.get("id")
                
                # Si no hay coincidencia por nombre, buscar por tipo
                for proc in processors:
                    proc_type = proc.get("type", "").lower()
                    if nombre.lower() in proc_type:
                        self.logger.info(f"✓ Coincidencia por tipo: {proc.get('name')} -> {proc.get('id')}")
                        return proc.get("id")
            
            self.logger.warning(f"No se encontró procesador con nombre: {nombre}")
            return None
            
        except Exception as e:
            self.logger.warning(f"Error buscando procesador {nombre}: {str(e)}")
            return None
    
    def configurar_topic_kafka(self, procesador_id, topic_nombre):
        """
        Configura un topic en un procesador Kafka.
        """
        try:
            # Primero obtener la configuración actual
            response = requests.get(
                f"{self.proxy_url}/nifi-api/processors/{procesador_id}",
                timeout=self.timeout
            )
            
            if response.status_code != 200:
                self.logger.warning(f"Error obteniendo configuración del procesador: {response.status_code}")
                return False
            
            processor_config = response.json()
            
            # Verificar si ya tiene el property 'Topic Name'
            config = processor_config.get("component", {}).get("config", {})
            properties = config.get("properties", {})
            
            # Comparar con el valor actual
            current_topic = properties.get("Topic Name")
            self.logger.info(f"Topic actual en {procesador_id}: {current_topic}")
            
            if current_topic == topic_nombre:
                self.logger.info(f"El procesador ya está configurado con el topic {topic_nombre}")
                return True
            
            # Actualizar la configuración con el nuevo topic
            processor_config["component"]["config"]["properties"]["Topic Name"] = topic_nombre
            processor_config["revision"] = {
                "version": processor_config.get("revision", {}).get("version", 0)
            }
            
            # Aplicar la configuración
            update_response = requests.put(
                f"{self.proxy_url}/nifi-api/processors/{procesador_id}",
                json=processor_config,
                timeout=self.timeout
            )
            
            if update_response.status_code in [200, 202]:
                self.logger.info(f"✅ Topic actualizado correctamente a {topic_nombre} en {procesador_id}")
                return True
            else:
                self.logger.warning(f"❌ Error actualizando topic: {update_response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error configurando topic: {str(e)}")
            return False
    
    def detener_procesador(self, procesador_id):
        """
        Detiene un procesador específico.
        """
        try:
            # Obtener el estado actual
            estado = self.obtener_estado_procesador(procesador_id)
            estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
            
            if estado_actual == "STOPPED":
                self.logger.info(f"Procesador {procesador_id} ya está detenido")
                return True
            
            # Preparar la solicitud para detener
            request_body = {
                "revision": {
                    "version": estado.get("revision", {}).get("version", 0),
                    "clientId": "airflow-client"
                },
                "component": {
                    "id": procesador_id,
                    "state": "STOPPED"
                }
            }
            
            # Enviar solicitud para detener
            response = requests.put(
                f"{self.proxy_url}/nifi-api/processors/{procesador_id}/run-status",
                json=request_body,
                timeout=self.timeout
            )
            
            if response.status_code in [200, 202]:
                self.logger.info(f"✅ Procesador {procesador_id} detenido correctamente")
                return True
            else:
                self.logger.warning(f"❌ Error deteniendo procesador {procesador_id}: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error deteniendo procesador {procesador_id}: {str(e)}")
            return False
    
    def iniciar_procesador(self, procesador_id):
        """
        Inicia un procesador específico con reintentos.
        """
        try:
            max_reintentos = 3
            for intento in range(1, max_reintentos + 1):
                self.logger.info(f"Intentando iniciar procesador {procesador_id} (intento {intento}/{max_reintentos})")
                
                # Obtener el estado actual
                estado = self.obtener_estado_procesador(procesador_id)
                estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
                
                if estado_actual == "RUNNING":
                    self.logger.info(f"Procesador {procesador_id} ya está en ejecución")
                    return True
                
                # Preparar la solicitud para iniciar
                request_body = {
                    "revision": {
                        "version": estado.get("revision", {}).get("version", 0),
                        "clientId": "airflow-client"
                    },
                    "component": {
                        "id": procesador_id,
                        "state": "RUNNING"
                    }
                }
                
                # Enviar solicitud para iniciar
                response = requests.put(
                    f"{self.proxy_url}/nifi-api/processors/{procesador_id}/run-status",
                    json=request_body,
                    timeout=self.timeout
                )
                
                if response.status_code in [200, 202]:
                    # Verificar que realmente se inició
                    time.sleep(3)
                    nuevo_estado = self.obtener_estado_procesador(procesador_id)
                    if nuevo_estado.get("component", {}).get("state") == "RUNNING":
                        self.logger.info(f"✅ Procesador {procesador_id} iniciado correctamente")
                        return True
                    else:
                        self.logger.warning(f"Procesador no pasó a RUNNING, estado: {nuevo_estado.get('component', {}).get('state', 'UNKNOWN')}")
                else:
                    self.logger.warning(f"Error al iniciar (código: {response.status_code})")
                
                # Si llegamos aquí, hubo error. Esperar antes de reintentar
                time.sleep(intento * 2)
            
            self.logger.error(f"❌ No se pudo iniciar el procesador {procesador_id} después de {max_reintentos} intentos")
            return False
                
        except Exception as e:
            self.logger.error(f"Error iniciando procesador {procesador_id}: {str(e)}")
            return False
    
    def obtener_estado_procesador(self, procesador_id):
        """
        Obtiene el estado actual de un procesador.
        """
        try:
            response = requests.get(
                f"{self.proxy_url}/nifi-api/processors/{procesador_id}",
                timeout=self.timeout
            )
            
            if response.status_code == 200:
                return response.json()
            else:
                self.logger.warning(f"Error obteniendo estado del procesador {procesador_id}: {response.status_code}")
                return {"component": {"state": "UNKNOWN"}}
                
        except Exception as e:
            self.logger.error(f"Error obteniendo estado del procesador {procesador_id}: {str(e)}")
            return {"component": {"state": "UNKNOWN"}}
    
    def detener_procesadores_grupo(self, grupo_id):
        """
        Detiene todos los procesadores de un grupo específico.
        """
        try:
            # Intentar detener todo el grupo con una sola solicitud
            group_stop_response = requests.put(
                f"{self.proxy_url}/nifi-api/flow/process-groups/{grupo_id}/stop",
                json={"id": grupo_id, "state": "STOPPED"},
                timeout=self.timeout
            )
            
            if group_stop_response.status_code in [200, 202]:
                self.logger.info(f"✅ Grupo {grupo_id} detenido exitosamente")
                return True
                
            self.logger.warning(f"No se pudo detener grupo completo: {group_stop_response.status_code}")
            
            # Si no funciona, buscar y detener procesadores individuales
            return False
            
        except Exception as e:
            self.logger.error(f"Error deteniendo procesadores del grupo {grupo_id}: {str(e)}")
            return False
    
    def detener_todos_procesadores(self):
        """
        Detiene todos los procesadores en los grupos productor y consumidor.
        """
        self.logger.info("Deteniendo todos los procesadores...")
        resultado_productor = self.detener_procesadores_grupo(self.producer_group_id)
        resultado_consumidor = self.detener_procesadores_grupo(self.consumer_group_id)
        
        # Si falla la detención por grupo, intentar detener individualmente
        if not resultado_productor or not resultado_consumidor:
            self.logger.info("Intentando detener procesadores individualmente...")
            for clave, id_proc in self.processor_ids.items():
                self.detener_procesador(id_proc)
        
        return True
    
    def iniciar_procesadores_consumidor(self):
        """
        Inicia los procesadores del grupo consumidor en el orden correcto.
        """
        self.logger.info("Iniciando procesadores del grupo consumidor...")
        
        # Orden específico para iniciar procesadores consumidores
        orden_consumidor = ["consume_kafka", "evaluate_json_path", "replace_text", "put_db"]
        
        for tipo in orden_consumidor:
            if tipo in self.processor_ids:
                procesador_id = self.processor_ids[tipo]
                nombre = self.processor_mapping[tipo]
                
                self.logger.info(f"Iniciando {nombre} (ID: {procesador_id})...")
                resultado = self.iniciar_procesador(procesador_id)
                
                self.logger.info(f"Resultado iniciar {nombre}: {resultado}")
                time.sleep(2)  # Pausa entre inicios
        
        return True
    
    def iniciar_procesadores_productor(self):
        """
        Inicia los procesadores del grupo productor en el orden correcto.
        """
        self.logger.info("Iniciando procesadores del grupo productor...")
        
        # Orden específico para iniciar procesadores productores
        orden_productor = ["query_db_table", "convert_record", "split_json", "merge_content", "publish_kafka"]
        
        for tipo in orden_productor:
            if tipo in self.processor_ids:
                procesador_id = self.processor_ids[tipo]
                nombre = self.processor_mapping[tipo]
                
                self.logger.info(f"Iniciando {nombre} (ID: {procesador_id})...")
                resultado = self.iniciar_procesador(procesador_id)
                
                self.logger.info(f"Resultado iniciar {nombre}: {resultado}")
                time.sleep(2)  # Pausa entre inicios
        
        return True
    
    def limpiar_estado_procesador(self, procesador_id):
        """
        Limpia el estado de un procesador (útil para reiniciar desde cero).
        """
        try:
            response = requests.post(
                f"{self.proxy_url}/nifi-api/processors/{procesador_id}/state/clear-requests",
                timeout=self.timeout
            )
            
            if response.status_code in [200, 202]:
                self.logger.info(f"✅ Estado del procesador {procesador_id} limpiado correctamente")
                return True
            else:
                self.logger.warning(f"❌ Error limpiando estado del procesador {procesador_id}: {response.status_code}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error limpiando estado del procesador {procesador_id}: {str(e)}")
            return False