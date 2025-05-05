"""
Utilidades específicas para la integración de NiFi con el procesamiento de potencia.
"""
import logging
import traceback
import time
import subprocess
from typing import Dict, List, Optional

import requests
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Importaciones de módulos propios
from modules.nifi_utils import NiFiConnector
from modules.gestor_ids_procesadores import GestorIdsProcesadores
from modules.fabrica_nifi import FabricaNifi, crear_instancia_nifi
from configs.potencia_config import PRODUCER_GROUP_ID, CONSUMER_GROUP_ID

# Configuración de logger
logger = logging.getLogger(__name__)


# Configuración de logging
logger = logging.getLogger(__name__)

class PotenciaNiFiManager:
    """
    Gestor especializado para manejar el flujo NiFi de potencia.
    """
    
    def __init__(self, nifi_connector=None, gestor_ids=None):
        """
        Inicializa el gestor de NiFi para procesamiento de potencia
        
        Args:
            nifi_connector: Conector NiFi existente o None para crear uno nuevo
            gestor_ids: Gestor de IDs de procesadores opcional
        """
        if isinstance(nifi_connector, NiFiConnector):
            # Si se pasa un NiFiConnector directamente
            self.nifi = nifi_connector
        else:
            # Crear nueva instancia con la URL del proxy
            proxy_url = nifi_connector if isinstance(nifi_connector, str) else None
            self.nifi = NiFiConnector(proxy_url=proxy_url)
        
        # Guardar referencia al gestor de IDs si se proporciona
        self.gestor_ids = gestor_ids
        
        # Configurar IDs de grupos desde la configuración
        self.nifi.producer_group_id = PRODUCER_GROUP_ID
        self.nifi.consumer_group_id = CONSUMER_GROUP_ID
    
    def verificar_y_actualizar_ids_grupos(self):
        """Verifica y actualiza los IDs de los grupos si es necesario"""
        logger.info("Verificando IDs de grupos de procesadores...")
        
        # IDs hardcodeados como fallback (los que funcionan en dag_prueba.py)
        producer_id_fallback = "4dcc0bed-9061-3a4c-0000-c375f77615eb"
        consumer_id_fallback = "723a3709-dabb-3fae-0000-2892da95b9a6"
        
        try:
            # Intentar obtener el root ID
            response = requests.get(
                f"{self.nifi.proxy_url}/api/flow/about",
                timeout=self.nifi.http_timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                root_id = data.get("controllerDTO", {}).get("id", "root")
                
                # Listar grupos en la raíz
                response = requests.get(
                    f"{self.nifi.proxy_url}/api/flow/process-groups/{root_id}/process-groups",
                    timeout=self.nifi.http_timeout
                )
                
                if response.status_code == 200:
                    grupos = response.json().get("processGroups", [])
                    
                    # Buscar grupos por nombre
                    for grupo in grupos:
                        nombre = grupo.get("component", {}).get("name", "").lower()
                        if "producer" in nombre or "productor" in nombre:
                            self.nifi.producer_group_id = grupo.get("id")
                            logger.info(f"Grupo productor encontrado: {self.nifi.producer_group_id}")
                            
                        if "consumer" in nombre or "consumidor" in nombre:
                            self.nifi.consumer_group_id = grupo.get("id")
                            logger.info(f"Grupo consumidor encontrado: {self.nifi.consumer_group_id}")
            
            # Si no se han encontrado, usar fallback
            if not hasattr(self.nifi, 'producer_group_id') or not self.nifi.producer_group_id:
                logger.warning("Usando ID fallback para grupo productor")
                self.nifi.producer_group_id = producer_id_fallback
                
            if not hasattr(self.nifi, 'consumer_group_id') or not self.nifi.consumer_group_id:
                logger.warning("Usando ID fallback para grupo consumidor")
                self.nifi.consumer_group_id = consumer_id_fallback
                
            logger.info(f"IDs finales: Productor={self.nifi.producer_group_id}, Consumidor={self.nifi.consumer_group_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error verificando IDs de grupos: {str(e)}")
            
            # En caso de error, usar IDs fallback
            self.nifi.producer_group_id = producer_id_fallback
            self.nifi.consumer_group_id = consumer_id_fallback
            logger.warning(f"Usando IDs fallback: Productor={self.nifi.producer_group_id}, Consumidor={self.nifi.consumer_group_id}")
            
            return False
    def crear_tabla_original_si_no_existe(self):
        """
        Crea la tabla potencia_dep_original si no existe, como copia de potencia_dep
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            
            # Verificar si existe la tabla original
            original_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
            )[0]
            
            if not original_existe:
                logger.info("🔍 Tabla potencia_dep_original no existe. Creando desde potencia_dep...")
                pg_hook.run("CREATE TABLE potencia_dep_original AS SELECT * FROM potencia_dep")
                logger.info("✅ Tabla potencia_dep_original creada correctamente")
            else:
                logger.info("✅ Tabla potencia_dep_original ya existe")
                
            return True
        except Exception as e:
            logger.error(f"❌ Error creando tabla original: {str(e)}")
            return False

    def _limpiar_estado_procesador_forzado(self, processor_id):
        """Método más agresivo para limpiar el estado de un procesador"""
        import requests
        
        logger.info(f"🧹 Limpiando estado del procesador {processor_id} con método forzado...")
        
        # 1. Intentar con DELETE a diferentes endpoints
        urls = [
            f"{self.nifi.proxy_url}/api/processors/{processor_id}/state",
            f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/state",
            f"{self.nifi.proxy_url}/api/processors/{processor_id}/state/clear",
            f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/state/clear"
        ]
        
        for url in urls:
            try:
                response = requests.delete(url, timeout=30)
                if response.status_code in [200, 201, 202, 204]:
                    logger.info(f"✅ Estado limpiado con éxito usando {url}")
                    return True
            except:
                pass
        
        # 2. Intentar detener y esperar antes de iniciar (reset manual)
        try:
            self._detener_procesador_forzado(processor_id)
            import time
            time.sleep(3)  # Esperar a que se detenga completamente
            logger.info("⏹️ Procesador detenido para reset manual")
            return True
        except:
            pass
        
        logger.warning("⚠️ No se pudo limpiar estado del procesador con ningún método")
        return False

    def _detener_procesador_forzado(self, processor_id):
        """Método agresivo para detener un procesador"""
        import requests
        
        urls = [
            f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
            f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/run-status"
        ]
        
        data_formats = [
            {"state": "STOPPED", "disconnectedNodeAcknowledged": True},
            {"revision": {"version": 0}, "state": "STOPPED", "disconnectedNodeAcknowledged": True},
            {"revision": {"clientId": "airflow", "version": 0}, "state": "STOPPED"}
        ]
        
        for url in urls:
            for data in data_formats:
                try:
                    response = requests.put(url, json=data, timeout=30)
                    if response.status_code in [200, 201, 202]:
                        return True
                except:
                    continue
        
        return False

    def _iniciar_procesador_forzado(self, processor_id):
        """Método agresivo y con múltiples reintentos para iniciar un procesador"""
        import requests
        import time
        
        # Máximo 3 intentos
        for intento in range(3):
            logger.info(f"Intento {intento+1}/3 para iniciar procesador {processor_id}...")
            
            # Obtener nombre del procesador para mejor logging
            proc_name = "Desconocido"
            try:
                resp = requests.get(
                    f"{self.nifi.proxy_url}/api/processors/{processor_id}",
                    timeout=30
                )
                if resp.status_code == 200:
                    data = resp.json()
                    if 'component' in data:
                        proc_name = data['component'].get('name', "Desconocido")
            except:
                pass
            
            # Intentar varios formatos y URLs
            urls = [
                f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
                f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/run-status"
            ]
            
            data_formats = [
                {"state": "RUNNING", "disconnectedNodeAcknowledged": True},
                {"revision": {"version": 0}, "state": "RUNNING", "disconnectedNodeAcknowledged": True},
                {"revision": {"clientId": "airflow", "version": 0}, "state": "RUNNING"},
                {"state": "RUNNING"}  # Formato más simple
            ]
            
            for url in urls:
                for data in data_formats:
                    try:
                        response = requests.put(url, json=data, timeout=30)
                        if response.status_code in [200, 201, 202]:
                            logger.info(f"✅ Procesador {proc_name} ({processor_id}) iniciado correctamente")
                            return True
                    except Exception as e:
                        logger.debug(f"Error con {url}: {str(e)}")
                        continue
            
            # Si llegamos aquí, es que falló el intento actual
            if intento < 2:  # Solo esperar si vamos a volver a intentar
                time.sleep(3)  # Esperar antes del siguiente intento
        
        logger.error(f"❌ No se pudo iniciar procesador {proc_name} ({processor_id}) después de 3 intentos")
        return False

    def iniciar_procesamiento_mes_nuclear(self, mes, year=None, source_table=None, dest_table=None):
        """
        Inicia el procesamiento utilizando el enfoque original que recupera datos de tension_dep
        pero con un filtro por mes en la consulta SQL.
        """
        import time
        import requests
        
        try:
            # Usar año específico o valor por defecto
            current_year = year if year else 2019
            tabla_origen = source_table or "tension_dep"
            
            # Calcular fechas de inicio y fin precisas para el mes
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = current_year if mes < 12 else current_year + 1
            fecha_inicio = f"{current_year}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            logger.info(f"📋 MÉTODO ORIGINAL: Configurando consulta personalizada para mes {mes} ({fecha_inicio} a {fecha_fin})")
            
            # 1. Obtener IDs de procesadores necesarios
            query_db_processor_id = self._resolver_id_procesador_query()
            put_db_processor_id = self._resolver_id_procesador_put_db()
            convert_record_id = getattr(self.gestor_ids, 'convert_record_processor_id', None)
            split_json_id = getattr(self.gestor_ids, 'split_json_processor_id', None)
            merge_content_id = getattr(self.gestor_ids, 'merge_content_processor_id', None)
            publish_kafka_id = getattr(self.gestor_ids, 'publish_kafka_processor_id', None)
            consume_kafka_id = getattr(self.gestor_ids, 'consume_kafka_processor_id', None)
            evaluate_json_path_id = getattr(self.gestor_ids, 'evaluate_json_path_processor_id', None)
            replace_text_id = getattr(self.gestor_ids, 'replace_text_processor_id', None)
            
            # 2. Detener todos los procesadores para iniciar desde un estado limpio
            logger.info("⏸️ Deteniendo todos los procesadores...")
            self._detener_todos_procesadores()
            time.sleep(5)
            
            # 3. Verificar registros existentes para este mes
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            query_verificar = f"""
                SELECT COUNT(*) FROM {tabla_origen} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {current_year}
            """
            count = pg_hook.get_first(query_verificar)[0]
            
            if count == 0:
                logger.info(f"⚠️ No hay datos para el mes {mes} en {tabla_origen}. Abortando procesamiento.")
                return False
            
            logger.info(f"✅ Encontrados {count} registros en {tabla_origen} para el mes {mes}")
            
            # 4. CREAR CONSULTA SQL SIMILAR A LA ORIGINAL pero con filtro por mes
            nueva_consulta = f"""/* CONSULTA FILTRADA POR MES {mes} */
    SELECT 
        fecha, 
        hora, 
        alimentador,
        dia_semana, 
        potencia_activa AS potencia_activa_real, 
        potencia_reactiva AS potencia_reactiva_real
    FROM {tabla_origen} 
    WHERE EXTRACT(MONTH FROM fecha) = {mes}
    AND EXTRACT(YEAR FROM fecha) = {current_year}
    AND fecha >= '{fecha_inicio}'
    AND fecha < '{fecha_fin}'
    ORDER BY fecha, hora, alimentador"""
            
            # 5. Configurar el QueryDatabaseTable con la consulta SQL
            try:
                # Primero detener el procesador
                self._detener_procesador_directo(query_db_processor_id)
                time.sleep(2)
                
                # Obtener la revisión actual para actualizar correctamente
                response = requests.get(
                    f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                    timeout=30
                )
                
                if response.status_code == 200:
                    processor_data = response.json()
                    revision = processor_data.get('revision', {})
                    
                    # Configurar con estilo similar al original
                    payload = {
                        "component": {
                            "id": query_db_processor_id,
                            "properties": {
                                "Table Name": tabla_origen,
                                "Custom Query": nueva_consulta,
                                "db-fetch-sql-query": nueva_consulta,  # Para compatibilidad
                                "where.clause": "",  # Vacío, usamos WHERE en la SQL
                                "Max Wait Time": "30 seconds"  # Valor original
                            }
                        },
                        "revision": revision
                    }
                    
                    # Actualizar procesador con nuevos valores
                    update_response = requests.put(
                        f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                        json=payload,
                        timeout=30
                    )
                    
                    if update_response.status_code in [200, 201, 202]:
                        logger.info("✅ QueryDatabaseTable configurado correctamente")
                    else:
                        logger.warning(f"⚠️ Error configurando QueryDatabaseTable: {update_response.status_code}")
                        # Continuar de todas formas
            except Exception as e:
                logger.warning(f"⚠️ Error en configuración de QueryDatabaseTable: {str(e)}")
            
            # 6. Limpiar estado de los procesadores
            logger.info("🧹 Limpiando estado de los procesadores...")
            for processor_id in [query_db_processor_id, put_db_processor_id]:
                if processor_id:
                    self._limpiar_estado_procesador(processor_id)
                    time.sleep(1)
            
            # 7. INICIO ORDENADO DE PROCESADORES INDIVIDUALES
            # Se inician en orden específico para garantizar el flujo correcto
            logger.info("▶️ Iniciando procesadores en secuencia...")
            
            # 7.1. Iniciar primero QueryDatabaseTable
            if self._iniciar_procesador_directo(query_db_processor_id):
                logger.info("✅ QueryDatabaseTable iniciado correctamente")
            else:
                logger.error("❌ No se pudo iniciar QueryDatabaseTable")
            
            time.sleep(2)  # Esperar antes de iniciar el siguiente
            
            # 7.2. Iniciar ConvertRecord
            if convert_record_id and self._iniciar_procesador_directo(convert_record_id):
                logger.info("✅ ConvertRecord iniciado correctamente")
            
            time.sleep(2)
            
            # 7.3. Iniciar SplitJson
            if split_json_id and self._iniciar_procesador_directo(split_json_id):
                logger.info("✅ SplitJson iniciado correctamente")
            
            time.sleep(2)
            
            # 7.4. Iniciar MergeContent
            if merge_content_id and self._iniciar_procesador_directo(merge_content_id):
                logger.info("✅ MergeContent iniciado correctamente")
            
            time.sleep(2)
            
            # 7.5. Iniciar PublishKafka
            if publish_kafka_id and self._iniciar_procesador_directo(publish_kafka_id):
                logger.info("✅ PublishKafka iniciado correctamente")
            
            time.sleep(3)  # Espera más larga antes de iniciar consumidores
            
            # 7.6. Iniciar ConsumeKafka
            if consume_kafka_id and self._iniciar_procesador_directo(consume_kafka_id):
                logger.info("✅ ConsumeKafka iniciado correctamente")
            
            time.sleep(2)
            
            # 7.7. Iniciar EvaluateJsonPath
            if evaluate_json_path_id and self._iniciar_procesador_directo(evaluate_json_path_id):
                logger.info("✅ EvaluateJsonPath iniciado correctamente")
            
            time.sleep(2)
            
            # 7.8. Iniciar ReplaceText
            if replace_text_id and self._iniciar_procesador_directo(replace_text_id):
                logger.info("✅ ReplaceText iniciado correctamente")
            
            time.sleep(2)
            
            # 7.9. Iniciar PutDatabaseRecord
            if self._iniciar_procesador_directo(put_db_processor_id):
                logger.info("✅ PutDatabaseRecord iniciado correctamente")
            else:
                logger.error("❌ No se pudo iniciar PutDatabaseRecord")
            
            # 8. Verificación final
            procesadores_iniciados = []
            for proc_id in [query_db_processor_id, convert_record_id, split_json_id, 
                        merge_content_id, publish_kafka_id, consume_kafka_id,
                        evaluate_json_path_id, replace_text_id, put_db_processor_id]:
                if proc_id:
                    estado = self._verificar_estado_procesador(proc_id)
                    if estado == "RUNNING":
                        procesadores_iniciados.append(proc_id)
            
            logger.info(f"✅ Total procesadores iniciados: {len(procesadores_iniciados)}/{9}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en método original para mes {mes}: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def _verificar_estado_procesador(self, processor_id):
        """Verifica el estado actual de un procesador"""
        import requests
        
        try:
            response = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{processor_id}",
                timeout=20
            )
            
            if response.status_code == 200:
                data = response.json()
                return data.get('component', {}).get('state', 'UNKNOWN')
        except:
            pass
            
        return "UNKNOWN"

    def _detener_procesador_directo(self, processor_id):
        """Detiene un procesador de forma directa"""
        import requests
        
        try:
            # Obtener revisión actual del procesador
            response = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{processor_id}",
                timeout=20
            )
            
            if response.status_code == 200:
                processor_data = response.json()
                if 'revision' in processor_data:
                    revision = processor_data['revision']
                    
                    # Parar el procesador con la revisión correcta
                    payload = {
                        "revision": revision,
                        "state": "STOPPED",
                        "disconnectedNodeAcknowledged": True
                    }
                    
                    stop_response = requests.put(
                        f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
                        json=payload,
                        timeout=20
                    )
                    
                    if stop_response.status_code in [200, 201, 202]:
                        return True
        except:
            pass
            
        return False

    def _iniciar_procesador_directo(self, processor_id):
        """Inicia un procesador de forma directa"""
        import requests
        
        try:
            # Obtener revisión actual del procesador
            response = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{processor_id}",
                timeout=20
            )
            
            if response.status_code == 200:
                processor_data = response.json()
                name = processor_data.get('component', {}).get('name', 'Desconocido')
                
                if 'revision' in processor_data:
                    revision = processor_data['revision']
                    
                    # Iniciar el procesador con la revisión correcta
                    payload = {
                        "revision": revision,
                        "state": "RUNNING",
                        "disconnectedNodeAcknowledged": True
                    }
                    
                    start_response = requests.put(
                        f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
                        json=payload,
                        timeout=20
                    )
                    
                    if start_response.status_code in [200, 201, 202]:
                        logger.info(f"✅ Procesador {name} iniciado correctamente")
                        return True
                    else:
                        logger.warning(f"⚠️ Error iniciando {name}: {start_response.status_code}")
        except Exception as e:
            logger.warning(f"⚠️ Excepción iniciando procesador {processor_id}: {str(e)}")
            
        return False

    def limpiar_vista_temporal(self):
        """
        Elimina la vista temporal creada para el procesamiento mensual
        """
        try:
            if hasattr(self, 'vista_actual') and self.vista_actual:
                logger.info(f"🧹 Eliminando vista temporal {self.vista_actual}...")
                
                pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
                
                # Verificar que la vista existe
                vista_existe = pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.views WHERE table_name = '{self.vista_actual}')"
                )[0]
                
                if vista_existe:
                    # Eliminar la vista
                    pg_hook.run(f"DROP VIEW IF EXISTS {self.vista_actual}")
                    logger.info(f"✅ Vista {self.vista_actual} eliminada correctamente")
                    
                    # Limpiar la referencia
                    self.vista_actual = None
                    return True
                else:
                    logger.warning(f"⚠️ La vista {self.vista_actual} no existe para eliminar")
            else:
                logger.warning("⚠️ No hay información de vista para limpiar")
            
            return False
        except Exception as e:
            logger.error(f"❌ Error eliminando vista temporal: {str(e)}")
            return False

    def finalizar_procesamiento(self):
        """
        Realiza tareas de limpieza al finalizar el procesamiento completo
        """
        try:
            # Limpiar vista si existe
            self.limpiar_vista_temporal()
            
            # Detener todos los procesadores
            logger.info("⏹️ Deteniendo todos los procesadores al finalizar...")
            self._detener_todos_procesadores()
            
            logger.info("✅ Procesamiento finalizado correctamente")
            return True
        except Exception as e:
            logger.error(f"❌ Error finalizando procesamiento: {str(e)}")
            return False

    def _resolver_id_procesador_query(self):
        """Resolver ID del procesador QueryDatabaseTable de múltiples fuentes"""
        query_db_processor_id = None
        
        # 1. Primero, verificar si está como atributo de esta instancia
        if hasattr(self, 'query_db_processor_id') and self.query_db_processor_id:
            query_db_processor_id = self.query_db_processor_id
        
        # 2. Luego, buscar en el gestor_ids si existe
        if not query_db_processor_id and self.gestor_ids:
            if hasattr(self.gestor_ids, 'query_db_table_processor_id'):
                query_db_processor_id = self.gestor_ids.query_db_table_processor_id
        
        # 3. Si aún no lo encontramos, usar el ID hardcodeado
        if not query_db_processor_id:
            query_db_processor_id = "a6ee2f4e-edbc-38f9-a152-b0a19044d4a9"
            
            # Guardar para uso futuro
            self.query_db_processor_id = query_db_processor_id
            
        return query_db_processor_id
    
    def _resolver_id_procesador_put_db(self):
        """Resolver ID del procesador PutDatabaseRecord de múltiples fuentes"""
        put_db_processor_id = None
        
        # 1. Primero desde self
        if hasattr(self, 'put_db_processor_id') and self.put_db_processor_id:
            put_db_processor_id = self.put_db_processor_id
        
        # 2. Luego desde gestor_ids con múltiples nombres posibles
        if not put_db_processor_id and self.gestor_ids:
            for attr_name in ['put_database_record_processor_id', 'put_db_processor_id']:
                if hasattr(self.gestor_ids, attr_name):
                    put_db_processor_id = getattr(self.gestor_ids, attr_name)
                    break
        
        # 3. Si aún no lo encontramos, usar el ID hardcodeado
        if not put_db_processor_id:
            put_db_processor_id = "10d89865-0196-1000-3eaf-c624cd671d97"
            
            # Guardar para uso futuro
            self.put_db_processor_id = put_db_processor_id
            
        return put_db_processor_id
        
    def _limpiar_estado_procesador(self, processor_id):
        """Limpia el estado del procesador para forzar procesamiento desde cero"""
        try:
            logger.info(f"🧹 Limpiando estado del procesador {processor_id}...")
            
            # Método 1: Usar API directamente
            try:
                response = requests.post(
                    f"{self.nifi.proxy_url}/api/processors/{processor_id}/state/clear-requests",
                    json={"disconnectedNodeAcknowledged": True},
                    timeout=60
                )
                
                if response.status_code in [200, 202, 204]:
                    logger.info("✅ Estado del procesador limpiado correctamente")
                    return True
            except Exception as e:
                logger.warning(f"Error con método 1 de limpieza: {str(e)}")
            
            # Método 2: Intentar URL alternativa si método 1 falla
            try:
                response = requests.post(
                    f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/state/clear-requests",
                    json={"disconnectedNodeAcknowledged": True},
                    timeout=60
                )
                
                if response.status_code in [200, 202, 204]:
                    logger.info("✅ Estado del procesador limpiado correctamente (método 2)")
                    return True
            except Exception as e:
                logger.warning(f"Error con método 2 de limpieza: {str(e)}")
            
            # Método 3: Similar a dag_prueba.py usando curl directamente
            try:
                host = "588a471cac3f"  # ID del contenedor NiFi
                cmd_token = f"curl -k -s -X POST -d 'username=admin&password=centrosur123' https://{host}:8443/nifi-api/access/token"
                token_process = subprocess.run(cmd_token, shell=True, capture_output=True, text=True)
                token = token_process.stdout.strip()
                
                if token:
                    cmd_clear = f"curl -k -s -X POST -H 'Authorization: Bearer {token}' -H 'Content-Type: application/json' https://{host}:8443/nifi-api/processors/{processor_id}/state/clear-requests"
                    clear_process = subprocess.run(cmd_clear, shell=True, capture_output=True, text=True)
                    
                    if clear_process.returncode == 0:
                        logger.info("✅ Estado del procesador limpiado correctamente (método curl)")
                        return True
            except Exception as e:
                logger.warning(f"Error con método 3 de limpieza: {str(e)}")
            
            logger.warning("⚠️ No se pudo limpiar estado del procesador con ningún método")
            return False
        except Exception as e:
            logger.error(f"❌ Error general limpiando estado: {str(e)}")
            return False

    def restaurar_configuracion_original(self, mes, year=None):
        """
        Restaura la configuración original sin vista temporal
        """
        import time
        import requests
        
        try:
            query_db_processor_id = self._resolver_id_procesador_query()
            
            logger.info("🔄 Restaurando configuración que funcionaba en potencia_integrador_anual")
            
            # 1. Detener procesador para evitar procesamiento durante cambio
            self._detener_procesador_directo(query_db_processor_id)
            time.sleep(3)
            
            # 2. Obtener revisión actual
            response = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                timeout=30
            )
            
            processor_data = response.json()
            revision = processor_data.get('revision', {})
            
            # 3. SOLUCIÓN FUNDAMENTAL: Crear payload con propiedades CORRECTAS
            #    - Usar Table Name = tension_dep (sin Custom Query)
            #    - Incluir SOLO propiedades esenciales
            payload = {
                "component": {
                    "id": query_db_processor_id,
                    "properties": {
                        "Table Name": "tension_dep",
                        "Custom Query": "",
                        "db-fetch-sql-query": "",
                        "where.clause": f"EXTRACT(MONTH FROM fecha) = {mes} AND EXTRACT(YEAR FROM fecha) = {year or 2019}",
                        "Columns to Return": "fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva"
                    }
                },
                "revision": revision
            }
            
            # 4. Actualizar con las propiedades correctas
            update_response = requests.put(
                f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                json=payload,
                timeout=30
            )
            
            if update_response.status_code != 200:
                logger.error(f"Error actualizando procesador: {update_response.status_code}")
                return False
                
            logger.info("✅ Configuración restaurada correctamente")
            
            # 5. Limpiar estado y reiniciar
            self._limpiar_estado_procesador(query_db_processor_id)
            time.sleep(2)
            self._iniciar_procesador_directo(query_db_processor_id)
            time.sleep(2)
            
            # 6. Iniciar el procesador PutDatabaseRecord
            put_db_processor_id = self._resolver_id_procesador_put_db()
            self._iniciar_procesador_directo(put_db_processor_id)
            
            return True
        except Exception as e:
            logger.error(f"Error restaurando configuración: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def configurar_consulta_con_filtro_mes(self, mes, year=None):
        """
        Configura una consulta única con filtro de mes incorporado directamente en el SQL
        """
        import time
        import requests
        
        try:
            # Obtener el ID del procesador QueryDatabaseTable
            query_db_processor_id = self._resolver_id_procesador_query()
            if not query_db_processor_id:
                logger.error("No se pudo resolver ID de QueryDatabaseTable")
                return False
                
            current_year = year if year else 2019
            
            # Detener el procesador primero
            self._detener_procesador_directo(query_db_processor_id)
            time.sleep(3)
            
            # Obtener revisión actual del procesador
            response = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                timeout=30
            )
            
            if response.status_code != 200:
                logger.error(f"Error obteniendo estado del procesador: {response.status_code}")
                return False
                
            processor_data = response.json()
            revision = processor_data.get('revision', {})
            
            # DEFINIR UNA ÚNICA CONSULTA SQL con filtro incorporado
            # Importante: Usar sólo Custom Query y dejar Table Name vacío
            consulta_filtrada = f"""/* CONSULTA PARA MES {mes} DEL {current_year} */
    SELECT 
        fecha, 
        hora, 
        alimentador,
        dia_semana, 
        potencia_activa, 
        potencia_reactiva
    FROM tension_dep 
    WHERE EXTRACT(MONTH FROM fecha) = {mes}
    AND EXTRACT(YEAR FROM fecha) = {current_year}
    ORDER BY fecha, hora, alimentador"""

            # CONFIGURACIÓN CLARA: Usar SOLO Custom Query
            payload = {
                "component": {
                    "id": query_db_processor_id,
                    "properties": {
                        "Table Name": "",  # IMPORTANTE: Vacío cuando se usa Custom Query
                        "Custom Query": consulta_filtrada,
                        "db-fetch-sql-query": consulta_filtrada,  # Para compatibilidad
                        "where.clause": "",  # IMPORTANTE: Vacío porque ya incluimos WHERE en la SQL
                        "Max Wait Time": "30 seconds"
                    }
                },
                "revision": revision
            }
            
            # Aplicar la configuración
            update_response = requests.put(
                f"{self.nifi.proxy_url}/api/processors/{query_db_processor_id}",
                json=payload,
                timeout=30
            )
            
            if update_response.status_code != 200:
                logger.error(f"Error actualizando procesador: {update_response.status_code}")
                logger.error(update_response.text[:500])
                return False
            
            logger.info(f"✅ Consulta configurada correctamente con filtro para mes {mes}/{current_year}")
            
            # Limpiar estado y reiniciar
            time.sleep(3)
            self._limpiar_estado_procesador_forzado(query_db_processor_id)
            time.sleep(3)
            
            # Iniciar procesadores
            self._iniciar_procesador_directo(query_db_processor_id)
            
            # También iniciar PutDatabaseRecord
            put_db_processor_id = self._resolver_id_procesador_put_db()
            if put_db_processor_id:
                time.sleep(2)
                self._iniciar_procesador_directo(put_db_processor_id)
            
            # Iniciar otros procesadores en orden
            otros_ids = [
                getattr(self.gestor_ids, 'convert_record_processor_id', None),
                getattr(self.gestor_ids, 'split_json_processor_id', None),
                getattr(self.gestor_ids, 'merge_content_processor_id', None),
                getattr(self.gestor_ids, 'publish_kafka_processor_id', None),
                getattr(self.gestor_ids, 'consume_kafka_processor_id', None),
                getattr(self.gestor_ids, 'evaluate_json_path_processor_id', None),
                getattr(self.gestor_ids, 'replace_text_processor_id', None)
            ]
            
            for proc_id in [p for p in otros_ids if p]:
                time.sleep(2)
                try:
                    self._iniciar_procesador_directo(proc_id)
                except:
                    pass
            
            return True
            
        except Exception as e:
            logger.error(f"Error configurando consulta filtrada: {str(e)}")
            import traceback
            traceback.print_exc()
            return False

    def restaurar_tabla_original(self):
        """Restaura la tabla original desde el backup creado"""
        try:
            if hasattr(self, 'backup_tabla') and self.backup_tabla:
                logger.info(f"🔄 Restaurando tabla original desde backup {self.backup_tabla}...")
                
                pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
                
                # Verificar que la tabla de backup existe
                backup_existe = pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.backup_tabla}')"
                )[0]
                
                if backup_existe:
                    # Eliminar tabla potencia_dep actual
                    pg_hook.run("DROP TABLE IF EXISTS potencia_dep")
                    
                    # Restaurar desde backup
                    pg_hook.run(f"ALTER TABLE {self.backup_tabla} RENAME TO potencia_dep")
                    logger.info("✅ Tabla potencia_dep restaurada desde backup")
                    return True
                else:
                    logger.error(f"❌ No se encuentra la tabla de backup {self.backup_tabla}")
            else:
                logger.warning("⚠️ No hay información de backup para restaurar")
            
            return False
        except Exception as e:
            logger.error(f"❌ Error restaurando tabla original: {str(e)}")
            return False

    def _buscar_procesadores_por_tipo(self, tipo_procesador):
        """Busca procesadores por tipo"""
        procesadores = []
        
        try:
            # Método 1: Usar búsqueda general
            response = requests.get(
                f"{self.nifi.proxy_url}/api/flow/search-results?q={tipo_procesador}",
                timeout=self.nifi.http_timeout
            )
            
            if response.status_code == 200:
                data = response.json()
                if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                    for proc in data["searchResultsDTO"]["processorResults"]:
                        procesadores.append({
                            "id": proc.get("id"),
                            "name": proc.get("name", "Desconocido"),
                            "type": tipo_procesador
                        })
        except:
            pass
            
        # Si no encontramos nada, buscar en los grupos conocidos
        if not procesadores:
            for grupo_id in [self.nifi.producer_group_id, self.nifi.consumer_group_id]:
                try:
                    # Intentar obtener lista de procesadores del grupo
                    response = requests.get(
                        f"{self.nifi.proxy_url}/api/flow/process-groups/{grupo_id}/processors",
                        timeout=self.nifi.http_timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        for proc in data.get("processors", []):
                            comp = proc.get("component", {})
                            if tipo_procesador.lower() in comp.get("type", "").lower():
                                procesadores.append({
                                    "id": comp.get("id"),
                                    "name": comp.get("name", "Desconocido"),
                                    "type": tipo_procesador
                                })
                except:
                    pass
        
        return procesadores
        
    def _iniciar_procesador_simple(self, processor_id):
        """Inicia un procesador usando el método más simple"""
        response = requests.put(
            f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
            json={"state": "RUNNING", "disconnectedNodeAcknowledged": True},
            timeout=self.nifi.http_timeout
        )
        
        return response.status_code in [200, 202]
        
    def _iniciar_procesador_con_revision(self, processor_id):
        """Inicia un procesador incluyendo revisión"""
        # Primero obtener estado actual para obtener revisión
        estado = requests.get(
            f"{self.nifi.proxy_url}/api/processors/{processor_id}",
            timeout=self.nifi.http_timeout
        )
        
        if estado.status_code == 200:
            data = estado.json()
            revision = data.get("revision", {"version": 0})
            
            response = requests.put(
                f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
                json={"revision": revision, "state": "RUNNING", "disconnectedNodeAcknowledged": True},
                timeout=self.nifi.http_timeout
            )
            
            return response.status_code in [200, 202]
        
        return False
        
    def _iniciar_procesador_alternativo(self, processor_id):
        """Inicia un procesador usando URL alternativa"""
        response = requests.put(
            f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/run-status",
            json={"state": "RUNNING", "disconnectedNodeAcknowledged": True},
            timeout=self.nifi.http_timeout
        )
        
        return response.status_code in [200, 202]
            
    def _detener_todos_procesadores(self):
        """
        Detiene todos los procesadores en ambos grupos.
        Similar a detener_procesadores_por_grupo() de dag_prueba.py
        """
        import requests
        try:
            # Detener por grupo en lugar de por IDs individuales
            for grupo_id, nombre in [
                (self.nifi.producer_group_id, "productor"),
                (self.nifi.consumer_group_id, "consumidor")
            ]:
                logger.info(f"Deteniendo procesadores del grupo {nombre}...")
                
                # Intento 1: Detener todo el grupo con una sola operación
                try:
                    response = requests.put(
                        f"{self.nifi.proxy_url}/api/flow/process-groups/{grupo_id}/stop",
                        json={"id": grupo_id, "state": "STOPPED", "disconnectedNodeAcknowledged": True},
                        timeout=self.nifi.http_timeout
                    )
                    
                    if response.status_code in [200, 202]:
                        logger.info(f"Grupo {nombre} detenido exitosamente")
                        continue
                    else:
                        logger.warning(f"No se pudo detener grupo completo: {response.status_code}")
                except Exception as e:
                    logger.warning(f"Error al detener grupo completo: {str(e)}")
                    
            return True
        except Exception as e:
            logger.error(f"Error general al detener procesadores: {str(e)}")
            return False

    

    def _iniciar_grupo_con_fallback(self, grupo_id, nombre_grupo, procesador_ids):
        """
        Versión mejorada que garantiza iniciar todos los procesadores individualmente
        """
        import time
        import requests
        
        logger.info(f"Iniciando grupo {nombre_grupo} (ID: {grupo_id})...")
        
        # Intento 1: Omitimos intentar iniciar grupo completo, vamos directo a iniciar uno por uno
        
        # Filtrar los IDs nulos
        valid_processor_ids = [pid for pid in procesador_ids if pid]
        total_count = len(valid_processor_ids)
        
        if total_count == 0:
            logger.warning(f"No hay procesadores para iniciar en el grupo {nombre_grupo}")
            return False
            
        logger.info(f"Iniciando {total_count} procesadores en grupo {nombre_grupo} uno por uno...")
        
        # IMPORTANTE: Resolución manual de todos los procesadores del grupo
        # Para resolver el problema con procesadores faltantes
        try:
            grupo_procesadores = []
            response = requests.get(
                f"{self.nifi.proxy_url}/api/process-groups/{grupo_id}/processors",
                timeout=60
            )
            
            if response.status_code == 200:
                data = response.json()
                for proc in data.get("processors", []):
                    grupo_procesadores.append(proc.get("id"))
                    
                logger.info(f"Se encontraron {len(grupo_procesadores)} procesadores en el grupo {nombre_grupo}")
                
                # Añadir los IDs que no tengamos en nuestra lista
                for proc_id in grupo_procesadores:
                    if proc_id not in valid_processor_ids and proc_id:
                        valid_processor_ids.append(proc_id)
                        logger.info(f"Añadido procesador adicional: {proc_id}")
        except:
            logger.warning("No se pudo obtener lista completa de procesadores del grupo")
        
        success_count = 0
        
        for proc_id in valid_processor_ids:
            try:
                logger.info(f"Iniciando procesador {proc_id}...")
                
                # Obtener nombre del procesador
                proc_name = "Desconocido"
                try:
                    proc_details_resp = requests.get(
                        f"{self.nifi.proxy_url}/api/processors/{proc_id}", 
                        timeout=self.nifi.http_timeout
                    )
                    if proc_details_resp.status_code == 200:
                        proc_data = proc_details_resp.json()
                        if 'component' in proc_data:
                            proc_name = proc_data['component'].get('name', "Desconocido")
                except:
                    pass
                    
                # ========================
                # MEJORA CRÍTICA: Usar varios formatos de datos y URLs
                # ========================
                urls_to_try = [
                    f"{self.nifi.proxy_url}/api/processors/{proc_id}/run-status",
                    f"{self.nifi.proxy_url}/nifi-api/processors/{proc_id}/run-status"
                ]
                
                data_formats = [
                    {"state": "RUNNING", "disconnectedNodeAcknowledged": True},
                    {"revision": {"version": 0}, "state": "RUNNING", "disconnectedNodeAcknowledged": True},
                    {"revision": {"clientId": "airflow", "version": 0}, "state": "RUNNING"}
                ]
                
                started = False
                for url in urls_to_try:
                    if started:
                        break
                        
                    for data in data_formats:
                        try:
                            response = requests.put(url, json=data, timeout=self.nifi.http_timeout)
                            if response.status_code in [200, 201, 202]:
                                logger.info(f"✅ Procesador {proc_name} ({proc_id}) iniciado con URL: {url}")
                                success_count += 1
                                started = True
                                break
                        except Exception as e:
                            logger.warning(f"Error iniciando {proc_id} con {url}: {str(e)}")
                
                # Dar tiempo entre inicios para evitar sobrecarga
                time.sleep(2)
                
            except Exception as e:
                logger.warning(f"Error general iniciando procesador {proc_id}: {str(e)}")
        
        logger.info(f"Se iniciaron {success_count}/{len(valid_processor_ids)} procesadores en {nombre_grupo}")
        return success_count > 0  # Éxito si al menos un procesador se inició
        
    def _iniciar_procesador_directo(self, processor_id):
        """Método mejorado para iniciar un procesador directamente"""
        import requests
        import time
        
        logger.info(f"Iniciando procesador {processor_id} directamente...")
        
        # Obtener nombre del procesador para mejor logging
        proc_name = "Desconocido"
        try:
            resp = requests.get(
                f"{self.nifi.proxy_url}/api/processors/{processor_id}",
                timeout=30
            )
            if resp.status_code == 200:
                data = resp.json()
                if 'component' in data:
                    proc_name = data['component'].get('name', "Desconocido")
        except:
            pass
        
        # Intentar varios formatos y URLs
        urls = [
            f"{self.nifi.proxy_url}/api/processors/{processor_id}/run-status",
            f"{self.nifi.proxy_url}/nifi-api/processors/{processor_id}/run-status",
            f"{self.nifi.proxy_url}/api/processors/{processor_id}"
        ]
        
        data_formats = [
            {"state": "RUNNING", "disconnectedNodeAcknowledged": True},
            {"revision": {"version": 0}, "state": "RUNNING", "disconnectedNodeAcknowledged": True},
            {"revision": {"clientId": "airflow", "version": 0}, "state": "RUNNING"}
        ]
        
        for url in urls:
            for data in data_formats:
                try:
                    response = requests.put(url, json=data, timeout=30)
                    if response.status_code in [200, 201, 202]:
                        logger.info(f"✅ Procesador {proc_name} ({processor_id}) iniciado correctamente")
                        return True
                except:
                    continue
        
        logger.error(f"❌ No se pudo iniciar procesador {proc_name} ({processor_id})")
        return False

    def _detener_todos_procesadores(self):
        """Detiene todos los procesadores en ambos grupos"""
        try:
            # Intento 1: Detener grupos completos
            for grupo_id, nombre in [
                (self.nifi.producer_group_id, "productor"),
                (self.nifi.consumer_group_id, "consumidor")
            ]:
                logger.info(f"Deteniendo grupo {nombre}...")
                self.nifi.detener_grupo(grupo_id)
            
            # No es necesario fallback para detener, ya que es menos crítico
            return True
        except Exception as e:
            logger.error(f"Error deteniendo procesadores: {str(e)}")
            return False

    def finalizar_procesamiento(self):
        """
        Finaliza el procesamiento deteniendo todos los procesadores
        
        Returns:
            bool: True si se completó correctamente, False en caso de error
        """
        try:
            logger.info("Finalizando procesamiento NiFi...")
            
            # Detener primero el grupo productor
            if hasattr(self.nifi, 'producer_group_id') and self.nifi.producer_group_id:
                try:
                    self.nifi.detener_grupo(self.nifi.producer_group_id)
                    logger.info("Grupo productor detenido correctamente")
                except Exception as e:
                    logger.warning(f"Error deteniendo grupo productor: {str(e)}")
            
            # Después detener el grupo consumidor
            if hasattr(self.nifi, 'consumer_group_id') and self.nifi.consumer_group_id:
                try:
                    self.nifi.detener_grupo(self.nifi.consumer_group_id)
                    logger.info("Grupo consumidor detenido correctamente")
                except Exception as e:
                    logger.warning(f"Error deteniendo grupo consumidor: {str(e)}")
            
            logger.info("Procesamiento NiFi finalizado correctamente")
            return True
        except Exception as e:
            logger.error(f"Error finalizando procesamiento: {str(e)}")
            return False


    # Función independiente para crear gestor de potencia NiFi
def crear_gestor_potencia_nifi():
    """
    Crea y configura un gestor de NiFi para procesamiento de potencia
    
    Returns:
        PotenciaNiFiManager o None si no se pudo crear
    """
    try:
        import traceback
        from modules.fabrica_nifi import crear_instancia_nifi
        from modules.gestor_ids_procesadores import GestorIdsProcesadores
        import logging
        
        logger = logging.getLogger(__name__)
        
        # Crear el conector NiFi
        nifi = crear_instancia_nifi()
        if not nifi:
            logger.error("No se pudo crear el conector NiFi")
            return None
            
        # Inicializar el gestor de IDs
        gestor_ids = GestorIdsProcesadores(nifi_utils=nifi)
        
        # Resolver todos los IDs de procesadores
        if not gestor_ids.resolver_ids_procesadores():
            logger.error("No se pudieron resolver los IDs de procesadores")
            return None
            
        # Extraer los IDs que necesitamos para el procesamiento
        query_db_id = getattr(gestor_ids, 'query_db_table_processor_id', None)
        put_db_id = getattr(gestor_ids, 'put_database_record_processor_id', None) or \
                   getattr(gestor_ids, 'put_db_processor_id', None)
        
        # Crear el gestor de potencia y guardar explícitamente los IDs
        gestor_potencia = PotenciaNiFiManager(nifi, gestor_ids)
        gestor_potencia.query_db_processor_id = query_db_id
        gestor_potencia.put_db_processor_id = put_db_id
        
        # Configurar IDs de grupos desde el nifi creado
        nifi.producer_group_id = nifi.producer_group_id or "4dcc0bed-9061-3a4c-0000-c375f77615eb"
        nifi.consumer_group_id = nifi.consumer_group_id or "723a3709-dabb-3fae-0000-2892da95b9a6"
        
        logger.info(f"Usando grupo productor: {nifi.producer_group_id}")
        logger.info(f"Usando grupo consumidor: {nifi.consumer_group_id}")
        logger.info(f"QueryDatabaseTable ID configurado: {gestor_potencia.query_db_processor_id}")
        logger.info(f"PutDatabaseRecord ID configurado: {gestor_potencia.put_db_processor_id}")
        
        return gestor_potencia
        
    except Exception as e:
        logger.error(f"Error al crear gestor NiFi: {str(e)}")
        traceback.print_exc()
        return None