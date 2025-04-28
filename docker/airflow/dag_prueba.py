import logging
import time
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Importaciones de tus módulos
from modules.fabrica_nifi import crear_instancia_nifi
from modules.process_utils import ProcessUtils

# Configuración de logging
logger = logging.getLogger(__name__)

def iniciar_flujo_secuencial():
    """
    Inicia el flujo NiFi en secuencia: primero el grupo productor completo,
    luego espera a que termine el procesamiento, y finalmente inicia el grupo consumidor.
    Esta estrategia evita conflictos entre procesadores similares como PutDatabaseRecord.
    """
    # 1. Crear instancia de NiFi
    logger.info("Creando instancia NiFi...")
    nifi = crear_instancia_nifi()
    
    if not nifi:
        logger.error("No se pudo crear la instancia NiFi")
        return False
    
    # 2. Detener todos los procesadores para empezar desde un estado limpio
    logger.info("Deteniendo todos los procesadores en ambos grupos...")
    detener_procesadores_por_grupo(nifi)
    time.sleep(10)  # Dar tiempo suficiente para que todos se detengan
    
    # 3. Crear configuración y limpiar estado
    logger.info("Configurando consulta y limpiando estados...")
    configurar_flujo_procesamiento(nifi)
    
    # 4. Iniciar grupo productor usando API directa por grupo
    logger.info("=== INICIANDO GRUPO PRODUCTOR ===")
    resultado_productor = iniciar_grupo_procesadores(nifi, nifi.producer_group_id, "productor")
    
    # 5. Esperar tiempo suficiente para que el productor procese datos
    logger.info("Esperando 30 segundos para que el grupo productor complete el procesamiento...")
    time.sleep(30)
    
    # 6. Iniciar grupo consumidor
    logger.info("=== INICIANDO GRUPO CONSUMIDOR ===")
    resultado_consumidor = iniciar_grupo_procesadores(nifi, nifi.consumer_group_id, "consumidor")
    
    # 7. Verificar estado de los procesadores
    logger.info("Verificando estado final de los procesadores...")
    verificar_estado_grupos(nifi)
    
    return resultado_productor and resultado_consumidor

def detener_procesadores_por_grupo(nifi):
    """
    Detiene todos los procesadores por grupo, sin depender de IDs específicos.
    Usa la API de búsqueda para encontrar y detener procesadores en cada grupo.
    """
    try:
        # Detener por grupo en lugar de por IDs individuales
        for grupo_id, nombre in [
            (nifi.producer_group_id, "productor"),
            (nifi.consumer_group_id, "consumidor")
        ]:
            logger.info(f"Deteniendo procesadores del grupo {nombre}...")
            
            # Usar un enfoque más directo
            try:
                # Intento 1: Detener todo el grupo con una sola operación
                response = requests.put(
                    f"{nifi.proxy_url}/api/flow/process-groups/{grupo_id}/stop",
                    json={"id": grupo_id, "state": "STOPPED"},
                    timeout=nifi.http_timeout
                )
                
                if response.status_code in [200, 202]:
                    logger.info(f"Grupo {nombre} detenido exitosamente")
                    continue
                else:
                    logger.warning(f"No se pudo detener grupo completo: {response.status_code}")
            except Exception as e:
                logger.warning(f"Error al detener grupo completo: {str(e)}")
            
            # Intento 2: Si falló el primer método, buscar procesadores individuales
            try:
                # Usar la búsqueda para encontrar procesadores en el grupo
                prefijo = "P" if nombre == "productor" else "C"
                for i in range(1, 10):  # Buscar P.1 hasta P.9 o C.1 hasta C.9
                    termino_busqueda = f"{prefijo}.{i}"
                    logger.info(f"Buscando procesadores con prefijo {termino_busqueda}...")
                    
                    response = requests.get(
                        f"{nifi.proxy_url}/api/flow/search-results?q={termino_busqueda}",
                        timeout=nifi.http_timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                            for proc in data["searchResultsDTO"]["processorResults"]:
                                proc_id = proc.get("id")
                                proc_name = proc.get("name", "Desconocido")
                                group_id = proc.get("groupId")
                                
                                # Verificar que el procesador pertenezca al grupo correcto
                                if group_id == grupo_id:
                                    nifi.stop_processor(proc_id)
                                    logger.info(f"Detenido procesador {proc_name} ({proc_id})")
                
            except Exception as e:
                logger.warning(f"Error al detener procesadores individuales: {str(e)}")
        
        return True
    except Exception as e:
        logger.error(f"Error general al detener procesadores: {str(e)}")
        return False

def configurar_flujo_procesamiento(nifi):
    """
    Configura el flujo de procesamiento con la consulta y parámetros necesarios.
    """
    try:
        # 1. Crear instancia de ProcessUtils
        process_utils = ProcessUtils(
            nifi_utils=nifi,
            tabla_base="potencia_dep_anual",
            tabla_procesada="potencia_dep_anual_processed"
        )
        
        # 2. Configurar procesamiento
        process_utils.configurar_procesamiento(
            producer_group_id=nifi.producer_group_id,
            consumer_group_id=nifi.consumer_group_id,
            query_processor_id=nifi.query_db_table_processor_id,
            publish_kafka_id=nifi.publish_kafka_processor_id,
            consume_kafka_id=nifi.consume_kafka_processor_id
        )
        
        # 3. Configurar consulta SQL para procesamiento limitado
        consulta_sql = """/* CONSULTA POTENCIA ANUAL */
        SELECT 
            fecha, 
            hora, 
            alimentador, 
            dia_semana,
            potencia_activa AS potencia_activa_real, 
            potencia_reactiva AS potencia_reactiva_real
        FROM potencia_dep_anual
        WHERE fecha >= '2019-01-01' AND fecha < '2019-01-02'
        ORDER BY fecha, hora, alimentador
        LIMIT 100
        """
        
        nifi.update_query_processor(nifi.query_db_table_processor_id, consulta_sql)
        
        return True
    except Exception as e:
        logger.error(f"Error configurando flujo: {str(e)}")
        return False

def iniciar_grupo_procesadores(nifi, grupo_id, nombre_grupo):
    """
    Inicia todos los procesadores dentro de un grupo usando un enfoque por tipo.
    """
    try:
        logger.info(f"Iniciando procesadores del grupo {nombre_grupo}...")
        
        # Buscar procesadores por tipo usando búsqueda
        tipos_procesadores = [
            # Procesadores iniciales (fuentes de datos)
            "QueryDatabaseTable", "ConsumeKafka",
            # Procesadores de transformación
            "ConvertRecord", "SplitJson", "MergeContent", 
            "EvaluateJsonPath", "ReplaceText",
            # Procesadores de salida
            "PublishKafka", "PutDatabaseRecord"
        ]
        
        # Iniciar procesadores por tipo en orden específico
        procesadores_iniciados = 0
        
        # 1. Primero reparar los procesadores especiales
        if nombre_grupo == "consumidor":
            # El PutDatabaseRecord del consumidor suele dar problemas
            procesadores_put_db = buscar_procesadores_por_tipo(nifi, "PutDatabaseRecord", grupo_id)
            for proc in procesadores_put_db:
                logger.info(f"Reparando PutDatabaseRecord en grupo {nombre_grupo}: {proc['name']}...")
                nifi.reparar_put_database_record(proc["id"], proc["name"])
        
        # 2. Iniciar procesadores en el orden correcto
        for tipo in tipos_procesadores:
            procesadores = buscar_procesadores_por_tipo(nifi, tipo, grupo_id)
            
            for proc in procesadores:
                logger.info(f"Iniciando {proc['name']} (tipo: {tipo})...")
                
                # Si es un procesador tipo fuente, limpiarlo primero
                if tipo in ["QueryDatabaseTable", "ConsumeKafka"]:
                    nifi.limpiar_estado_procesador(proc["id"])
                
                # Iniciar el procesador
                resultado = nifi.start_processor(proc["id"], proc["name"], max_retries=3)
                logger.info(f"Resultado inicio {proc['name']}: {resultado}")
                
                # Verificar estado
                time.sleep(3)
                estado = nifi.get_processor_status(proc["id"])
                estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
                
                if estado_actual == "RUNNING":
                    procesadores_iniciados += 1
                    logger.info(f"✅ {proc['name']} está en ejecución")
                else:
                    validation_errors = estado.get("component", {}).get("validationErrors", [])
                    if validation_errors:
                        logger.error(f"❌ Error en {proc['name']}: {validation_errors}")
                    else:
                        logger.warning(f"⚠️ {proc['name']} no está en ejecución (estado: {estado_actual})")
                
                # Esperar entre inicios
                time.sleep(3)
        
        logger.info(f"Se iniciaron {procesadores_iniciados} procesadores en el grupo {nombre_grupo}")
        return procesadores_iniciados > 0
    
    except Exception as e:
        logger.error(f"Error iniciando grupo de procesadores {nombre_grupo}: {str(e)}")
        return False

def buscar_procesadores_por_tipo(nifi, tipo_procesador, grupo_id):
    """
    Busca procesadores por tipo dentro de un grupo específico.
    """
    procesadores = []
    
    try:
        # Usar búsqueda general por tipo
        response = requests.get(
            f"{nifi.proxy_url}/api/flow/search-results?q={tipo_procesador}",
            timeout=nifi.http_timeout
        )
        
        if response.status_code == 200:
            data = response.json()
            if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                for proc in data["searchResultsDTO"]["processorResults"]:
                    if proc.get("groupId") == grupo_id:
                        procesadores.append({
                            "id": proc.get("id"),
                            "name": proc.get("name", "Desconocido"),
                            "type": tipo_procesador
                        })
    
    except Exception as e:
        logger.error(f"Error buscando procesadores de tipo {tipo_procesador}: {str(e)}")
    
    return procesadores

def verificar_estado_grupos(nifi):
    """
    Verifica el estado de los procesadores en ambos grupos.
    """
    resultados = {
        "productor": {"total": 0, "ejecutando": 0, "procesadores": []},
        "consumidor": {"total": 0, "ejecutando": 0, "procesadores": []}
    }
    
    try:
        for grupo_id, nombre in [
            (nifi.producer_group_id, "productor"),
            (nifi.consumer_group_id, "consumidor")
        ]:
            # Buscar procesadores en el grupo
            for prefijo in ["P", "C"]:
                for i in range(1, 10):
                    termino_busqueda = f"{prefijo}.{i}"
                    
                    response = requests.get(
                        f"{nifi.proxy_url}/api/flow/search-results?q={termino_busqueda}",
                        timeout=nifi.http_timeout
                    )
                    
                    if response.status_code == 200:
                        data = response.json()
                        if "searchResultsDTO" in data and "processorResults" in data["searchResultsDTO"]:
                            for proc in data["searchResultsDTO"]["processorResults"]:
                                if proc.get("groupId") == grupo_id:
                                    proc_id = proc.get("id")
                                    proc_name = proc.get("name", "Desconocido")
                                    
                                    resultados[nombre]["total"] += 1
                                    
                                    # Obtener estado actual
                                    estado = nifi.get_processor_status(proc_id)
                                    estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
                                    
                                    # Agregar a la lista de procesadores
                                    proc_info = {
                                        "name": proc_name,
                                        "state": estado_actual
                                    }
                                    resultados[nombre]["procesadores"].append(proc_info)
                                    
                                    if estado_actual == "RUNNING":
                                        resultados[nombre]["ejecutando"] += 1
        
        # Mostrar resumen
        logger.info("=== RESUMEN DE ESTADO DE PROCESADORES ===")
        
        for grupo, datos in resultados.items():
            logger.info(f"Grupo {grupo}: {datos['ejecutando']}/{datos['total']} procesadores en ejecución")
            
            # Mostrar procesadores ejecutando
            if datos['ejecutando'] > 0:
                logger.info(f"Procesadores ejecutando en {grupo}:")
                for proc in datos["procesadores"]:
                    if proc["state"] == "RUNNING":
                        logger.info(f"  ✅ {proc['name']}")
            
            # Mostrar procesadores no ejecutando
            if datos['ejecutando'] < datos['total']:
                logger.warning(f"Procesadores no ejecutando en {grupo}:")
                for proc in datos["procesadores"]:
                    if proc["state"] != "RUNNING":
                        logger.warning(f"  ❌ {proc['name']} (estado: {proc['state']})")
    
    except Exception as e:
        logger.error(f"Error verificando estado de procesadores: {str(e)}")

# Definición del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'iniciar_flujo_nifi',
    default_args=default_args,
    description='Inicia flujo NiFi en secuencia productor-consumidor',
    schedule_interval=None,  # Solo ejecución manual
    catchup=False
)

iniciar_flujo_task = PythonOperator(
    task_id='iniciar_flujo_secuencial',
    python_callable=iniciar_flujo_secuencial,
    dag=dag,
)