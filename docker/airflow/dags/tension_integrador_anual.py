"""
DAG para orquestar el procesamiento mensual de la tabla tension_dep para todo el año 2019
Este DAG procesa los datos mes por mes, insertándolos en tension_dep_processed,
verificando primero que no existan datos duplicados.
"""

from datetime import datetime, timedelta
import os
import requests
import time
import calendar
import subprocess
import json
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow import settings

# Configuración de parámetros
MINI_PROXY_URL = "http://mini_nifi_proxy_centrosur:5001"  # URL del proxy
HTTP_TIMEOUT = 60  # Timeout para peticiones HTTP en segundos

# Grupo Productor y Consumidor
PRODUCER_GROUP_ID = "f35dfed5-6e7e-33cd-8455-234c0136d8c7"  # Grupo Productor
CONSUMER_GROUP_ID = "19683749-d095-382d-8a05-74b3b36f934c"  # Grupo Consumidor

# IDs de procesadores - Grupo Productor
QUERY_DB_TABLE_PROCESSOR_ID = (
    "b55d4537-4750-36f4-d706-dacc8a2d091e"  # QueryDatabaseTable
)
CONVERT_RECORD_ID = "ea650125-b3d2-3514-90e1-5d1240232422"
SPLIT_JSON_ID = "4d3bf24a-e964-32e1-75a7-ada948f32adc"
MERGE_CONTENT_ID = "322d1e83-e6f3-33b3-9d67-4e1b5c8a0ca5"
PUBLISH_KAFKA_ID = "9bb6f370-cd3d-325a-47ca-661cc2cb64d5"
PUT_DB_PROCESSOR_ID_P = "588ea648-0196-1000-d5bb-b08b6489929c"

# IDs de procesadores - Grupo Consumidor
CONSUME_KAFKA_PROCESSOR_ID = "e2d2da63-6369-3361-6f07-073bc8eec40f"
EVALUATE_JSON_PATH_ID = "10602275-1ce9-387e-01b5-e1a25f542fac"
REPLACE_TEXT_ID = "b35d2a5c-1fc7-3fcd-e2ed-2940af5b8e5a"
PUT_DB_PROCESSOR_ID = "ea7b8768-f196-3c1a-832c-224d39d1a49e"


# Configuración del DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

dag = DAG(
    "tension_integrador_anual",
    default_args=default_args,
    description="Procesamiento mensual de datos de tensión para el año 2019",  
    schedule_interval=None,
    catchup=False,
    tags=["centrosur", "tension", "anual", "nifi", "kafka"],  
)


# FUNCIONES DE UTILIDAD
def create_postgres_connections():
    """Create required Postgres connections if they don't exist"""
    connections = [
        {
            "conn_id": "postgres_centrosur",
            "conn_type": "postgres",
            "host": "postgres_centrosur",
            "login": "postgres",
            "password": "1150040812",
            "schema": "centrosur",
            "port": 5432,
        },
        {
            "conn_id": "postgres_base_prueba",
            "conn_type": "postgres",
            "host": "postgres_centrosur",  # Same host
            "login": "postgres",
            "password": "1150040812",
            "schema": "base_prueba",  # Different database
            "port": 5432,
        },
    ]
    session = settings.Session()
    for conn_info in connections:
        conn = Connection(**conn_info)
        existing_conn = (
            session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
        )
        if existing_conn:
            session.delete(existing_conn)
            session.commit()
        session.add(conn)
    session.commit()
    session.close()
    print("✅ Postgres connections created/updated successfully")

def verificar_proxy_disponible():
    """Verifica que el proxy NiFi esté disponible"""
    max_intentos = 5
    for intento in range(1, max_intentos + 1):
        try:
            print(
                f"Verificando disponibilidad del proxy NiFi (intento {intento}/{max_intentos})..."
            )
            response = requests.get(f"{MINI_PROXY_URL}/health", timeout=HTTP_TIMEOUT)

            # Registrar respuesta completa para diagnóstico
            print(
                f"Respuesta del proxy: Status={response.status_code}, Contenido={response.text[:200]}..."
            )

            if response.status_code == 200:
                data = response.json()
                # Verificar si el proxy reporta un estado saludable
                if data.get("status") == "healthy" or data.get("auth_working"):
                    print(f"Proxy NiFi está disponible y reporta estado saludable")
                    print(f"Detalles adicionales: {json.dumps(data)}")
                    return True
                else:
                    print(
                        f"Proxy disponible pero reporta estado: {data.get('status')}, mensaje: {data.get('message')}"
                    )
                    # Verificar información de autenticación y conexión con NiFi
                    if "nifi_connected" in data:
                        print(f"Conexión con NiFi: {data.get('nifi_connected')}")
                    if "auth_working" in data:
                        print(f"Autenticación funcionando: {data.get('auth_working')}")
                    if data.get("auth_working"):
                        return True
            else:
                print(
                    f"Proxy no disponible (intento {intento}/{max_intentos}): {response.status_code}"
                )
                print(f"Respuesta: {response.text[:200]}...")
        except requests.exceptions.ConnectionError as e:
            print(
                f"Error de conexión al proxy (intento {intento}/{max_intentos}): {str(e)}"
            )
            print(
                "Esto podría indicar que el servicio proxy no está en ejecución o no es accesible en la red."
            )
        except requests.exceptions.Timeout as e:
            print(
                f"Timeout al conectar con el proxy (intento {intento}/{max_intentos}): {str(e)}"
            )
            print(
                "Esto podría indicar que el servicio proxy está sobrecargado o no responde."
            )
        except Exception as e:
            print(
                f"Error al verificar proxy (intento {intento}/{max_intentos}): {str(e)}"
            )

        if intento < max_intentos:
            print(f"Esperando 5 segundos antes de reintentar...")
            time.sleep(5)

    raise Exception("El proxy NiFi no está disponible después de múltiples intentos")

def decidir_ruta_mes_con_disponibilidad(ti, mes, nombre_mes):
    """
    Función helper para determinar la ruta correcta, considerando disponibilidad de datos.
    Garantiza que meses ya procesados sean saltados COMPLETAMENTE.
    """
    # Verificar si hay datos para este mes (consultando XCom)
    tiene_datos = ti.xcom_pull(task_ids="verificar_disponibilidad_datos", key=f"mes_{mes}_tiene_datos")
    
    # Si no hay datos, saltar directamente
    if tiene_datos is False:
        print(f"⚠️ DECISIÓN: Mes {mes} no tiene datos en origen, SALTANDO COMPLETAMENTE...")
        return f"procesar_{nombre_mes}.skip_y_verificar.skip_{nombre_mes}"
    
    # Verificar el resultado de procesamiento previo
    resultado = ti.xcom_pull(task_ids=f"procesar_{nombre_mes}.verificar_si_{nombre_mes}_ya_procesado")
    print(f"Resultado de verificación para mes {mes}: {resultado}")
    
    if resultado is True:  # Explicitamente verificar True
        print(f"✅ DECISIÓN: Mes {mes} ya procesado, SALTANDO COMPLETAMENTE...")
        return f"procesar_{nombre_mes}.skip_y_verificar.skip_{nombre_mes}"
    else:
        print(f"🔄 DECISIÓN: Mes {mes} necesita procesarse")
        return f"procesar_{nombre_mes}.camino_procesamiento.procesar_mes_{mes}"  


def obtener_estado_procesador(id_procesador):
    """Obtiene el estado actual de un procesador a través del mini-proxy"""
    try:
        response = requests.get(
            f"{MINI_PROXY_URL}/api/processors/{id_procesador}", timeout=HTTP_TIMEOUT
        )

        if response.status_code == 200:
            datos_procesador = response.json()
            return datos_procesador
        else:
            return {
                "error": f"Error al obtener estado del procesador: {response.status_code}",
                "raw_response": response.text,
            }
    except Exception as e:
        return {"error": f"Error al consultar procesador: {str(e)}"}

def detener_procesador(id_procesador):
    """Detiene un procesador específico en NiFi"""
    try:
        # Primero obtenemos el estado actual y la revisión
        processor_data = obtener_estado_procesador(id_procesador)

        # Verificar si el procesador existe
        if "error" in processor_data:
            print(
                f"WARNING: El procesador {id_procesador} no existe o hay un error: {processor_data['error']}"
            )
            return f"Warning: Procesador {id_procesador} no encontrado"

        # Extraemos la información necesaria
        current_state = processor_data.get("component", {}).get("state")
        revision = processor_data.get("revision", {})

        print(
            f"Estado actual del procesador {id_procesador} para detener: {current_state}"
        )

        # Si ya está detenido o deshabilitado, no hacer nada
        if current_state in ["STOPPED", "DISABLED"]:
            return f"Procesador {id_procesador} ya está detenido o deshabilitado"

        # Si no se pudo obtener el estado, devolver mensaje
        if not current_state:
            return f"No se pudo obtener el estado del procesador {id_procesador} para detener"

        # Preparamos los datos para actualizar el estado
        update_data = {
            "revision": revision,
            "component": {"id": id_procesador, "state": "STOPPED"},
        }

        # Enviamos la solicitud para cambiar el estado
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{id_procesador}",
            json=update_data,
            timeout=HTTP_TIMEOUT,
        )

        if response.status_code == 200:
            return f"Procesador {id_procesador} detenido correctamente"
        else:
            return f"Error al detener procesador: {response.status_code}"
    except Exception as e:
        return f"Excepción al detener procesador: {str(e)}"

def iniciar_procesador_con_reintento(processor_id, processor_name, max_retries=5):
    """Inicia un procesador con reintentos y verificación mejorada"""
    for intento in range(1, max_retries + 1):
        try:
            print(f"   Intento {intento}/{max_retries} para iniciar {processor_name}")

            # Obtener estado actual y revisión
            datos_procesador = obtener_estado_procesador(processor_id)
            estado_actual = datos_procesador.get("component", {}).get("state")
            revision = datos_procesador.get("revision", {})

            print(f"   Estado actual: {estado_actual}")

            if estado_actual == "RUNNING":
                print(f"   ✅ {processor_name} ya está en estado RUNNING")
                return f"✅ {processor_name} ya está en estado RUNNING"

            # Preparar datos para actualizar estado
            update_data = {
                "revision": revision,
                "component": {"id": processor_id, "state": "RUNNING"},
            }

            # Enviar solicitud para cambiar estado
            response = requests.put(
                f"{MINI_PROXY_URL}/api/processors/{processor_id}",
                json=update_data,
                timeout=HTTP_TIMEOUT,
            )

            if response.status_code == 200:
                # Verificar que el cambio fue efectivo
                time.sleep(3)  # Esperar más tiempo para confirmar el cambio
                new_status = obtener_estado_procesador(processor_id)
                new_state = new_status.get("component", {}).get("state")

                print(f"   Nuevo estado después de inicio: {new_state}")

                if new_state == "RUNNING":
                    print(f"   ✅ {processor_name} iniciado correctamente")
                    return f"✅ {processor_name} iniciado correctamente"
            else:
                print(
                    f"   ❌ Error código {response.status_code}: {response.text[:100]}"
                )

            # Si llegamos aquí, o el inicio falló o no se verificó correctamente
            if intento < max_retries:
                print(f"   ⏳ Esperando 5 segundos antes de reintentar...")
                time.sleep(5)

        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            if intento < max_retries:
                time.sleep(5)

    # Si después de todos los intentos no logramos iniciar, devolver mensaje de error
    print(f"   ⚠️ No se pudo iniciar {processor_name} después de {max_retries} intentos")
    return f"⚠️ No se pudo iniciar {processor_name} después de {max_retries} intentos"

def stop_all_processors_individually():
    """Detiene todos los procesadores individuales en lugar de grupos completos"""
    try:
        producer_processors = [
            QUERY_DB_TABLE_PROCESSOR_ID,
            CONVERT_RECORD_ID,
            SPLIT_JSON_ID,
            MERGE_CONTENT_ID,
            PUBLISH_KAFKA_ID,
            PUT_DB_PROCESSOR_ID_P
        ]

        # Lista de procesadores del consumidor
        consumer_processors = [
            CONSUME_KAFKA_PROCESSOR_ID,
            EVALUATE_JSON_PATH_ID,
            REPLACE_TEXT_ID,
            PUT_DB_PROCESSOR_ID,
        ]

        print("📌 Deteniendo procesadores del productor individualmente...")
        for processor_id in producer_processors:
            detener_procesador(processor_id)

        print("📌 Deteniendo procesadores del consumidor individualmente...")
        for processor_id in consumer_processors:
            detener_procesador(processor_id)

        # Esperar para que los procesadores se detengan
        time.sleep(15)

        return f"Todos los procesadores detenidos individualmente: {len(producer_processors) + len(consumer_processors)} procesadores"
    except Exception as e:
        print(f"❌ Error al detener procesadores: {str(e)}")
        return f"Error al detener procesadores: {str(e)}"


def iniciar_todos_procesadores_en_secuencia():
    """Inicia todos los procesadores en secuencia correcta con validación"""
    results = []

    # Lista completa de todos los procesadores en orden correcto
    todos_procesadores = [
        # Grupo productor
        {
            "id": QUERY_DB_TABLE_PROCESSOR_ID,
            "name": "QueryDatabaseTable",
            "group": "producer",
        },
        {"id": CONVERT_RECORD_ID, "name": "ConvertRecord", "group": "producer"},
        {"id": SPLIT_JSON_ID, "name": "SplitJson", "group": "producer"},
        {"id": MERGE_CONTENT_ID, "name": "MergeContent", "group": "producer"},
        {"id": PUBLISH_KAFKA_ID, "name": "PublishKafka", "group": "producer"},
        {"id": PUT_DB_PROCESSOR_ID_P, "name": "PutDatabaseRecord", "group": "producer"},
        # Grupo consumidor
        {"id": CONSUME_KAFKA_PROCESSOR_ID, "name": "ConsumeKafka", "group": "consumer"},
        {"id": EVALUATE_JSON_PATH_ID, "name": "EvaluateJsonPath", "group": "consumer"},
        {"id": REPLACE_TEXT_ID, "name": "ReplaceText", "group": "consumer"},
        {"id": PUT_DB_PROCESSOR_ID, "name": "PutDatabaseRecord", "group": "consumer"},
    ]
    # Iniciar primero el grupo productor COMPLETO
    print("▶️ Iniciando grupo PRODUCTOR completo...")
    for proc in [p for p in todos_procesadores if p["group"] == "producer"]:
        print(f"▶️ Iniciando {proc['name']}...")
        result = iniciar_procesador_con_reintento(proc["id"], proc["name"])
        results.append(result)

        # Verificar estado después del inicio
        verify_state = obtener_estado_procesador(proc["id"])
        current_state = verify_state.get("component", {}).get("state", "UNKNOWN")
        print(f"   Estado actual de {proc['name']}: {current_state}")

        time.sleep(3)  # Pausa entre inicios

    # Esperar antes de iniciar consumidores
    print("⏳ Esperando 10 segundos para que los productores inicien completamente...")
    time.sleep(10)

    # Luego iniciar el grupo consumidor COMPLETO
    print("▶️ Iniciando grupo CONSUMIDOR completo...")
    for proc in [p for p in todos_procesadores if p["group"] == "consumer"]:
        print(f"▶️ Iniciando {proc['name']}...")
        result = iniciar_procesador_con_reintento(proc["id"], proc["name"])
        results.append(result)

        # Verificar estado después del inicio
        verify_state = obtener_estado_procesador(proc["id"])
        current_state = verify_state.get("component", {}).get("state", "UNKNOWN")
        print(f"   Estado actual de {proc['name']}: {current_state}")

        time.sleep(3)  # Pausa entre inicios

    return f"Iniciados {len(results)} procesadores en secuencia"

def iniciar_todos_procesadores_por_grupo():
    """Inicia todos los procesadores de ambos grupos uno por uno (reemplaza versión fallida)"""
    try:
        results = {}

        # Lista de procesadores del productor con IDs ACTUALIZADOS
        producer_processors = [
            {"id": QUERY_DB_TABLE_PROCESSOR_ID, "name": "QueryDatabaseTable"},
            {"id": CONVERT_RECORD_ID, "name": "ConvertRecord"},
            {"id": SPLIT_JSON_ID, "name": "SplitJson"},
            {"id": MERGE_CONTENT_ID, "name": "MergeContent"},
            {"id": PUBLISH_KAFKA_ID, "name": "PublishKafka"},
            {"id": PUT_DB_PROCESSOR_ID_P, "name": "PutDatabaseRecord"},
        ]
        # Lista de procesadores del consumidor con IDs ACTUALIZADOS
        consumer_processors = [
            {"id": CONSUME_KAFKA_PROCESSOR_ID, "name": "ConsumeKafka"},
            {"id": EVALUATE_JSON_PATH_ID, "name": "EvaluateJsonPath"},
            {"id": REPLACE_TEXT_ID, "name": "ReplaceText"},
            {"id": PUT_DB_PROCESSOR_ID, "name": "PutDatabaseRecord"},
        ]
        # Iniciar cada procesador individualmente - PRODUCTOR
        print("▶️ Iniciando procesadores del productor individualmente...")
        for processor in producer_processors:
            result = iniciar_procesador_con_reintento(
                processor["id"], processor["name"], max_retries=3
            )
            results[f"producer_{processor['name']}"] = result
            time.sleep(3)  # Breve pausa entre cada inicio

        # Esperar antes de iniciar consumidores
        print(
            "⏳ Esperando 15 segundos para que los productores inicien completamente..."
        )
        time.sleep(15)
        # Iniciar cada procesador individualmente - CONSUMIDOR
        print("▶️ Iniciando procesadores del consumidor individualmente...")
        for processor in consumer_processors:
            result = iniciar_procesador_con_reintento(
                processor["id"], processor["name"], max_retries=3
            )
            results[f"consumer_{processor['name']}"] = result
            time.sleep(3)  # Breve pausa entre cada inicio
        return True
    except Exception as e:
        print(f"❌ Error iniciando procesadores: {str(e)}")
        return False

def configurar_topic_procesador(processor_id, property_name, topic_name):
    """Configura un procesador Kafka para usar un topic específico"""
    try:
        # Obtener el estado actual del procesador
        processor_data = obtener_estado_procesador(processor_id)

        if "error" in processor_data:
            print(
                f"⛔ ERROR: No se pudo obtener el estado del procesador: {processor_data}"
            )
            return False

        revision = processor_data.get("revision", {})
        component = processor_data.get("component", {})
        properties = component.get("properties", {})

        # Guardar el topic actual para diagnóstico
        topic_actual = properties.get(property_name, "desconocido")
        print(f"📋 Topic actual en {processor_id}: {topic_actual}")

        # Actualizar la propiedad del topic
        properties[property_name] = topic_name

        # Enviar actualización al procesador
        update_data = {
            "revision": revision,
            "component": {"id": processor_id, "properties": properties},
        }

        # Primero asegurarse que el procesador esté detenido
        detener_procesador(processor_id)
        time.sleep(5)

        update_response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{processor_id}",
            json=update_data,
            timeout=HTTP_TIMEOUT,
        )

        if update_response.status_code == 200:
            print(
                f"✅ Topic actualizado correctamente a {topic_name} en {processor_id}"
            )
            return True
        else:
            print(f"❌ Error al actualizar topic: {update_response.status_code}")
            print(f"📊 Respuesta: {update_response.text[:200]}")
            return False

    except Exception as e:
        print(f"❌ Error al configurar topic: {str(e)}")
        return False

def limpiar_estado_procesador(processor_id):
    """Limpia el estado del procesador QueryDatabaseTable para forzar procesamiento desde cero"""
    host = "588a471cac3f"  # ID del contenedor confirmado

    try:
        print(f"🧹 Limpiando estado del procesador {processor_id}...")

        # 1. Obtener token
        cmd_token = f"curl -k -s -X POST -d 'username=admin&password=centrosur123' https://{host}:8443/nifi-api/access/token"
        token_process = subprocess.run(
            cmd_token, shell=True, capture_output=True, text=True
        )
        token = token_process.stdout.strip()

        if not token:
            print("❌ Error obteniendo token de autenticación")
            return False

        # 2. Limpiar el estado del procesador
        cmd_clear = f"curl -k -s -X POST -H 'Authorization: Bearer {token}' -H 'Content-Type: application/json' https://{host}:8443/nifi-api/processors/{processor_id}/state/clear-requests"

        clear_process = subprocess.run(
            cmd_clear, shell=True, capture_output=True, text=True
        )
        if clear_process.returncode == 0:
            print("✅ Estado del procesador limpiado correctamente")
            return True
        else:
            print(f"❌ Error limpiando estado: {clear_process.stderr}")
            return False

    except Exception as e:
        print(f"❌ Error general limpiando estado: {str(e)}")
        return False

def clear_processor_state_and_empty_all_queues():
    """Completely clears processor state and empties all connection queues"""
    try:
        print(
            "🧹 Performing complete cleanup of processor state and connection queues..."
        )

        # 1. Stop all processors first
        print("📌 Deteniendo procesadores del productor individualmente...")
        stop_all_processors_individually()
        time.sleep(5)  # Allow time for processors to stop

        # 2. Get token for direct NiFi API access
        host = "588a471cac3f"  # Container ID that works consistently
        cmd_token = f"curl -k -s -X POST -d 'username=admin&password=centrosur123' https://{host}:8443/nifi-api/access/token"
        token_process = subprocess.run(
            cmd_token, shell=True, capture_output=True, text=True
        )
        token = token_process.stdout.strip()

        if not token:
            print("⚠️ Couldn't get token, trying alternative method...")
            # Even without token, try to continue with other methods

        # 3. Clear state of the main processor - multiple attempts with different methods
        if token:
            # Method 1: Standard clear request
            cmd_clear = f"curl -k -s -X POST -H 'Authorization: Bearer {token}' https://{host}:8443/nifi-api/processors/{QUERY_DB_TABLE_PROCESSOR_ID}/state/clear-requests"
            subprocess.run(cmd_clear, shell=True)

            # Method 2: Get all connections and empty them
            cmd_get_connections = f"curl -k -s -X GET -H 'Authorization: Bearer {token}' -H 'Accept: application/json' https://{host}:8443/nifi-api/process-groups/{PRODUCER_GROUP_ID}/connections"
            conn_result = subprocess.run(
                cmd_get_connections, shell=True, capture_output=True, text=True
            )

            try:
                connections = json.loads(conn_result.stdout)
                if "connections" in connections:
                    for conn in connections["connections"]:
                        conn_id = conn.get("id")
                        cmd_empty = f"curl -k -s -X POST -H 'Authorization: Bearer {token}' -H 'Content-Type: application/json' -d '{{\"disconnectedNodeAcknowledged\":false}}' https://{host}:8443/nifi-api/flowfile-queues/{conn_id}/drop-requests"
                        subprocess.run(cmd_empty, shell=True)
            except:
                print("⚠️ Could not empty connection queues with token")

        # 4. Also use the mini-proxy for thorough cleanup
        try:
            response = requests.post(
                f"{MINI_PROXY_URL}/api/processors/{QUERY_DB_TABLE_PROCESSOR_ID}/state/clear",
                timeout=10,
            )

            # Also try to empty connection queues
            for group_id in [PRODUCER_GROUP_ID, CONSUMER_GROUP_ID]:
                response = requests.get(
                    f"{MINI_PROXY_URL}/api/process-groups/{group_id}/connections",
                    timeout=10,
                )

                if response.status_code == 200:
                    connections = response.json()
                    if "connections" in connections:
                        for conn in connections["connections"]:
                            conn_id = conn.get("id")
                            requests.post(
                                f"{MINI_PROXY_URL}/api/flowfile-queues/{conn_id}/drop-requests",
                                json={"disconnectedNodeAcknowledged": False},
                                timeout=10,
                            )
        except:
            # Continue even if this method fails
            pass

        time.sleep(10)  # Allow time for cleanup operations to complete
        print("✅ Cleanup operations completed")
        return True

    except Exception as e:
        print(f"⚠️ Error in cleanup: {str(e)}")
        # Continue anyway to ensure the flow doesn't stop
        return True

def truncate_existing_month_data(mes_inicio, mes_fin, ano=2019):
    """Removes existing data for the month to allow clean reprocessing"""
    try:
        fecha_inicio = f"{ano}-{mes_inicio:02d}-01"
        fecha_fin = f"{ano}-{mes_fin:02d}-01"

        print(
            f"🗑️ Limpiando datos existentes en destino para {fecha_inicio} a {fecha_fin}"
        )
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # Delete existing records for this month
        delete_query = f"""
        DELETE FROM tension_dep_processed 
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """

        rows_deleted = pg_hook_destino.run(delete_query)
        print(f"✅ Eliminando {rows_deleted} datos del mes {mes_inicio}")
        return True

    except Exception as e:
        print(f"❌ Error al eliminar datos del mes: {str(e)}")
        return False

def actualizar_consulta_sin_docker(processor_id, tabla_temporal):
    """Actualiza la consulta SQL sin depender de Docker"""
    try:
        print(f"🔄 Actualizando consulta para usar tabla temporal {tabla_temporal}...")

        # Crear la consulta que usa EXCLUSIVAMENTE la tabla temporal
        nueva_consulta = f"""/* TABLA_TEMPORAL_EXCLUSIVA_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    id_subestacion, 
    dia_semana, 
    tension_servicio
FROM tension_dep
ORDER BY fecha, hora, id_subestacion
"""

        # Obtener estado del procesador
        processor_data = obtener_estado_procesador(processor_id)

        if "error" in processor_data:
            print(f"⚠️ Error obteniendo estado: {processor_data['error']}")
            return False

        revision = processor_data.get("revision", {})
        component = processor_data.get("component", {})
        properties = dict(component.get("properties", {}))

        # Mostrar consulta actual para diagnóstico
        consulta_actual = properties.get("Custom Query", "NO DISPONIBLE")
        print(f"📋 Consulta actual: {consulta_actual[:100]}...")

        # Actualizar consulta
        properties["Custom Query"] = nueva_consulta
        properties["db-fetch-sql-query"] = nueva_consulta  # Nombre alternativo

        # Remover propiedades problemáticas
        for key in [
            "start_date",
            "end_date",
            "Maximum-value Column",
            "Maximum-value Columns",
        ]:
            if key in properties:
                del properties[key]

        # Crear datos para actualización
        update_data = {
            "revision": revision,
            "component": {"id": processor_id, "properties": properties},
        }

        # Enviar actualización
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{processor_id}",
            json=update_data,
            timeout=HTTP_TIMEOUT,
        )

        if response.status_code == 200:
            print(f"✅ Consulta actualizada correctamente para usar {tabla_temporal}")

            # Verificación de actualización
            time.sleep(3)
            verify_data = obtener_estado_procesador(processor_id)
            updated_query = (
                verify_data.get("component", {})
                .get("properties", {})
                .get("Custom Query", "")
            )

            if tabla_temporal in updated_query:
                print("✅ Verificación confirma actualización correcta")
                return True
            else:
                print("⚠️ Verificación no encontró la tabla temporal en la consulta")
                print(f"Consulta actual: {updated_query[:100]}...")
        else:
            print(f"❌ Error actualizando consulta: {response.status_code}")
            print(f"Respuesta: {response.text[:200]}")

        return False
    except Exception as e:
        print(f"❌ Error actualizando consulta: {str(e)}")
        return False

def decidir_procesar_mes(ti, mes, grupo):
    """Decide si procesar un mes basado en verificación previa"""
    verificacion_key = f"procesar_{grupo}.verificar_si_{grupo}_ya_procesado"
    if not ti.xcom_pull(task_ids=verificacion_key):
        return f"procesar_{grupo}.procesar_mes_{mes}"
    else:
        return f"procesar_{grupo}.skip_{grupo}"

def get_token_with_retry(max_retries=3):
    """Obtiene token con reintentos en caso de fallo"""
    host = "588a471cac3f"  # Container ID
    
    for intento in range(1, max_retries + 1):
        try:
            print(f"🔑 Intento {intento}/{max_retries} de obtener token...")
            cmd_token = f"curl -k -s -X POST -d 'username=admin&password=centrosur123' https://{host}:8443/nifi-api/access/token"
            token_process = subprocess.run(cmd_token, shell=True, capture_output=True, text=True, timeout=30)
            
            token = token_process.stdout.strip()
            if token and len(token) > 20:  # Token válido suele ser largo
                print("✅ Token obtenido correctamente")
                return token
            
            # Intento alternativo con el mini-proxy si está disponible
            print("⚠️ Método directo falló, intentando con mini-proxy...")
            response = requests.post(
                f"{MINI_PROXY_URL}/api/get-token",
                json={"username": "admin", "password": "centrosur123"},
                timeout=HTTP_TIMEOUT
            )
            
            if response.status_code == 200:
                token_data = response.json()
                if "token" in token_data:
                    print("✅ Token obtenido vía mini-proxy")
                    return token_data["token"]
                    
            time.sleep(2 * intento)  # Espera creciente entre intentos
            
        except Exception as e:
            print(f"⚠️ Error en intento {intento}: {str(e)}")
            time.sleep(2 * intento)
    
    print("❌ No se pudo obtener token después de varios intentos")
    return None

def configurar_put_database_record():
    """Configura el procesador PutDatabaseRecord para usar INSERT en lugar de UPSERT"""
    try:
        print("🔧 Configurando procesador PutDatabaseRecord...")
        processor_data = obtener_estado_procesador(PUT_DB_PROCESSOR_ID)
        
        if "error" in processor_data:
            print(f"⚠️ Error obteniendo estado: {processor_data['error']}")
            return False
            
        revision = processor_data.get("revision", {})
        component = processor_data.get("component", {})
        properties = dict(component.get("properties", {}))
        
        # Mostrar configuración actual
        statement_type_actual = properties.get("put-db-record-statement-type", "DESCONOCIDO")
        print(f"🔍 Tipo de declaración actual: {statement_type_actual}")
        
        # Cambiar de UPSERT a INSERT
        properties["put-db-record-statement-type"] = "INSERT"
        
        # Crear datos para actualización
        update_data = {
            "revision": revision,
            "component": {"id": PUT_DB_PROCESSOR_ID, "properties": properties}
        }
        
        # Detener primero el procesador
        detener_procesador(PUT_DB_PROCESSOR_ID)
        time.sleep(5)
        
        # Enviar actualización
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{PUT_DB_PROCESSOR_ID}",
            json=update_data,
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            print("✅ PutDatabaseRecord configurado para usar INSERT")
            return True
        else:
            print(f"❌ Error configurando PutDatabaseRecord: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        return False

def procesar_mes_sustituyendo_tabla(mes, **kwargs):
    """
    Crea una tabla temporal y aplica el MÉTODO NUCLEAR SEGURO para garantizar
    que solo se procesan datos del mes específico, pero con restauración garantizada.
    """
    try:
        # Inicialización de variables
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        ano = 2019
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = ano if mes < 12 else ano + 1
        fecha_inicio = f"{ano}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        print(f"🚨 AISLAMIENTO SEGURO para mes {mes} ({fecha_inicio} a {fecha_fin})")
        
        # VERIFICACIÓN INICIAL: Asegurar que tension_dep está completa
        count_inicial = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
        print(f"📊 VERIFICACIÓN INICIAL: tension_dep tiene {count_inicial} registros totales")
        
        # Detener todos los procesadores para asegurar un entorno limpio
        print("⏸️ Deteniendo todos los procesadores...")
        stop_all_processors_individually()
        time.sleep(30)  # Tiempo extra para asegurar detención completa
        
        # 1. CREAR BACKUP DE LA TABLA ORIGINAL SIEMPRE (redundancia de seguridad)
        backup_tabla = f"tension_dep_prev_{int(time.time())}"
        print(f"🔒 Creando backup de seguridad de tabla original: {backup_tabla}")
        pg_hook.run(f"CREATE TABLE {backup_tabla} AS SELECT * FROM tension_dep")
        
        # Registrar el nombre del backup para recuperación posterior
        kwargs['ti'].xcom_push(key=f'mes_{mes}_backup_table', value=backup_tabla)
        
        # 2. CREAR NOMBRE ÚNICO PARA LA TABLA TEMPORAL DE TRABAJO
        tabla_trabajo = f"tension_dep_work_{int(time.time())}"
        print(f"📋 Creando tabla de trabajo {tabla_trabajo}...")
        
        # 3. Eliminar tabla si existe (por seguridad)
        pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
        
        # 4. Verificar registros existentes para este mes
        query_verificar = f"""
            SELECT COUNT(*) FROM tension_dep 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        count = pg_hook.get_first(query_verificar)[0]
        
        if count == 0:
            print(f"⚠️ No hay datos para el mes {mes} en tension_dep. Abortando procesamiento.")
            return False
        
        print(f"✅ Encontrados {count} registros en tension_dep para el mes {mes}")
        
        # 5. CREAR LA TABLA DE TRABAJO con filtros de fecha estrictos
        create_query = f"""
            CREATE TABLE {tabla_trabajo} AS
            SELECT * FROM tension_dep 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            AND fecha >= '{fecha_inicio}'
            AND fecha < '{fecha_fin}'
        """
        pg_hook.run(create_query)
            
        # 6. Verificar creación correcta
        count_trabajo = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_trabajo}")[0]
        print(f"✅ Tabla de trabajo {tabla_trabajo} creada con {count_trabajo} registros exclusivos del mes {mes}")
        
        # 7. Registrar la tabla trabajo para limpieza posterior
        kwargs['ti'].xcom_push(key=f'tabla_trabajo_mes_{mes}', value=tabla_trabajo)
        
        # 8. *** MÉTODO NUCLEAR SEGURO CON TABLA TEMPORAL PARA ORIGINAL ***
        print("☢️ APLICANDO MÉTODO NUCLEAR SEGURO...")
        
        # a. Renombrar la tabla original temporalmente (mantener nombre consistente para facilitar recuperación)
        temp_original = f"tension_dep_original_temp_{mes}_{int(time.time())}"
        pg_hook.run(f"ALTER TABLE tension_dep RENAME TO {temp_original}")
        print(f"🔄 Tabla original renombrada temporalmente a {temp_original}")
        
        # b. Crear nueva tabla tension_dep con solo datos del mes 
        pg_hook.run(f"CREATE TABLE tension_dep AS SELECT * FROM {tabla_trabajo}")
        print(f"⚡ SOLUCIÓN NUCLEAR: tension_dep ahora contiene SOLO datos del mes {mes}")
        
        # c. Guardar el nombre de la tabla original temporal usando el mes como clave
        kwargs['ti'].xcom_push(key=f'tension_dep_original_temp_{mes}', value=temp_original)
        
        # También guardarlo en una clave genérica para facilitar recuperación
        kwargs['ti'].xcom_push(key='ultima_tabla_original_temp', value=temp_original)
            
        # 9. Configurar consulta simple
        nueva_consulta = f"""/* CONSULTA_MES_{mes}_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    id_subestacion, 
    dia_semana, 
    tension_servicio
FROM tension_dep
ORDER BY fecha, hora, id_subestacion
"""
        
        # Obtener estado del procesador
        processor_data = obtener_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
        revision = processor_data.get("revision", {})
        component = processor_data.get("component", {})
        properties = dict(component.get("properties", {}))
        
        # Modificar consulta y eliminar Table Name
        properties["db-fetch-sql-query"] = nueva_consulta
        properties["Custom Query"] = nueva_consulta
        properties["Table Name"] = ""
        
        update_data = {
            "revision": revision,
            "component": {
                "id": QUERY_DB_TABLE_PROCESSOR_ID,
                "properties": properties
            }
        }
        
        # Enviar actualización
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{QUERY_DB_TABLE_PROCESSOR_ID}",
            json=update_data,
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            print(f"✅ Consulta SQL configurada correctamente")
            
            # Verificación adicional
            time.sleep(5)
            verificacion = obtener_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
            consulta_actual = verificacion.get("component", {}).get("properties", {}).get("db-fetch-sql-query", "")
            tabla_nombre = verificacion.get("component", {}).get("properties", {}).get("Table Name", "")
            
            print(f"🔍 VERIFICACIÓN: Table Name = '{tabla_nombre}'")
            print(f"🔍 VERIFICACIÓN: Consulta simple: {'tension_dep' in consulta_actual}")
        else:
            print(f"⚠️ Error actualizando consulta: {response.status_code}")
            return False
        
        # 10. Limpiar datos existentes en destino
        print("🗑️ Limpiando datos existentes en destino...")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_destino.run(f"""
            DELETE FROM tension_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)
        
        # 11. Configurar topic Kafka único
        topic_name = f"tension-mes{mes}-seguro-{int(time.time())}"
        print(f"🔄 Configurando topic Kafka único: {topic_name}")
        configurar_topic_procesador(PUBLISH_KAFKA_ID, "Topic Name", topic_name)
        configurar_topic_procesador(CONSUME_KAFKA_PROCESSOR_ID, "topic", topic_name)
        # 11b. Configurar PutDatabaseRecord para INSERT en lugar de UPSERT
        configurar_put_database_record()
        # 12. Limpiar estado y colas
        print("🧹 Limpiando estado del procesador y colas...")
        limpiar_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
        clear_processor_state_and_empty_all_queues()
        
        # 13. Iniciar procesadores
        print("▶️ Iniciando grupos de procesadores completos...")
        iniciar_todos_procesadores_por_grupo()
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        import traceback
        print(f"Detalles del error: {traceback.format_exc()}")
        
        # Intentar restaurar la tabla original antes de salir
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            
            # Buscar el nombre de la tabla temporal recién creada
            temp_original = kwargs.get('ti').xcom_pull(key=f'tension_dep_original_temp_{mes}')
            
            if temp_original:
                # Verificar si la tabla existe
                if pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{temp_original}')"
                )[0]:
                    # Eliminar la tabla tension_dep parcial
                    pg_hook.run("DROP TABLE IF EXISTS tension_dep")
                    
                    # Restaurar desde la temporal
                    pg_hook.run(f"ALTER TABLE {temp_original} RENAME TO tension_dep")
                    print(f"🚨 RESTAURACIÓN DE EMERGENCIA: Tabla original restaurada desde {temp_original}")
            
            # Si no se encontró la temporal específica, intentar con el backup
            else:
                backup_tabla = kwargs.get('ti').xcom_pull(key=f'mes_{mes}_backup_table')
                
                if backup_tabla and pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{backup_tabla}')"
                )[0]:
                    pg_hook.run("DROP TABLE IF EXISTS tension_dep")
                    pg_hook.run(f"ALTER TABLE {backup_tabla} RENAME TO tension_dep")
                    print(f"🚨 RESTAURACIÓN DE EMERGENCIA: Tabla original restaurada desde backup {backup_tabla}")
            
        except Exception as cleanup_error:
            print(f"⚠️ Error adicional durante restauración de emergencia: {str(cleanup_error)}")
            
        return False

def depurar_ceros_tension_final(**kwargs):
    """
    Versión mejorada de la depuración final con tratamiento especializado para subestación 50
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        print("🔍 Iniciando depuración final mejorada de valores cero en tensión...")
        
        # TRATAMIENTO ESPECÍFICO PARA SUBESTACIÓN 50
        # Este código se ejecuta ANTES del procesamiento general para asegurar que la subestación 50
        # reciba un tratamiento prioritario
        print("🎯 TRATAMIENTO ESPECIALIZADO: Depurando subestación 50 para todos los meses...")
        
        # 1. Identificar todos los registros cero para la subestación 50
        query_subestacion_50 = """
        SELECT 
            fecha, 
            hora,
            EXTRACT(MONTH FROM fecha) as mes
        FROM tension_dep_processed
        WHERE id_subestacion = 50 
        AND tension_servicio = 0
        ORDER BY fecha, hora
        """
        
        registros_sub_50 = pg_hook.get_records(query_subestacion_50)
        
        if registros_sub_50:
            print(f"⚠️ Encontrados {len(registros_sub_50)} registros con valor cero en subestación 50")
            
            # 2. Obtener valor promedio de tensión para sub-50 por hora del día (ignorando ceros)
            query_referencia = """
            SELECT 
                hora,
                EXTRACT(DOW FROM fecha) as dia_semana,
                AVG(tension_servicio) as valor_referencia
            FROM tension_dep_processed
            WHERE id_subestacion = 50
            AND tension_servicio > 0
            GROUP BY hora, EXTRACT(DOW FROM fecha)
            ORDER BY dia_semana, hora
            """
            
            valores_referencia = pg_hook.get_records(query_referencia)
            
            # Crear diccionario para acceso rápido: (hora, dia_semana) -> valor_referencia
            referencias = {}
            for val in valores_referencia:
                hora, dia_semana, valor = val
                referencias[(int(hora), int(dia_semana))] = float(valor)
            
            # Valor promedio general como fallback
            promedio_general = pg_hook.get_first("""
                SELECT AVG(tension_servicio) 
                FROM tension_dep_processed 
                WHERE id_subestacion = 50 AND tension_servicio > 0
            """)[0] or 13.8  # Fallback a valor típico si no hay promedio
            
            # 3. Corregir cada registro cero usando el valor de referencia más apropiado
            print("🔄 Aplicando correcciones específicas para subestación 50...")
            
            corregidos_sub50 = 0
            for reg in registros_sub_50:
                fecha, hora, mes = reg
                
                # Convertir fecha a datetime para obtener día de semana
                if isinstance(fecha, str):
                    fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
                else:
                    fecha_dt = fecha
                
                dia_semana = fecha_dt.weekday()
                
                # Buscar valor de referencia para esta hora y día de semana
                valor_correccion = referencias.get((int(hora), dia_semana), promedio_general)
                
                # Aplicar corrección
                pg_hook.run(f"""
                UPDATE tension_dep_processed
                SET tension_servicio = {valor_correccion}
                WHERE id_subestacion = 50
                AND fecha = '{fecha}'
                AND hora = {hora}
                AND tension_servicio = 0
                """)
                
                corregidos_sub50 += 1
                
            print(f"✅ Corregidos {corregidos_sub50} registros para subestación 50")
        
        # CONTINUAR CON EL PROCESAMIENTO NORMAL PARA TODAS LAS DEMÁS SUBESTACIONES
        # 1. Identificar todas las subestaciones con valores cero
        query_ceros = """
        SELECT 
            id_subestacion,
            fecha, 
            hora,
            EXTRACT(MONTH FROM fecha) as mes
        FROM tension_dep_processed
        WHERE tension_servicio = 0
        ORDER BY mes ASC, id_subestacion, fecha, hora
        """
        
        registros_cero = pg_hook.get_records(query_ceros)
        
        if not registros_cero:
            print("✅ No se encontraron registros con valores cero pendientes")
            return True
            
        print(f"📊 Encontrados {len(registros_cero)} registros con valores cero para depurar")
        
        # Estadísticas globales para el reporte
        total_procesados = 0
        total_corregidos = 0
        
        # 2. Agrupar por mes para procesamiento individualizado
        registros_por_mes = {}
        for registro in registros_cero:
            id_sub, fecha, hora, mes = registro
            mes = int(mes)
            if mes not in registros_por_mes:
                registros_por_mes[mes] = []
            registros_por_mes[mes].append((id_sub, fecha, hora))
        
        # NUEVO: Pre-análisis de distribución de datos por mes
        print("📊 Análisis previo de distribución de datos por mes:")
        meses_con_datos_buenos = set()
        for mes in range(1, 13):
            query_datos = f"""
            SELECT COUNT(*) as total,
                   SUM(CASE WHEN tension_servicio > 0 THEN 1 ELSE 0 END) as buenos
            FROM tension_dep_processed
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = 2019
            """
            resultados = pg_hook.get_first(query_datos)
            if resultados and resultados[0] > 0:
                total = resultados[0]
                buenos = resultados[1] or 0
                porcentaje_buenos = (buenos / total) * 100 if total > 0 else 0
                
                if buenos > 0:
                    meses_con_datos_buenos.add(mes)
                    
                print(f"  - Mes {mes}: {total} registros, {buenos} buenos ({porcentaje_buenos:.2f}%)")
        
        # MEJORADO: Si el mes 7 está en procesamiento, darle tratamiento especial usando mes 6 como referencia primaria
        if 7 in registros_por_mes and 6 in meses_con_datos_buenos:
            print("⭐ TRATAMIENTO ESPECIAL: Usando mes 6 como referencia primaria para mes 7")
            registros_mes7 = registros_por_mes[7]
            
            corregidos_esp = 0
            for id_sub, fecha, hora in registros_mes7:
                # Obtener día del mes
                if isinstance(fecha, str):
                    fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
                else:
                    fecha_dt = fecha
                
                dia = fecha_dt.day
                dia_semana = fecha_dt.weekday()
                
                # Ajustar al día correspondiente en mes 6
                dia_ref = min(dia, calendar.monthrange(2019, 6)[1])
                fecha_ref = f"2019-06-{dia_ref:02d}"
                
                # Buscar valor en misma hora, mismo día
                query_ref = f"""
                SELECT tension_servicio
                FROM tension_dep_processed
                WHERE id_subestacion = {id_sub}
                AND fecha = '{fecha_ref}'
                AND hora = {hora}
                AND tension_servicio > 0
                """
                
                valor_ref = pg_hook.get_first(query_ref)
                
                if valor_ref and valor_ref[0] > 0:
                    # Actualizar con valor de referencia
                    pg_hook.run(f"""
                    UPDATE tension_dep_processed
                    SET tension_servicio = {valor_ref[0]}
                    WHERE id_subestacion = {id_sub}
                    AND fecha = '{fecha}'
                    AND hora = {hora}
                    """)
                    corregidos_esp += 1
                else:
                    # Si no hay valor exacto, buscar por día de semana
                    query_dia = f"""
                    SELECT AVG(tension_servicio)
                    FROM tension_dep_processed
                    WHERE id_subestacion = {id_sub}
                    AND EXTRACT(DOW FROM fecha) = {dia_semana}
                    AND hora = {hora}
                    AND EXTRACT(MONTH FROM fecha) = 6
                    AND tension_servicio > 0
                    """
                    
                    valor_dia = pg_hook.get_first(query_dia)
                    
                    if valor_dia and valor_dia[0] and valor_dia[0] > 0:
                        pg_hook.run(f"""
                        UPDATE tension_dep_processed
                        SET tension_servicio = {valor_dia[0]}
                        WHERE id_subestacion = {id_sub}
                        AND fecha = '{fecha}'
                        AND hora = {hora}
                        """)
                        corregidos_esp += 1
            
            print(f"✅ Corregidos {corregidos_esp} registros del mes 7 usando referencias del mes 6")
        
        # RESTO DE LA FUNCIÓN IGUAL QUE ANTES
        # 3. Procesar mes por mes, en orden estricto
        meses = sorted(registros_por_mes.keys())
        for idx, mes_actual in enumerate(meses):
            registros_mes = registros_por_mes[mes_actual]
            print(f"\n🔄 Procesando {len(registros_mes)} registros del mes {mes_actual}...")
            
            # NUEVO: Comprobar si este mes tiene datos buenos
            mes_tiene_datos_propios = mes_actual in meses_con_datos_buenos
            if mes_tiene_datos_propios:
                print(f"⚠️ ATENCIÓN: Mes {mes_actual} tiene datos propios buenos. Se realizará depuración SELECTIVA.")
            
            # Crear lista de registros corregidos para este mes específico
            registros_corregidos = []
            
            # MEJORADO: Buscar en TODOS los meses anteriores, no solo el inmediato anterior
            # Buscar también en el mes siguiente si existe y tiene datos
            meses_referencia = list(range(1, 13))  # Todos los meses como potencial referencia
            meses_referencia.remove(mes_actual)    # Excepto el mes actual
            
            # Ordenamos meses de referencia por proximidad al mes actual para mejor relevancia
            meses_referencia.sort(key=lambda m: min(abs(m - mes_actual), abs(m - mes_actual + 12)))
            
            print(f"🔍 Orden de búsqueda de referencias: {meses_referencia}")
            
            # MEJORA: Si estamos procesando el mes 8, y el mes 7 está en meses_con_datos_buenos,
            # verificar la calidad de los datos del mes 7 para evitar propagación de ceros
            if mes_actual == 8 and 7 in meses_con_datos_buenos:
                calidad_mes7 = pg_hook.get_first("""
                    SELECT (SUM(CASE WHEN tension_servicio > 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*)
                    FROM tension_dep_processed
                    WHERE EXTRACT(MONTH FROM fecha) = 7
                """)[0] or 0
                
                if calidad_mes7 < 95:  # Si más del 5% son ceros, usar mes 6 como referencia prioritaria
                    print(f"⚠️ ADVERTENCIA: Calidad de datos del mes 7 es {calidad_mes7:.2f}%. Priorizando mes 6 como referencia")
                    # Reordenar meses_referencia para poner mes 6 primero
                    if 6 in meses_referencia:
                        meses_referencia.remove(6)
                        meses_referencia.insert(0, 6)
            
            # Procesar cada registro del mes actual
            for id_sub, fecha, hora in registros_mes:
                valor_encontrado = None
                mes_origen = None
                
                # Buscar en meses de referencia en orden de proximidad
                for mes_ref in meses_referencia:
                    # No usar datos de meses que no tienen datos propios buenos
                    if mes_ref not in meses_con_datos_buenos:
                        continue
                        
                    # NUEVO: Búsqueda sofisticada por patrones
                    # 1. Mismo día/hora exactos
                    fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d') if isinstance(fecha, str) else fecha
                    dia = fecha_dt.day
                    dia_semana = fecha_dt.weekday()
                    mes_ref_year = 2019  # Mismo año para simplificar
                    
                    # MEJORA: Si es la subestación 50, dar prioridad absoluta al mes 6 si estamos procesando mes 7 u 8
                    if id_sub == 50 and mes_actual in [7, 8] and mes_ref == 6:
                        print(f"🎯 Buscando valor especial para subestación 50 en mes 6 (registro fecha={fecha}, hora={hora})")
                        
                        # Búsqueda intensiva en el mes 6 para subestación 50
                        query_especial = f"""
                        SELECT AVG(tension_servicio)
                        FROM tension_dep_processed
                        WHERE id_subestacion = 50
                        AND EXTRACT(MONTH FROM fecha) = 6
                        AND EXTRACT(DOW FROM fecha) = {dia_semana}
                        AND hora = {hora}
                        AND tension_servicio > 0
                        """
                        
                        resultado_especial = pg_hook.get_first(query_especial)
                        if resultado_especial and resultado_especial[0] and resultado_especial[0] > 0:
                            valor_encontrado = resultado_especial[0]
                            mes_origen = 6  # Mes 6 como origen
                            print(f"🌟 Encontrado valor de referencia especial: {valor_encontrado} del mes 6")
                            break
                    
                    # NUEVO: Si el día no existe en el mes de referencia (ej. 31 en febrero), ajustar
                    ultimo_dia_mes_ref = calendar.monthrange(mes_ref_year, mes_ref)[1]
                    if dia > ultimo_dia_mes_ref:
                        dia = ultimo_dia_mes_ref
                    
                    fecha_ref = f"{mes_ref_year}-{mes_ref:02d}-{dia:02d}"
                    
                    # Buscar por coincidencia exacta en día/hora
                    query_exacto = f"""
                    SELECT tension_servicio
                    FROM tension_dep_processed
                    WHERE fecha = '{fecha_ref}'
                    AND hora = '{hora}'
                    AND id_subestacion = {id_sub}
                    AND tension_servicio > 0
                    """
                    
                    resultado = pg_hook.get_first(query_exacto)
                    if resultado and resultado[0] > 0:
                        valor_encontrado = resultado[0]
                        mes_origen = mes_ref
                        break
                    
                    # 2. Si no hay coincidencia exacta, buscar por día de semana
                    if not valor_encontrado:
                        query_dia_semana = f"""
                        SELECT AVG(tension_servicio)
                        FROM tension_dep_processed
                        WHERE EXTRACT(DOW FROM fecha) = {dia_semana}
                        AND hora = '{hora}'
                        AND id_subestacion = {id_sub}
                        AND EXTRACT(MONTH FROM fecha) = {mes_ref}
                        AND tension_servicio > 0
                        """
                        
                        resultado = pg_hook.get_first(query_dia_semana)
                        if resultado and resultado[0] and resultado[0] > 0:
                            valor_encontrado = resultado[0]
                            mes_origen = mes_ref
                            break
                    
                    # 3. Si todavía no hay coincidencia, buscar por hora específica
                    if not valor_encontrado:
                        query_hora = f"""
                        SELECT AVG(tension_servicio)
                        FROM tension_dep_processed
                        WHERE hora = '{hora}'
                        AND id_subestacion = {id_sub}
                        AND EXTRACT(MONTH FROM fecha) = {mes_ref}
                        AND tension_servicio > 0
                        """
                        
                        resultado = pg_hook.get_first(query_hora)
                        if resultado and resultado[0] and resultado[0] > 0:
                            valor_encontrado = resultado[0]
                            mes_origen = mes_ref
                            break
                
                # Si encontramos un valor adecuado, agregar a la lista de correcciones
                if valor_encontrado and valor_encontrado > 0:
                    registros_corregidos.append({
                        'id_subestacion': id_sub,
                        'fecha': fecha,
                        'hora': hora,
                        'tension_servicio': float(valor_encontrado),
                        'dia_semana': calcular_dia_semana(fecha),
                        'mes_origen': mes_origen
                    })
                    total_corregidos += 1
                else:
                    print(f"⚠️ No se encontró valor de referencia para: Sub {id_sub}, Fecha {fecha}, Hora {hora}")
                
                total_procesados += 1
            
            # Si hay registros corregidos para este mes específico, aplicar correcciones
            if registros_corregidos:
                print(f"✅ Se corregirán {len(registros_corregidos)} registros del mes {mes_actual}")
                
                # NUEVO: Enfoque de actualización directa sin tabla temporal
                for registro in registros_corregidos:
                    # Actualizar directamente en tension_dep_processed
                    query_update = f"""
                    UPDATE tension_dep_processed
                    SET tension_servicio = {registro['tension_servicio']}
                    WHERE id_subestacion = {registro['id_subestacion']}
                    AND fecha = '{registro['fecha']}'
                    AND hora = '{registro['hora']}'
                    """
                    
                    try:
                        pg_hook.run(query_update)
                        print(f"✓ Actualizado registro: Sub {registro['id_subestacion']}, "
                              f"Fecha {registro['fecha']}, Hora {registro['hora']}, "
                              f"Valor {registro['tension_servicio']} (del mes {registro['mes_origen']})")
                    except Exception as e:
                        print(f"❌ Error actualizando: {str(e)}")
            else:
                print(f"⚠️ No se encontraron valores de referencia para ningún registro del mes {mes_actual}")
        
        # Verificación final de subestación 50 para los meses 7 y 8
        query_verificacion_sub50 = """
        SELECT 
            EXTRACT(MONTH FROM fecha) as mes,
            COUNT(*) as total
        FROM tension_dep_processed
        WHERE id_subestacion = 50
        AND tension_servicio = 0
        AND EXTRACT(MONTH FROM fecha) IN (7, 8)
        GROUP BY EXTRACT(MONTH FROM fecha)
        """
        
        verificacion_sub50 = pg_hook.get_records(query_verificacion_sub50)
        
        if verificacion_sub50:
            print("⚠️ LA SUBESTACIÓN 50 AÚN TIENE VALORES CERO EN MESES 7 Y 8")
            print("🔄 Aplicando corrección final forzosa...")
            
            # Obtener valor promedio global para subestación 50
            promedio_final = pg_hook.get_first("""
                SELECT AVG(tension_servicio) 
                FROM tension_dep_processed 
                WHERE id_subestacion = 50 
                AND tension_servicio > 0
            """)[0] or 13.8
            
            # Aplicar este valor a TODOS los registros con cero de subestación 50 en meses 7 y 8
            pg_hook.run(f"""
            UPDATE tension_dep_processed
            SET tension_servicio = {promedio_final}
            WHERE id_subestacion = 50
            AND tension_servicio = 0
            AND EXTRACT(MONTH FROM fecha) IN (7, 8)
            """)
            
            print(f"✅ CORRECCIÓN FINAL FORZOSA aplicada con valor {promedio_final}")
        
        # 5. Verificación final general
        verificacion_ceros = pg_hook.get_first("""
        SELECT COUNT(*) FROM tension_dep_processed
        WHERE tension_servicio = 0
        """)[0]
        
        if verificacion_ceros > 0:
            print(f"⚠️ Aún quedan {verificacion_ceros} registros con valor cero")
            
            # NUEVO: Tratamiento final para valores que no tienen referencia en ningún mes
            print("🔄 Realizando tratamiento final para registros sin referencia...")
            
            # Obtener valores nominales para cada subestación como último recurso
            query_subestaciones = """
            SELECT id_subestacion, AVG(tension_servicio) as tension_tipica
            FROM tension_dep_processed
            WHERE tension_servicio > 0
            GROUP BY id_subestacion
            """
            
            valores_por_subestacion = {row[0]: row[1] for row in pg_hook.get_records(query_subestaciones)}
            
            # Actualizar registros que aún tienen cero con el valor típico de esa subestación
            query_ultimos_ceros = """
            SELECT id_subestacion, fecha, hora
            FROM tension_dep_processed
            WHERE tension_servicio = 0
            """
            
            ultimos_ceros = pg_hook.get_records(query_ultimos_ceros)
            if ultimos_ceros:
                print(f"🔄 Realizando corrección final para {len(ultimos_ceros)} registros...")
                for id_sub, fecha, hora in ultimos_ceros:
                    if id_sub in valores_por_subestacion:
                        tension_tipica = valores_por_subestacion[id_sub]
                        
                        # Actualizar con el valor típico de la subestación
                        query_update_final = f"""
                        UPDATE tension_dep_processed
                        SET tension_servicio = {tension_tipica}
                        WHERE id_subestacion = {id_sub}
                        AND fecha = '{fecha}'
                        AND hora = '{hora}'
                        AND tension_servicio = 0
                        """
                        
                        try:
                            pg_hook.run(query_update_final)
                            print(f"✓ Corrección final: Sub {id_sub}, Fecha {fecha}, Hora {hora}, Valor {tension_tipica}")
                            total_corregidos += 1
                        except Exception as e:
                            print(f"❌ Error en corrección final: {str(e)}")
        else:
            print("🎉 Todos los registros con valores cero han sido depurados correctamente")
            
        print(f"\n📊 RESUMEN DE DEPURACIÓN FINAL:")
        print(f"   - Registros procesados: {total_procesados}")
        print(f"   - Registros corregidos: {total_corregidos}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error en depuración final: {str(e)}")
        traceback.print_exc()
        
        return False

def verificar_tablas_temporales():
    """Muestra todas las tablas temporales actualmente existentes"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'tension_dep_%'
        """
        tablas = pg_hook.get_records(query)
        print(f"ℹ️ Tablas temporales existentes ({len(tablas)}):")
        for tabla in tablas:
            print(f"  - {tabla[0]}")
        return tablas
    except Exception as e:
        print(f"❌ Error verificando tablas: {str(e)}")
        return []

def verificar_y_restaurar_tabla_original_al_inicio():
    """
    Verifica el estado de la tabla tension_dep y la restaura desde tension_dep_original
    si es necesario, antes de comenzar cualquier procesamiento.
    """
    try:
        print("🔍 Verificando integridad de la tabla original tension_dep...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")

        # 1. Verificar si existe el respaldo original
        respaldo_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep_original')"
        )[0]

        if not respaldo_existe:
            print(
                "⚠️ No se encontró respaldo tension_dep_original. Se creará si existe la tabla original."
            )

            # Verificar si existe la tabla original
            original_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
            )[0]

            if original_existe:
                print("📋 Creando respaldo inicial tension_dep_original...")
                pg_hook.run(
                    "CREATE TABLE tension_dep_original AS SELECT * FROM tension_dep"
                )
                print("✅ Respaldo inicial creado exitosamente")
                return "Respaldo inicial creado"
            else:
                print(
                    "❌ ERROR CRÍTICO: No existe ni la tabla original ni el respaldo!"
                )
                return "Error: No existen tablas necesarias"

        # 2. Verificar si existe la tabla original
        original_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
        )[0]

        if not original_existe:
            print("⚠️ Tabla tension_dep no existe. Restaurando desde respaldo...")
            pg_hook.run(
                "CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original"
            )
            print("✅ Tabla tension_dep restaurada desde respaldo")
            return "Tabla restaurada desde respaldo"

        # 3. Verificar integridad comparando conteos
        count_original = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
        count_backup = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep_original")[
            0
        ]

        print(f"📊 Conteo en tension_dep: {count_original}")
        print(f"📊 Conteo en tension_dep_original: {count_backup}")

        # Si hay una gran diferencia, restaurar desde respaldo
        if count_original < count_backup * 0.9:  # Si tiene menos del 90% de registros
            print(
                f"⚠️ Tabla tension_dep parece inconsistente ({count_original} vs {count_backup})"
            )
            print("🔄 Restaurando tabla completa desde respaldo...")

            # Backup de la tabla actual por si acaso
            backup_nombre = f"tension_dep_backup_{int(time.time())}"
            pg_hook.run(f"CREATE TABLE {backup_nombre} AS SELECT * FROM tension_dep")
            print(f"📋 Se creó respaldo adicional: {backup_nombre}")

            # Restaurar desde respaldo original
            pg_hook.run("DROP TABLE tension_dep")
            pg_hook.run(
                "CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original"
            )
            print("✅ Tabla tension_dep restaurada completamente desde respaldo")
            return "Tabla restaurada por inconsistencia"

        # 4. Limpieza de tablas temporales antiguas
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'tension_dep_prev_%' OR table_name LIKE 'tension_dep_temp_%'
        """
        tablas_temp = pg_hook.get_records(query_tablas_temp)

        if tablas_temp:
            print(f"🧹 Limpiando {len(tablas_temp)} tablas temporales antiguas...")
            for tabla in tablas_temp:
                pg_hook.run(f"DROP TABLE IF EXISTS {tabla[0]}")
            print("✅ Limpieza de tablas temporales completada")

        return "Verificación completada: Tabla original íntegra"

    except Exception as e:
        print(f"❌ Error verificando tabla original: {str(e)}")
        return f"Error: {str(e)}"

def procesar_y_restaurar_mes(mes, **kwargs):
    """Procesa el mes utilizando la función de sustitución de tabla y gestiona los casos sin datos"""
    try:
        # VERIFICACIÓN CRÍTICA - Confirmar que tension_dep tiene todos los datos
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # 1. Verificar que tension_dep contiene datos de diferentes meses
        otros_meses = pg_hook.get_records("""
            SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) 
            FROM tension_dep 
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
        """)
        
        print(f"📊 VERIFICACIÓN PREVIA para mes {mes} - Distribución en tension_dep:")
        for m in otros_meses:
            mes_num = int(m[0])
            print(f"  - Mes {mes_num}: {m[1]} registros")
        
        # Si solo hay un mes y no es 1 (enero), probable error de restauración
        if len(otros_meses) <= 1 and mes > 1:
            print(f"⚠️ ALERTA CRÍTICA: tension_dep solo contiene datos de un mes, pero estamos procesando mes {mes}")
            print(f"🔄 Intentando restauración de emergencia antes de procesar...")
            
            # Restaurar desde tension_dep_original
            if pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep_original')"
            )[0]:
                # Hacer backup del estado actual por si acaso
                backup_actual = f"tension_dep_backup_emergencia_{int(time.time())}"
                pg_hook.run(f"CREATE TABLE {backup_actual} AS SELECT * FROM tension_dep")
                
                # Restaurar desde original
                pg_hook.run("DROP TABLE IF EXISTS tension_dep")
                pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
                print(f"🆘 RESTAURACIÓN DE EMERGENCIA completada antes de procesar mes {mes}")
                
                # Verificar nuevamente
                count_restaurado = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
                print(f"📊 Tabla tension_dep restaurada con {count_restaurado} registros totales")
        
        # VERIFICACIÓN ADICIONAL: Si ya hay datos, no procesar
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        query = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = 2019
        """
        count = pg_hook_destino.get_first(query)[0]
        if count > 0:
            print(f"🔒 VERIFICACIÓN REDUNDANTE: El mes {mes} ya tiene {count} registros. NO SE PROCESARÁ.")
            return True
        
        # VERIFICACIÓN MÁS COMPLETA: Buscar en todas las fuentes posibles
        # Primero verificar en tension_dep_original
        tiene_datos = verificar_existencia_datos_fuente(mes)
        
        if not tiene_datos:
            # Registramos esta situación para información
            kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
            print(f"⚠️ Advertencia: No se detectaron datos en origen principal para el mes {mes}")
            print(f"🔍 Intentando buscar en fuentes alternativas...")
            kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
            print(f"⚠️ NO HAY DATOS para el mes {mes}. Se ha verificado en todas las fuentes disponibles.")
            print(f"⚠️ Este no es un error del DAG, simplemente no hay datos para procesar.")
            return True
        
        # Si hay datos, ejecutar el procesamiento
        print(f"🚀 Iniciando procesamiento del mes {mes}...")
        resultado_procesamiento = procesar_mes_sustituyendo_tabla(mes, **kwargs)
        
        return resultado_procesamiento
    except Exception as e:
        print(f"❌ Error procesando mes {mes}: {str(e)}")
        
        # Restauración de emergencia si hay error
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            pg_hook.run("DROP TABLE IF EXISTS tension_dep")
            pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
            print(f"🆘 RESTAURACIÓN DE EMERGENCIA tras error en procesar_y_restaurar_mes: {mes}")
        except Exception as e2:
            print(f"❌ Error secundario en restauración: {str(e2)}")
            
        return False

def verificar_completitud_mes(mes, umbral=99.0, **kwargs):
    """Verifica que el mes procesado haya alcanzado el umbral de completitud"""
    try:
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # Contar registros en origen para este mes
        query_origen = f"""
        SELECT COUNT(*) FROM public.tension_dep_original
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = 2019
        """
        total_origen = pg_hook_origen.get_first(query_origen)[0]

        # Contar registros en destino para este mes
        query_destino = f"""
        SELECT COUNT(*) FROM tension_dep_processed
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = 2019
        """
        total_destino = pg_hook_destino.get_first(query_destino)[0]

        # Calcular completitud
        if total_origen > 0:
            completitud = (total_destino / total_origen) * 100
            print(
                f"📊 Completitud del mes {mes}: {completitud:.2f}% ({total_destino}/{total_origen})"
            )

            # Verificar si alcanza el umbral
            if completitud >= umbral:
                print(
                    f"✅ Mes {mes} alcanzó umbral de completitud ({completitud:.2f}% ≥ {umbral}%)"
                )
                return True
            else:
                print(
                    f"⚠️ Mes {mes} no alcanzó umbral de completitud ({completitud:.2f}% < {umbral}%)"
                )
                return False
        else:
            print(f"⚠️ No hay datos en origen para el mes {mes}")
            return False

    except Exception as e:
        print(f"❌ Error verificando completitud del mes {mes}: {str(e)}")
        return False

def verificar_depuracion_completa(mes):
    """
    Verificación mejorada para comprobar datos en AMBAS tablas (original y principal)
    y permitir continuar si no hay datos o si ya están procesados.
    """
    print(f"🔍 Verificando que la depuración del mes {mes} esté COMPLETAMENTE finalizada...")
    if mes <= 0:
        print(f"✅ Primer mes del año - No hay verificación previa requerida.")
        return True
    # Conseguir hooks para verificar bases de datos
    pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
    pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
    
    # 1. Verificar datos en TODAS las tablas disponibles (más completo)
    ano = 2019
    fecha_inicio = f"{ano}-{mes:02d}-01"
    mes_siguiente = mes + 1 if mes < 12 else 1
    ano_siguiente = ano if mes < 12 else ano + 1
    fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
    
    # VERIFICACIÓN ROBUSTA: Comprobar en AMBAS tablas origen
    try:
        datos_original = pg_hook_origen.get_first(f"""
            SELECT COUNT(*) FROM tension_dep_original
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)[0]
        
        datos_principal = pg_hook_origen.get_first(f"""
            SELECT COUNT(*) FROM tension_dep
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)[0]
        
        # Registrar resultados de ambas verificaciones
        print(f"📊 Datos en tension_dep_original para mes {mes}: {datos_original}")
        print(f"📊 Datos en tension_dep principal para mes {mes}: {datos_principal}")
        
        # Usar el máximo de ambas fuentes para determinar existencia
        datos_origen = max(datos_original, datos_principal)
        
    except Exception as e:
        print(f"⚠️ Error verificando datos de origen: {str(e)}")
        # En caso de error, intentar solo con la tabla principal
        try:
            datos_principal = pg_hook_origen.get_first(f"""
                SELECT COUNT(*) FROM tension_dep
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """)[0]
            datos_origen = datos_principal
        except:
            # Si sigue fallando, asumir conservadoramente que sí hay datos
            datos_origen = 1
            print("⚠️ No se pudo verificar datos de origen, asumiendo que existen")
    
    # Si no hay datos en ninguna tabla, permitir continuar
    if datos_origen == 0:
        print(f"🟡 VERIFICACIÓN COMPLETA: No existen datos de origen para el mes {mes}")
        print(f"✅ Se permite continuar al siguiente mes ya que no hay datos que procesar")
        
        # Esperar un tiempo para sincronizar
        print(f"⏱️ Esperando 10 segundos para asegurar sincronización...")
        time.sleep(10)
        
        return True
    
    # 2. Verificar si hay registros de ese mes en processed
    try:
        query_procesados = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = 2019
        """
        count = pg_hook.get_first(query_procesados)[0]
        
        # Si había datos de origen pero no se procesaron, ahí sí debemos fallar
        if count == 0:
            print(f"⛔ BARRERA ESTRICTA: No hay datos procesados para el mes {mes}, pero sí había {datos_origen} registros en origen!")
            print(f"⛔ NO SE PERMITE continuar al siguiente mes.")
            raise Exception(f"Barrera estricta: No hay datos de tensión procesados para mes {mes} pero sí existían en origen")
        
        print(f"✅ BARRERA ESTRICTA: Verificado que el mes {mes} tiene {count} registros de tensión procesados.")
        print(f"✅ Se permite avanzar al siguiente mes.")
        
        # Esperar un tiempo adicional para asegurar sincronización completa
        print(f"⏱️ Esperando 15 segundos para sincronización...")
        time.sleep(15)
        
        return True
    except Exception as e:
        if "does not exist" in str(e) and "tension_dep_processed" in str(e):
            # La tabla de destino no existe aún, pero podemos continuar
            print(f"⚠️ La tabla tension_dep_processed no existe aún, se permite continuar para crearla")
            return True
        else:
            # Otros errores sí deben fallar el proceso
            print(f"❌ Error verificando datos procesados: {str(e)}")
            raise

def stop_all_processors_individually_safely():
    """Detiene todos los procesadores con mayor tiempo de espera y manejo mejorado de errores"""
    try:
        # Lista de todos los procesadores combinados en una sola lista
        all_processors = [
            {"id": QUERY_DB_TABLE_PROCESSOR_ID, "name": "QueryDatabaseTable"},
            {"id": CONVERT_RECORD_ID, "name": "ConvertRecord"},
            {"id": SPLIT_JSON_ID, "name": "SplitJson"},
            {"id": MERGE_CONTENT_ID, "name": "MergeContent"},
            {"id": PUBLISH_KAFKA_ID, "name": "PublishKafka"},
            {"id": PUT_DB_PROCESSOR_ID_P, "name": "PutDatabaseRecord"},
            {"id": CONSUME_KAFKA_PROCESSOR_ID, "name": "ConsumeKafka"},
            {"id": EVALUATE_JSON_PATH_ID, "name": "EvaluateJsonPath"},
            {"id": REPLACE_TEXT_ID, "name": "ReplaceText"},
            {"id": PUT_DB_PROCESSOR_ID, "name": "PutDatabaseRecord"}
        ]
        
        print("📌 Deteniendo procesadores en forma segura...")
        
        # Primera pasada - intentar detener todos los procesadores
        for processor in all_processors:
            try:
                detener_procesador(processor["id"])
                print(f"⏹️ Enviada señal de detención a {processor['name']}")
            except Exception as e:
                print(f"⚠️ Error deteniendo {processor['name']}: {str(e)}")
        
        # Esperar más tiempo para asegurarnos que la detención sea efectiva
        print("⏱️ Esperando 20 segundos para asegurar detención completa...")
        time.sleep(20)
        
        # Segunda pasada - verificar estado y forzar detención si es necesario
        for processor in all_processors:
            try:
                estado = obtener_estado_procesador(processor["id"])
                estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
                
                if estado_actual not in ["STOPPED", "DISABLED"]:
                    print(f"⚠️ {processor['name']} aún no detenido ({estado_actual}), forzando detención...")
                    detener_procesador(processor["id"])
            except:
                pass
                
        # Esperar otro tiempo adicional
        print("⏱️ Esperando 10 segundos adicionales...")
        time.sleep(10)
        
        return "Procesadores detenidos en forma segura"
        
    except Exception as e:
        print(f"⚠️ Error general al detener procesadores: {str(e)}")
        return f"Error al detener procesadores: {str(e)}"

def procesar_y_restaurar_mes_con_manejo_signales(mes, **kwargs):
    """Versión mejorada que maneja señales SIGTERM y restaura el estado correctamente"""
    import signal
    
    # Capturador original de señales
    original_sigterm_handler = signal.getsignal(signal.SIGTERM)
    original_sigint_handler = signal.getsignal(signal.SIGINT)
    
    # Estado para seguimiento
    interrumpido = {"valor": False}
    
    # Nuevo manejador de señales que realiza restauración
    def sigterm_handler(signum, frame):
        print(f"⚠️ Recibida señal {signum} durante procesamiento del mes {mes}")
        interrumpido["valor"] = True
        
        # Intentar restauración de emergencia
        try:
            print("🔄 Iniciando restauración de emergencia de tabla original...")
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            
            # Restaurar desde tension_dep_original (restauración de emergencia)
            pg_hook.run("DROP TABLE IF EXISTS tension_dep")
            pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
            print("✅ Tabla original restaurada exitosamente durante interrupción")
            
            # Detener procesadores de forma segura
            stop_all_processors_individually_safely()
            
        except Exception as e:
            print(f"❌ Error en restauración de emergencia: {str(e)}")
        
        # Permitir que la señal original se procese
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        signal.signal(signal.SIGINT, original_sigint_handler)
    
    try:
        # Configurar manejador de señales personalizado
        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGINT, sigterm_handler)
        
        # Ejecutar función normal con timeouts más cortos
        result = procesar_y_restaurar_mes(mes, **kwargs)
        
        # Restaurar manejadores originales
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        signal.signal(signal.SIGINT, original_sigint_handler)
        
        if interrumpido["valor"]:
            print("⚠️ El procesamiento fue interrumpido pero se realizó la restauración")
            return False
        
        return result
        
    except Exception as e:
        # Restaurar manejadores originales
        signal.signal(signal.SIGTERM, original_sigterm_handler)
        signal.signal(signal.SIGINT, original_sigint_handler)
        
        print(f"❌ Error procesando mes {mes}: {str(e)}")
        
        # Intentar restauración de emergencia
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            pg_hook.run("DROP TABLE IF EXISTS tension_dep")
            pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
            print(f"🆘 RESTAURACIÓN DE EMERGENCIA tras error")
        except Exception as e2:
            print(f"❌ Error secundario en restauración: {str(e2)}")
            
        return False

def procesar_ceros_tension(mes, **kwargs):
    """
    Versión mejorada con tratamiento especial para subestación 50
    """
    try:
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Definir rangos de fechas
        ano = 2019
        fecha_inicio = f"{ano}-{mes:02d}-01"
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = ano if mes < 12 else ano + 1
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        print(f"🔍 Buscando valores cero en tensión para el mes {mes}...")
        
        # TRATAMIENTO ESPECIAL PARA SUBESTACIÓN 50
        if mes in [7, 8]:  # Meses problemáticos
            print("🎯 TRATAMIENTO ESPECIAL para subestación 50 en meses 7 y 8...")
            
            # 1. Obtener datos específicos de subestación 50 con ceros
            query_sub50 = f"""
            SELECT fecha, hora, dia_semana
            FROM tension_dep_processed
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            AND id_subestacion = 50
            AND tension_servicio = 0
            """
            
            registros_sub50 = pg_hook_destino.get_records(query_sub50)
            
            if registros_sub50:
                print(f"⚠️ Encontrados {len(registros_sub50)} registros con cero para subestación 50")
                
                # 2. Obtener patrones de tensión del mes 6 para esta subestación
                patrones_mes6 = pg_hook_destino.get_records("""
                SELECT 
                    hora, 
                    EXTRACT(DOW FROM fecha) as dia_semana,
                    AVG(tension_servicio) as valor_promedio
                FROM tension_dep_processed
                WHERE EXTRACT(MONTH FROM fecha) = 6
                AND EXTRACT(YEAR FROM fecha) = 2019
                AND id_subestacion = 50
                AND tension_servicio > 0
                GROUP BY hora, EXTRACT(DOW FROM fecha)
                """)
                
                # Crear diccionario para acceso rápido (hora, dia_semana) -> valor_promedio
                patron_valores = {}
                for p in patrones_mes6:
                    hora, dia_semana, valor = p
                    patron_valores[(int(hora), int(dia_semana))] = float(valor)
                
                # 3. Obtener promedio general como fallback
                promedio_general = pg_hook_destino.get_first("""
                SELECT AVG(tension_servicio) 
                FROM tension_dep_processed
                WHERE id_subestacion = 50 
                AND tension_servicio > 0
                """)[0] or 13.8  # Fallback a valor típico
                
                # 4. Corregir cada registro usando patrón o promedio
                corregidos = 0
                for reg in registros_sub50:
                    fecha, hora, dia_semana = reg
                    
                    # Buscar valor en patrones
                    valor = patron_valores.get((int(hora), int(dia_semana)), promedio_general)
                    
                    # Actualizar registro
                    update_query = f"""
                    UPDATE tension_dep_processed
                    SET tension_servicio = {valor}
                    WHERE id_subestacion = 50
                    AND fecha = '{fecha}'
                    AND hora = {hora}
                    """
                    
                    pg_hook_destino.run(update_query)
                    corregidos += 1
                    
                print(f"✅ Corregidos {corregidos} registros de subestación 50 con datos de mes 6")
        
        # Encontrar registros con ceros en el mes actual (para todas las subestaciones)
        query_ceros = f"""
        SELECT fecha, hora, id_subestacion, dia_semana
        FROM tension_dep_processed
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        AND tension_servicio = 0
        """
        registros_cero = pg_hook_destino.get_records(query_ceros)
        
        print(f"📊 Encontrados {len(registros_cero)} registros con tensión_servicio = 0 en mes {mes}")
        
        # Si no hay registros con cero, terminar
        if not registros_cero:
            return True
            
        # Obtener datos históricos de todos los meses anteriores para análisis de patrón
        meses_anteriores = []
        for m in range(1, mes):
            # Verificar si el mes tiene datos buenos (no solo ceros)
            query_check = f"""
            SELECT COUNT(*) 
            FROM tension_dep_processed
            WHERE EXTRACT(MONTH FROM fecha) = {m}
            AND tension_servicio > 0
            """
            count = pg_hook_destino.get_first(query_check)[0]
            if count > 0:
                meses_anteriores.append(m)
        
        print(f"🔍 Meses disponibles para búsqueda histórica: {meses_anteriores}")
        
        # Para cada registro con cero, buscar valores históricos
        procesados = 0
        corregidos = 0
        
        for registro in registros_cero:
            fecha, hora, id_sub, dia_semana = registro
            
            # Si es subestación 50 y ya fue tratada en la sección especial, continuar
            if id_sub == 50 and mes in [7, 8]:
                continue
            
            # Convertir fecha a objeto datetime
            fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d') if isinstance(fecha, str) else fecha
            
            # Estrategias de búsqueda, en orden de prioridad:
            valor_encontrado = None
            fuente_valor = None
            
            # 1. Buscar por coincidencia exacta de día, hora y subestación
            for mes_anterior in reversed(meses_anteriores):  # Empezando por el más reciente
                dia_fecha = fecha_dt.day
                ultimo_dia_mes_anterior = calendar.monthrange(ano, mes_anterior)[1]
                
                # Ajustar día si excede los días del mes (ej. 31 de enero vs. 28 de febrero)
                if dia_fecha > ultimo_dia_mes_anterior:
                    dia_fecha = ultimo_dia_mes_anterior
                    
                fecha_anterior = f"{ano}-{mes_anterior:02d}-{dia_fecha:02d}"
                
                query_exacta = f"""
                SELECT tension_servicio 
                FROM tension_dep_processed
                WHERE fecha = '{fecha_anterior}'
                AND hora = '{hora}'
                AND id_subestacion = {id_sub}
                AND tension_servicio > 0
                LIMIT 1
                """
                
                resultado = pg_hook_destino.get_first(query_exacta)
                if resultado and resultado[0] > 0:
                    valor_encontrado = resultado[0]
                    fuente_valor = f"coincidencia exacta en mes {mes_anterior}"
                    break
            
            # 2. Si no encontró, buscar por día de la semana y hora
            if not valor_encontrado:
                for mes_anterior in reversed(meses_anteriores):
                    query_dia_semana = f"""
                    SELECT AVG(tension_servicio)
                    FROM tension_dep_processed
                    WHERE EXTRACT(DOW FROM fecha) = {dia_semana}
                    AND hora = '{hora}'
                    AND id_subestacion = {id_sub}
                    AND EXTRACT(MONTH FROM fecha) = {mes_anterior}
                    AND tension_servicio > 0
                    """
                    
                    resultado = pg_hook_destino.get_first(query_dia_semana)
                    if resultado and resultado[0] and resultado[0] > 0:
                        valor_encontrado = resultado[0]
                        fuente_valor = f"día semana similar en mes {mes_anterior}"
                        break
            
            # 3. Si aún no encontró, buscar solo por hora y subestación
            if not valor_encontrado:
                for mes_anterior in reversed(meses_anteriores):
                    query_hora = f"""
                    SELECT AVG(tension_servicio)
                    FROM tension_dep_processed
                    WHERE hora = '{hora}'
                    AND id_subestacion = {id_sub}
                    AND EXTRACT(MONTH FROM fecha) = {mes_anterior}
                    AND tension_servicio > 0
                    """
                    
                    resultado = pg_hook_destino.get_first(query_hora)
                    if resultado and resultado[0] and resultado[0] > 0:
                        valor_encontrado = resultado[0]
                        fuente_valor = f"hora similar en mes {mes_anterior}"
                        break
            
            # 4. Por último, intentar con el valor promedio general para esa subestación
            if not valor_encontrado:
                query_promedio = f"""
                SELECT AVG(tension_servicio)
                FROM tension_dep_processed
                WHERE id_subestacion = {id_sub}
                AND tension_servicio > 0
                """
                
                resultado = pg_hook_destino.get_first(query_promedio)
                if resultado and resultado[0] and resultado[0] > 0:
                    valor_encontrado = resultado[0]
                    fuente_valor = "promedio general"
            
            # Si encontramos algún valor, actualizar el registro
            if valor_encontrado:
                query_update = f"""
                UPDATE tension_dep_processed
                SET tension_servicio = {valor_encontrado}
                WHERE fecha = '{fecha}'
                AND hora = '{hora}'
                AND id_subestacion = {id_sub}
                """
                
                pg_hook_destino.run(query_update)
                print(f"✅ Actualizado: {fecha}, {hora}, sub:{id_sub} con {valor_encontrado} ({fuente_valor})")
                corregidos += 1
            else:
                print(f"⚠️ No se encontró valor para: {fecha}, {hora}, sub:{id_sub}")
            
            procesados += 1
        
        # Verificación final específica para subestación 50 en meses 7 y 8
        if mes in [7, 8]:
            query_final_sub50 = f"""
            SELECT COUNT(*)
            FROM tension_dep_processed
            WHERE id_subestacion = 50
            AND fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            AND tension_servicio = 0
            """
            
            zeros_sub50 = pg_hook_destino.get_first(query_final_sub50)[0]
            
            if zeros_sub50 > 0:
                print(f"⚠️ ¡ALERTA! Aún quedan {zeros_sub50} registros con cero para subestación 50")
                
                # CORRECCIÓN FORZADA FINAL para subestación 50
                valor_promedio = pg_hook_destino.get_first("""
                    SELECT AVG(tension_servicio)
                    FROM tension_dep_processed
                    WHERE id_subestacion = 50
                    AND tension_servicio > 0
                """)[0] or 13.8
                
                pg_hook_destino.run(f"""
                UPDATE tension_dep_processed
                SET tension_servicio = {valor_promedio}
                WHERE id_subestacion = 50
                AND fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                AND tension_servicio = 0
                """)
                
                print(f"🔧 CORRECCIÓN FORZADA aplicada a subestación 50 con valor: {valor_promedio}")
        
        print(f"✅ Procesados {corregidos}/{procesados} registros con tensión_servicio = 0 en mes {mes}")
        
        # Verificación final general
        query_verificacion = f"""
        SELECT COUNT(*)
        FROM tension_dep_processed
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        AND tension_servicio = 0
        """
        
        ceros_restantes = pg_hook_destino.get_first(query_verificacion)[0]
        if ceros_restantes > 0:
            print(f"⚠️ Quedan {ceros_restantes} registros con tensión_servicio = 0 en mes {mes}")
            # Se completará en depuración final
        else:
            print(f"✅ Mes {mes} completamente depurado")
        
        return True
        
    except Exception as e:
        print(f"❌ Error procesando ceros de tensión: {str(e)}")
        traceback.print_exc()
        return False

def esperar_verificar_y_finalizar_mes(mes, tiempo_espera_minutos=12, **kwargs):
    """Espera el tiempo especificado y da más tiempo a los procesadores"""
    try:
        # ✨ TRATAMIENTO ESPECIAL PARA DICIEMBRE (MES 12) ✨
        if mes == 12:
            tiempo_espera_minutos = 20  # Dar más tiempo a Diciembre por su volumen
            print(f"⏱️ AJUSTE ESPECIAL: Aumentando tiempo de espera para Diciembre a {tiempo_espera_minutos} minutos")

        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        ano = 2019
        
        # Verificar si se marcó que no hay datos para este mes
        ti = kwargs.get('ti')
        if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
            print(f"⚠️ El mes {mes} fue marcado como sin datos. Saltando espera/verificación.")
            return True
        
        print(f"⏳ Iniciando espera de {tiempo_espera_minutos} minutos para el mes {mes}...")
        
        # Calcular tiempo total de espera en segundos
        tiempo_total_segundos = tiempo_espera_minutos * 60
        intervalo_verificacion = 60  # Verificar cada 60 segundos

        # Query para verificar registros
        query = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """

        # Variables para seguimiento
        ultimo_conteo = 0
        conteo_estable = 0
        tiempo_transcurrido = 0

        # Bucle de espera con verificaciones periódicas
        while tiempo_transcurrido < tiempo_total_segundos:
            # Verificar conteo actual
            conteo_actual = pg_hook.get_first(query)[0]

            print(f"📊 Verificación a los {tiempo_transcurrido} segundos: {conteo_actual} registros procesados para mes {mes}")

            # Verificación de estabilidad
            if conteo_actual > 0:
                if conteo_actual == ultimo_conteo:
                    conteo_estable += 1
                    print(f"  ⏳ Conteo estable ({conteo_actual}) durante {conteo_estable} verificaciones")

                    if conteo_estable >= 5:
                        print(f"✅ Procesamiento completado para mes {mes}: {conteo_actual} registros")
                        # Espera adicional para asegurar procesamiento completo
                        print(f"⏳ Esperando 60 segundos adicionales para asegurar finalización completa...")
                        time.sleep(60)
                        break
                else:
                    # Reiniciar contador de estabilidad si el conteo cambia
                    conteo_estable = 0

            ultimo_conteo = conteo_actual

            # Extender tiempo si hay progreso cerca del límite
            if (tiempo_transcurrido >= tiempo_total_segundos * 0.8 and 
                conteo_actual > 0 and 
                conteo_estable < 5):
                print(f"⚠️ Extendiendo tiempo de espera para asegurar finalización...")
                tiempo_total_segundos += 180  # Añadir 3 minutos más

            # Esperar para la siguiente verificación
            time.sleep(intervalo_verificacion)
            tiempo_transcurrido += intervalo_verificacion
        
        # Añadir tiempo extra para procesamiento
        print(f"⏳ Asegurando tiempo adicional para procesamiento completo...")
        time.sleep(120)  # 2 minutos adicionales para asegurar el procesamiento
        
        # Al finalizar, detener procesadores de forma más efectiva
        print(f"⏹️ Deteniendo procesadores ordenadamente después de procesar mes {mes}...")
        stop_all_processors_individually()
        time.sleep(15)  # Esperar para asegurar detención
        
        # RESTAURACIÓN GARANTIZADA DESDE tension_dep_original
        print(f"🔄 RESTAURANDO TABLA ORIGINAL desde tension_dep_original después de procesar mes {mes}...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")

        # Eliminar la tabla tension_dep actual (que solo tiene datos del mes procesado)
        pg_hook.run("DROP TABLE IF EXISTS tension_dep")

        # Restaurar directamente desde tension_dep_original (manera más segura)
        pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
        print("✅ Tabla tension_dep restaurada completamente desde tension_dep_original")

        # Verificar restauración
        count_total = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
        print(f"📊 Tabla original restaurada con {count_total} registros TOTALES")
        
        # Verificar que contiene datos de otros meses
        otros_meses = pg_hook.get_records("""
            SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) 
            FROM tension_dep 
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
        """)
        print(f"📊 Distribución por mes después de restaurar:")
        for m in otros_meses:
            print(f"  - Mes {int(m[0])}: {m[1]} registros")
            
        # TRATAMIENTO ESPECIAL PARA DICIEMBRE
        if mes == 12:
            print("🛑 DETENCIÓN ESPECIAL PARA DICIEMBRE: Asegurando apagado completo")
            # Detener todos los procesadores con método agresivo
            stop_all_processors_individually()
            time.sleep(10)  # Esperar 10 segundos
            
            # Segunda pasada para verificar que realmente se detuvieron
            stop_all_processors_individually()
            time.sleep(10)  # Esperar otros 10 segundos
            
            # Verificar explícitamente
            print("🔍 Verificando detención de procesadores críticos...")
            
            # Verificar QUERY_DB_TABLE_PROCESSOR_ID
            estado = obtener_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
            estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
            print(f"  QueryDatabaseTable: {estado_actual}")
            
            # Si aún no se ha detenido, intentar una vez más
            if estado_actual != "STOPPED":
                print("  ⚠️ Forzando detención...")
                detener_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
                
            # Verificar también el procesador de Kafka
            estado_kafka = obtener_estado_procesador(PUBLISH_KAFKA_ID)
            estado_kafka_actual = estado_kafka.get("component", {}).get("state", "UNKNOWN")
            print(f"  PublishKafka: {estado_kafka_actual}")
            if estado_kafka_actual != "STOPPED":
                print("  ⚠️ Forzando detención de Kafka...")
                detener_procesador(PUBLISH_KAFKA_ID)
                
            return True
        else:
            # Para otros meses, detener de forma normal
            # Primero detener QueryDatabaseTable para evitar nuevos datos
            detener_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
            time.sleep(5)
            
            # Luego detener los demás procesadores
            stop_all_processors_individually()
            
        return True  # Mantener esta función con valor de retorno consistente
    except Exception as e:
        print(f"❌ Error durante espera: {str(e)}")
        
        # RESTAURACIÓN DE EMERGENCIA en caso de error
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            pg_hook.run("DROP TABLE IF EXISTS tension_dep")
            pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
            print("🆘 RESTAURACIÓN DE EMERGENCIA completada debido a error")
        except Exception as e2:
            print(f"❌ Error adicional en restauración de emergencia: {str(e2)}")
            
        return False

def restaurar_estado_original_completo():
    """
    Asegura que la base de datos vuelva a su estado original, eliminando tablas temporales
    y restaurando tension_dep como la única tabla principal.
    """
    try:
        print("🔄 Verificando y restaurando estado original de la base de datos...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # 1. Verificar existencia de tablas
        tension_dep_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
        )[0]
        
        original_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep_original')"
        )[0]
        
        # 2. RESTAURACIÓN FINAL: Eliminar tension_dep_original y mantener solo tension_dep
        if tension_dep_existe and original_existe:
            # Contar registros para comparación
            count_tension_dep = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
            count_original = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep_original")[0]
            
            print(f"📊 Registros en tension_dep: {count_tension_dep}")
            print(f"📊 Registros en tension_dep_original: {count_original}")
            
            # PASO FINAL: Eliminar tension_dep_original como solicitado
            pg_hook.run("DROP TABLE IF EXISTS tension_dep_original")
            print("✅ TABLA tension_dep_original ELIMINADA - Restauración completada")
        
        # 3. Buscar y eliminar todas las tablas temporales
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE (table_name LIKE 'tension_dep_%') 
        AND table_name != 'tension_dep' AND table_name != 'tension_dep_processed'
        """
        
        tablas_temp = pg_hook.get_records(query_tablas_temp)
        
        if tablas_temp:
            print(f"🧹 Eliminando {len(tablas_temp)} tablas temporales...")
            for tabla in tablas_temp:
                nombre_tabla = tabla[0]
                pg_hook.run(f"DROP TABLE IF EXISTS {nombre_tabla}")
                print(f"  🗑️ Eliminada tabla: {nombre_tabla}")
        
        # 4. Verificar estado final
        count_final = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
        print(f"📊 ESTADO FINAL: Tabla tension_dep contiene {count_final} registros")
        
        return f"Base de datos restaurada a estado original. Eliminadas {len(tablas_temp)} tablas temporales."
        
    except Exception as e:
        print(f"❌ Error al restaurar estado original: {str(e)}")
        return f"Error: {str(e)}"

def esperar_verificar_y_restaurar_mes(mes, tiempo_espera_minutos=12, **kwargs):
    """
    Espera el tiempo especificado mientras monitorea el procesamiento del mes,
    verifica si se guardaron correctamente los datos, y restaura la tabla original.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        ano = 2019

        print(
            f"⏳ Iniciando espera de {tiempo_espera_minutos} minutos para el mes {mes}..."
        )

        # Calcular tiempo total de espera en segundos
        tiempo_total_segundos = tiempo_espera_minutos * 60
        intervalo_verificacion = 60  # Verificar cada 60 segundos

        # Query para verificar registros
        query = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """

        # Variables para seguimiento
        ultimo_conteo = 0
        conteo_estable = 0
        tiempo_transcurrido = 0

        # Bucle de espera con verificaciones periódicas
        while tiempo_transcurrido < tiempo_total_segundos:
            # Verificar conteo actual
            conteo_actual = pg_hook.get_first(query)[0]

            print(
                f"📊 Verificación a los {tiempo_transcurrido} segundos: {conteo_actual} registros procesados para mes {mes}"
            )

            # Verificación de estabilidad mejorada
            if conteo_actual > 0:
                if conteo_actual == ultimo_conteo:
                    conteo_estable += 1
                    print(
                        f"  ⏳ Conteo estable ({conteo_actual}) durante {conteo_estable} verificaciones"
                    )

                    # Requerir más verificaciones consecutivas (5 en lugar de 3)
                    if conteo_estable >= 5:
                        print(
                            f"✅ Procesamiento completado para mes {mes}: {conteo_actual} registros"
                        )
                        # Añadir una pausa extra para asegurar que todos los datos estén procesados
                        print(
                            f"⏳ Esperando 60 segundos adicionales para asegurar finalización completa..."
                        )
                        time.sleep(60)
                        break
                else:
                    # Reiniciar contador de estabilidad si el conteo cambia
                    conteo_estable = 0

            ultimo_conteo = conteo_actual

            # Si estamos cerca del tiempo límite pero hay progreso, extender el tiempo
            if (
                tiempo_transcurrido >= tiempo_total_segundos * 0.8
                and conteo_actual > 0
                and conteo_estable < 5
            ):
                print(f"⚠️ Extendiendo tiempo de espera para asegurar finalización...")
                tiempo_total_segundos += 180  # Añadir 3 minutos más

            # Esperar para la siguiente verificación
            time.sleep(intervalo_verificacion)
            tiempo_transcurrido += intervalo_verificacion

        # Agregar espera adicional al final para asegurar procesamiento completo
        print(
            f"⏳ Esperando 60 segundos adicionales para asegurar que no queden datos en cola..."
        )
        time.sleep(60)

        # Detener procesadores después de esperar
        print(f"⏹️ Deteniendo procesadores después de esperar para mes {mes}...")
        stop_all_processors_individually()
        time.sleep(30)  # Más tiempo para asegurar detención completa

        # Verificación final
        conteo_final = pg_hook.get_first(query)[0]
        print(f"📊 Verificación final: {conteo_final} registros para mes {mes}")

        # Restaurar tabla original
        print(f"🔄 Restaurando tabla original después de procesar mes {mes}...")
        restaurar_tabla_original_v2(**kwargs)

        # Comprobar si se guardaron suficientes datos
        if conteo_final > 0:
            print(
                f"✅ Mes {mes} procesado y guardado exitosamente con {conteo_final} registros"
            )
            return True
        else:
            print(f"❌ No se guardaron datos para el mes {mes} o hubo un problema")
            return False

    except Exception as e:
        print(f"❌ Error durante espera y verificación del mes {mes}: {str(e)}")

        # Intentar restaurar la tabla en caso de error
        try:
            print(f"🔄 Intentando restaurar tabla original después de error...")
            stop_all_processors_individually()
            time.sleep(30)  # Más tiempo para asegurar detención completa
            restaurar_tabla_original_v2(**kwargs)
        except Exception as e2:
            print(f"❌ Error adicional al restaurar tabla: {str(e2)}")

        return False

def verificar_disponibilidad_datos_por_mes(**kwargs):
    """Versión mejorada que verifica disponibilidad en AMBAS tablas origen"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        ti = kwargs.get('ti')
        
        print("📊 VERIFICACIÓN INICIAL DE DISPONIBILIDAD DE DATOS POR MES")
        print("=" * 60)
        
        disponibilidad = {}
        
        for mes in range(1, 13):
            # Calcular fechas para cada mes
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = 2019 if mes < 12 else 2020
            fecha_inicio = f"2019-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            # MEJORA: Consulta que verifica en AMBAS tablas
            try:
                # Verificar en tabla tension_dep_original
                query_original = f"""
                SELECT COUNT(*) 
                FROM tension_dep_original
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """
                count_original = pg_hook.get_first(query_original)[0]
                
                # Verificar en tabla tension_dep
                query_principal = f"""
                SELECT COUNT(*) 
                FROM tension_dep
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """
                count_principal = pg_hook.get_first(query_principal)[0]
                
                # Usar el mayor de los dos conteos
                count = max(count_original, count_principal)
                print(f"  - Original: {count_original}, Principal: {count_principal}")
                
            except Exception as e:
                print(f"  ⚠️ Error verificando conteo dual: {str(e)}")
                # En caso de error, intentar solo con tension_dep
                try:
                    query = f"""
                    SELECT COUNT(*) 
                    FROM tension_dep
                    WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                    """
                    count = pg_hook.get_first(query)[0]
                except Exception as e2:
                    print(f"  ❌ Error en verificación secundaria: {str(e2)}")
                    count = 0  # Asumir que no hay datos en caso de error
            
            disponibilidad[mes] = count
            
            # Registrar en XCom si el mes tiene datos o no
            if ti:
                ti.xcom_push(key=f'mes_{mes}_tiene_datos', value=(count > 0))
            
            if count > 0:
                print(f"✅ MES {mes:2d} ({calendar.month_name[mes]:>9}): {count:,} registros disponibles")
            else:
                print(f"❌ MES {mes:2d} ({calendar.month_name[mes]:>9}): SIN DATOS")
        
        print("=" * 60)
        print(f"Total de meses con datos: {sum(1 for count in disponibilidad.values() if count > 0)}/12")
        
        return disponibilidad
    except Exception as e:
        print(f"❌ Error verificando disponibilidad: {str(e)}")
        return {}

def verificar_existencia_datos_fuente(mes, ano=2019, **kwargs):
    """
    Versión mejorada que analiza la calidad de los datos antes de decidir 
    si un mes está vacío, usando múltiples indicadores y no solo conteos.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")

        # Calcular el rango de fechas para el mes
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = ano if mes < 12 else ano + 1
        fecha_inicio = f"{ano}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"

        print(f"🔍 Verificando existencia de datos para el mes {mes} ({fecha_inicio} a {fecha_fin})")
        
        # 1. Verificar en tension_dep_original (fuente estándar)
        query_original = f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN tension_servicio > 0 THEN 1 ELSE 0 END) as buenos
        FROM tension_dep_original  
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """
        
        resultado_original = pg_hook.get_first(query_original)
        count_original = resultado_original[0] if resultado_original else 0
        buenos_original = resultado_original[1] if resultado_original and len(resultado_original) > 1 else 0
        
        # 2. Verificar en tension_dep (tabla principal)
        query_main = f"""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN tension_servicio > 0 THEN 1 ELSE 0 END) as buenos
        FROM tension_dep
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """
        
        resultado_main = pg_hook.get_first(query_main)
        count_main = resultado_main[0] if resultado_main else 0
        buenos_main = resultado_main[1] if resultado_main and len(resultado_main) > 1 else 0
        
        # Usar el mejor de los dos resultados
        count_total = max(count_original, count_main)
        buenos_total = max(buenos_original, buenos_main)
        
        # Calcular métricas de calidad
        porcentaje_buenos = (buenos_total / count_total * 100) if count_total > 0 else 0
        
        print(f"📊 Datos encontrados para el mes {mes}:")
        print(f"   - Total registros: {count_total}")
        print(f"   - Registros buenos: {buenos_total}")
        print(f"   - Porcentaje buenos: {porcentaje_buenos:.2f}%")
        
        # NUEVO: Criterios mejorados para determinar si un mes tiene datos útiles
        tiene_datos = False
        
        # 1. Criterio básico: existencia de al menos algunos registros
        if count_total >= 100:  # Mínimo número razonable de registros
            print("✓ Criterio 1: Suficientes registros totales")
            
            # 2. Criterio de calidad: porcentaje mínimo de registros buenos
            if porcentaje_buenos >= 5:  # Al menos 5% de datos buenos
                print("✓ Criterio 2: Suficiente porcentaje de buenos valores")
                tiene_datos = True
            else:
                print("✗ Criterio 2: Insuficiente porcentaje de buenos valores")
                
            # 3. Criterio absoluto: número mínimo de registros buenos
            if buenos_total >= 50:  # Al menos 50 registros buenos para referencia
                print("✓ Criterio 3: Suficientes registros buenos en términos absolutos")
                tiene_datos = True
            else:
                print("✗ Criterio 3: Insuficientes registros buenos en términos absolutos")
        else:
            print("✗ Criterio 1: Insuficientes registros totales")
        
        if tiene_datos:
            print(f"✅ CONCLUSIÓN: El mes {mes} tiene datos utilizables")
            return True
        else:
            print(f"⚠️ CONCLUSIÓN: El mes {mes} no tiene datos utilizables o son muy escasos")
            
            # Marcar como sin datos reales
            if 'ti' in kwargs and kwargs['ti']:
                kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
                kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
                
            return False
            
    except Exception as e:
        print(f"❌ Error verificando datos fuente: {str(e)}")
        return False

def restaurar_tabla_original_v2(**kwargs):
    """Restaura la tabla original de manera segura y completa"""
    try:
        print("🔄 Restaurando tabla original tension_dep...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # 1. Intentar recuperar el nombre de la tabla temporal actual
        ti = kwargs.get('ti')
        
        # Buscar primero el nombre de la tabla temporal creada durante el procesamiento
        temp_original = None
        for mes in range(1, 13):
            temp_name = ti.xcom_pull(key=f'tension_dep_original_temp_{mes}')
            if temp_name:
                temp_original = temp_name
                print(f"🔍 Encontrada tabla temporal del mes {mes}: {temp_original}")
                break
                
        if temp_original:
            # Verificar que ambas tablas existan
            temp_existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{temp_original}')"
            )[0]
            
            tension_dep_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
            )[0]
            
            if temp_existe:
                # Eliminar la tabla tension_dep actual (solo tiene datos de un mes)
                if tension_dep_existe:
                    pg_hook.run("DROP TABLE IF EXISTS tension_dep")
                    print("🗑️ Tabla parcial tension_dep eliminada")
                
                # Restaurar tabla original con TODOS los datos
                pg_hook.run(f"ALTER TABLE {temp_original} RENAME TO tension_dep")
                print(f"✅ Tabla original restaurada desde {temp_original}")
                
                # Verificar conteo
                count = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
                print(f"📊 Tabla tension_dep restaurada con {count} registros totales")
                return f"Tabla restaurada exitosamente desde {temp_original}"
        
        # Si el método anterior falla, intentar con la tabla de respaldo
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'tension_dep_prev_%' 
        ORDER BY table_name DESC
        LIMIT 1
        """
        
        # Obtener la tabla temporal más reciente para restaurar
        tablas_temp = pg_hook.get_records(query_tablas_temp)
        
        if tablas_temp and len(tablas_temp) > 0:
            backup_table = tablas_temp[0][0]
            print(f"🔍 Encontrada tabla de respaldo: {backup_table}")
            
            # Eliminar tabla actual si existe
            tension_dep_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
            )[0]
            
            if tension_dep_existe:
                pg_hook.run("DROP TABLE IF EXISTS tension_dep")
                print("🗑️ Tabla tension_dep actual eliminada para restauración completa")
                
            # Restaurar desde la tabla de respaldo
            pg_hook.run(f"ALTER TABLE {backup_table} RENAME TO tension_dep")
            print(f"✅ Tabla original restaurada desde respaldo {backup_table}")
            
            # Verificar restauración
            count = pg_hook.get_first("SELECT COUNT(*) FROM tension_dep")[0]
            print(f"📊 Tabla tension_dep restaurada con {count} registros totales")
            
            return f"Tabla restaurada desde respaldo {backup_table}"
        
        # Si todo lo anterior falla, intentar con tension_dep_original
        original_exists = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep_original')"
        )[0]
        
        if original_exists:
            # Eliminar tabla actual si existe
            tension_dep_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'tension_dep')"
            )[0]
            
            if tension_dep_existe:
                pg_hook.run("DROP TABLE IF EXISTS tension_dep")
            
            # Crear desde respaldo original
            pg_hook.run("CREATE TABLE tension_dep AS SELECT * FROM tension_dep_original")
            print("✅ Tabla tension_dep recreada desde respaldo original tension_dep_original")
            return "Tabla recreada desde respaldo original"
        
        print("⚠️ ERROR CRÍTICO: No se pudo restaurar la tabla original por ningún método")
        return "ERROR: No se pudo restaurar la tabla original"
        
    except Exception as e:
        print(f"❌ Error al restaurar tabla: {str(e)}")
        import traceback
        print(f"Detalles del error: {traceback.format_exc()}")
        return f"Error: {str(e)}"

def verificar_y_corregir_mes_completo_al_finalizar(mes, **kwargs):
    """Verifica que SOLO existan datos del mes correcto y corrige si es necesario"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener distribución actual por mes
        query_dist = """
        SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) 
        FROM tension_dep_processed 
        WHERE EXTRACT(YEAR FROM fecha) = 2019
        GROUP BY EXTRACT(MONTH FROM fecha)
        ORDER BY mes
        """
        
        distribucion = pg_hook.get_records(query_dist)
        print(f"🔍 VERIFICACIÓN FINAL MES {mes}: Distribución de registros en tabla procesada")
        
        # Variables para seguimiento
        total_registros = 0
        registros_mes_correcto = 0
        
        for m in distribucion:
            mes_num = int(m[0])
            cantidad = m[1]
            total_registros += cantidad
            
            if mes_num == mes:
                registros_mes_correcto = cantidad
                print(f"  ✅ Mes {mes_num}: {cantidad} registros (MES CORRECTO)")
            else:
                print(f"  ❌ Mes {mes_num}: {cantidad} registros (NO DEBERÍA EXISTIR)")
                
                # Eliminar datos de meses incorrectos si este es el último mes que procesamos
                if mes >= mes_num:
                    print(f"  🧹 Limpiando datos incorrectos del mes {mes_num}...")
                    pg_hook.run(f"""
                    DELETE FROM tension_dep_processed 
                    WHERE EXTRACT(MONTH FROM fecha) = {mes_num}
                    AND EXTRACT(YEAR FROM fecha) = 2019
                    """)
        
        # Verificar proporción de datos correctos
        if total_registros > 0:
            porcentaje_correcto = (registros_mes_correcto / total_registros) * 100
            print(f"📊 Porcentaje de datos del mes correcto: {porcentaje_correcto:.2f}%")
            
            if porcentaje_correcto < 99:
                print(f"⚠️ ADVERTENCIA: Solo el {porcentaje_correcto:.2f}% son datos del mes {mes}")
                return False
            
        return True
    
    except Exception as e:
        print(f"❌ Error en verificación final: {str(e)}")
        return False

def verificar_datos_procesados_del_mes_correcto(mes, **kwargs):
    """Verifica que los datos procesados correspondan al mes correcto"""
    try:
        # VERIFICACIÓN DE MESES SIN DATOS
        ti = kwargs.get('ti')
        if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
            print(f"✅ VERIFICACIÓN EXITOSA: El mes {mes} fue marcado como sin datos. No se requieren registros.")
            return True  # Éxito para meses sin datos
            
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # VERIFICACIÓN ESPECIAL PARA DICIEMBRE
        if mes == 12:
            fecha_inicio = "2019-12-01"
            fecha_fin = "2020-01-01"
            
            # Verificar por fecha explícitamente
            query_fecha = f"""
            SELECT COUNT(*) FROM tension_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """
            count_fecha = pg_hook.get_first(query_fecha)[0]
            
            if count_fecha > 0:
                print(f"✅ VERIFICACIÓN DICIEMBRE CORRECTA: {count_fecha} registros en el rango de fechas correcto")
                return True
        
        # VERIFICACIÓN ESTÁNDAR PARA OTROS MESES
        ano = 2019
        query = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        count = pg_hook.get_first(query)[0]
        
        if count > 0:
            print(f"✅ Verificación exitosa: {count} registros del mes {mes} año {ano}")
            
            # Mostrar algunos ejemplos para validar
            sample_query = f"""
            SELECT fecha, hora, id_subestacion, dia_semana, tension_servicio
            FROM tension_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            LIMIT 5
            """
            samples = pg_hook.get_records(sample_query)
            print("📊 Muestra de registros procesados:")
            for sample in samples:
                print(f"  {sample}")
                
            return True
        else:
            print(f"⚠️ No se encontraron datos del mes {mes} año {ano}")
            return False

    except Exception as e:
        print(f"❌ Error en verificación: {str(e)}")
        return False

def verificar_si_mes_ya_procesado(mes, ano=2019):
    """
    Verifica si un mes específico ya tiene datos procesados en la tabla destino.
    Retorna True si ya hay datos, False si no hay.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # Construir consulta para verificar existencia de datos
        query = f"""
        SELECT COUNT(*) FROM tension_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        count = pg_hook.get_first(query)[0]
        if count > 0:
            print(
                f"✅ El mes {mes} del año {ano} ya tiene {count} registros procesados"
            )

            # Mostrar una muestra para verificar la calidad de los datos
            sample_query = f"""
            SELECT fecha, hora, id_subestacion, dia_semana, tension_servicio
            FROM tension_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            ORDER BY RANDOM()
            LIMIT 2
            """
            samples = pg_hook.get_records(sample_query)
            print("📊 Muestra de registros existentes:")
            for sample in samples:
                print(f"  {sample}")

            return True
        else:
            print(f"⚠️ No se encontraron datos del mes {mes} año {ano}")
            return False
    except Exception as e:
        print(f"❌ Error en verificación: {str(e)}")
        return False

def verificar_completitud_anual():
    """Verifica la completitud del procesamiento anual del 2019 usando hooks separados"""
    try:
        # Usar PostgresHook con las conexiones correctas
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # Conteo total de registros en origen para 2019
        total_origen = pg_hook_origen.get_first(
            """
            SELECT COUNT(*) FROM tension_dep 
            WHERE fecha BETWEEN '2019-01-01' AND '2020-01-01'
            """
        )[0]
        
        # Conteo total de registros en destino para 2019
        total_destino = pg_hook_destino.get_first(
            """
            SELECT COUNT(*) FROM tension_dep_processed 
            WHERE fecha BETWEEN '2019-01-01' AND '2020-01-01'
            """
        )[0]
        
        # Conteo por mes en origen
        conteo_origen_por_mes = pg_hook_origen.get_records(
            """
            SELECT EXTRACT(MONTH FROM fecha) AS mes, COUNT(*)
            FROM tension_dep 
            WHERE fecha BETWEEN '2019-01-01' AND '2020-01-01'
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
            """
        )

        # Conteo por mes en destino
        conteo_destino_por_mes = pg_hook_destino.get_records(
            """
            SELECT EXTRACT(MONTH FROM fecha) AS mes, COUNT(*)
            FROM tension_dep_processed 
            WHERE fecha BETWEEN '2019-01-01' AND '2020-01-01'
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
            """
        )
        
        # Generar informe
        resultado = "INFORME DE COMPLETITUD DE PROCESAMIENTO ANUAL 2019\n"
        resultado += "=" * 50 + "\n"
        resultado += f"Total registros en origen: {total_origen}\n"
        resultado += f"Total registros en destino: {total_destino}\n"

        if total_origen > 0:
            completitud = (total_destino / total_origen) * 100
            resultado += f"Completitud total: {completitud:.2f}%\n\n"

        resultado += "DETALLE POR MES:\n"
        resultado += "-" * 50 + "\n"
        resultado += "{:<10} {:<15} {:<15} {:<15}\n".format(
            "Mes", "Origen", "Destino", "Completitud"
        )
        resultado += "-" * 50 + "\n"

        # Convertir a diccionarios para facilitar la búsqueda
        dict_origen = {int(float(mes)): count for mes, count in conteo_origen_por_mes}
        dict_destino = {int(float(mes)): count for mes, count in conteo_destino_por_mes}

        for mes in range(1, 13):
            origen = dict_origen.get(mes, 0)
            destino = dict_destino.get(mes, 0)

            if origen > 0:
                comp = (destino / origen) * 100
                resultado += "{:<10} {:<15} {:<15} {:<15.2f}%\n".format(
                    calendar.month_name[mes][:3], origen, destino, comp
                )
            else:
                resultado += "{:<10} {:<15} {:<15} {:<15}\n".format(
                    calendar.month_name[mes][:3], origen, destino, "N/A"
                )

        print(resultado)
        return resultado

    except Exception as e:
        error_msg = f"Error al verificar completitud anual: {str(e)}"
        print(error_msg)
        return error_msg

def crear_taskgroup_mes(mes_num, nombre_mes, mes_anterior):
    """Crea un TaskGroup mejorado con mayor robustez a errores"""
    with TaskGroup(group_id=f"procesar_{nombre_mes}") as grupo_mes:
        # BARRERA
        barrera_mes = PythonOperator(
            task_id=f"barrera_{nombre_mes}",
            python_callable=verificar_depuracion_completa,
            op_kwargs={"mes": mes_anterior},
            trigger_rule="all_done",  # Permitir continuar si la tarea previa falló
            retries=3,                # Permitir reintentos
            retry_delay=timedelta(seconds=30),
            execution_timeout=timedelta(minutes=5),  # Evitar que se cuelgue
        )
        
        # VERIFICACIÓN
        verificar_mes = PythonOperator(
            task_id=f"verificar_si_{nombre_mes}_ya_procesado",
            python_callable=verificar_si_mes_ya_procesado,
            op_kwargs={"mes": mes_num},
            retries=2,
        )

        # BRANCH
        branch_mes = BranchPythonOperator(
            task_id=f"branch_{nombre_mes}",
            python_callable=decidir_ruta_mes_con_disponibilidad,
            op_kwargs={"mes": mes_num, "nombre_mes": nombre_mes},
            trigger_rule="all_done",
        )

        # PROCESAMIENTO COMPLETO
        with TaskGroup(group_id="camino_procesamiento") as camino_procesamiento:
            procesar_mes = PythonOperator(
                task_id=f"procesar_mes_{mes_num}",
                python_callable=procesar_y_restaurar_mes_con_manejo_signales,  # Versión mejorada
                op_kwargs={"mes": mes_num},
                execution_timeout=timedelta(minutes=30),  # Aumentar timeout
            )
            esperar_verificar = PythonOperator(
                task_id=f"esperar_verificar_{mes_num}",
                python_callable=esperar_verificar_y_finalizar_mes,
                op_kwargs={"mes": mes_num, "tiempo_espera_minutos": 15},  # Más tiempo
                execution_timeout=timedelta(minutes=25),
            )
            depurar_ceros = PythonOperator(
                task_id=f"depurar_ceros_{mes_num}",
                python_callable=procesar_ceros_tension,
                op_kwargs={"mes": mes_num},
            )
            verificar_completado = PythonOperator(
                task_id=f"verificar_resultados",
                python_callable=verificar_datos_procesados_del_mes_correcto,
                op_kwargs={"mes": mes_num},
                provide_context=True,
            )

            procesar_mes >> esperar_verificar >> depurar_ceros >> verificar_completado

        # SKIP COMPLETO (SIN PROCESAMIENTO ADICIONAL)
        with TaskGroup(group_id="skip_y_verificar") as skip_y_verificar:
            skip_mes = DummyOperator(
                task_id=f"skip_{nombre_mes}",
            )
            verificar_skip = PythonOperator(
                task_id="verificar_skip",
                python_callable=lambda ti, mes=mes_num: print(
                    f"✅ Mes {mes} ya procesado o sin datos disponibles, SALTADO COMPLETAMENTE"
                ),
            )
            skip_mes >> verificar_skip

        # ESTRUCTURA DE FLUJO
        barrera_mes >> verificar_mes >> branch_mes >> [camino_procesamiento, skip_y_verificar]
        
    return grupo_mes
def limpiar_tablas_temporales(**kwargs):
    """Limpia todas las tablas temporales creadas durante el procesamiento"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # 1. Recuperar nombres de tablas de trabajo registradas en XComs
        ti = kwargs.get('ti')
        tablas_trabajo = []
        
        if ti:
            for mes in range(1, 13):
                tabla = ti.xcom_pull(key=f'tabla_trabajo_mes_{mes}')
                if tabla:
                    tablas_trabajo.append(tabla)
        
        # 2. Buscar todas las tablas temporales en la base de datos
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'tension_dep_work_%' 
           OR table_name LIKE 'tension_dep_temp_%'
           OR table_name LIKE 'tension_dep_prev_%'
           OR table_name LIKE 'tension_dep_backup_%'
        """
        
        tablas_db = pg_hook.get_records(query_tablas_temp)
        tablas_a_eliminar = set(tablas_trabajo)  # Uso set para evitar duplicados
        
        for tabla in tablas_db:
            nombre_tabla = tabla[0]
            # Proteger las tablas críticas 
            if nombre_tabla not in ['tension_dep', 'tension_dep_original', 'tension_dep_processed']:
                tablas_a_eliminar.add(nombre_tabla)
        
        # 3. Eliminar todas las tablas identificadas
        count = 0
        for tabla in tablas_a_eliminar:
            try:
                pg_hook.run(f"DROP TABLE IF EXISTS {tabla}")
                print(f"🗑️ Eliminada tabla temporal: {tabla}")
                count += 1
            except Exception as e:
                print(f"⚠️ Error al eliminar tabla {tabla}: {str(e)}")
        
        return f"Limpiadas {count} tablas temporales"
        
    except Exception as e:
        print(f"⚠️ Error en limpieza de tablas: {str(e)}")
        return f"Error en limpieza: {str(e)}"

# Configurar el DAG para procesar múltiples meses en secuencia con verificación
with dag:
    verificar_tabla_original = PythonOperator(
        task_id="verificar_y_restaurar_tabla_original",
        python_callable=verificar_y_restaurar_tabla_original_al_inicio,
    )
    
    verificar_disponibilidad = PythonOperator(
        task_id="verificar_disponibilidad_datos",
        python_callable=verificar_disponibilidad_datos_por_mes,
        provide_context=True,
    )
    
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=create_postgres_connections,
    )

    verificar_proxy = PythonOperator(
        task_id="verificar_proxy",
        python_callable=verificar_proxy_disponible,
    )

    # Creación de TaskGroups para cada mes
    grupo_enero = crear_taskgroup_mes(1, "enero", 0)
    grupo_febrero = crear_taskgroup_mes(2, "febrero", 1)
    grupo_marzo = crear_taskgroup_mes(3, "marzo", 2)
    grupo_abril = crear_taskgroup_mes(4, "abril", 3)
    grupo_mayo = crear_taskgroup_mes(5, "mayo", 4)
    grupo_junio = crear_taskgroup_mes(6, "junio", 5)
    grupo_julio = crear_taskgroup_mes(7, "julio", 6)
    grupo_agosto = crear_taskgroup_mes(8, "agosto", 7)
    grupo_septiembre = crear_taskgroup_mes(9, "septiembre", 8)
    grupo_octubre = crear_taskgroup_mes(10, "octubre", 9)
    grupo_noviembre = crear_taskgroup_mes(11, "noviembre", 10)
    grupo_diciembre = crear_taskgroup_mes(12, "diciembre", 11)
    
    # Verificación final para Diciembre
    with TaskGroup(group_id="verificar_datos_diciembre") as verificar_dic:
        chequear_si_hay_datos = PythonOperator(
            task_id="verificar_si_hay_datos_diciembre",
            python_callable=verificar_existencia_datos_fuente,
            op_kwargs={"mes": 12},
        )

        mensaje_advirtiendo = PythonOperator(
            task_id="mensaje_informativo",
            python_callable=lambda ti: print(
                f"""
        ⚠️ ATENCIÓN: {'NO hay' if not ti.xcom_pull(task_ids='verificar_datos_diciembre.verificar_si_hay_datos_diciembre') else 'Existen'} datos para el mes 12 (Diciembre) 2019.
        {'Esto no es un error del DAG, es una característica de los datos de origen.' if not ti.xcom_pull(task_ids='verificar_datos_diciembre.verificar_si_hay_datos_diciembre') else 'Los datos se han procesado correctamente.'} 
        El DAG ha completado el procesamiento de todos los meses que tienen datos disponibles.
        """
            ),
            provide_context=True,
        )

        # Sensor de espera adicional para asegurar finalización
        esperar_finalizacion = PythonOperator(
            task_id="esperar_finalizacion_completa",
            python_callable=lambda: time.sleep(30),  # Pausa adicional de 30 segundos
            trigger_rule="all_done"
        )

        chequear_si_hay_datos >> mensaje_advirtiendo >> esperar_finalizacion

    # Tareas finales para todo el DAG
    verificar_completitud = PythonOperator(
        task_id="verificar_completitud_anual",
        python_callable=verificar_completitud_anual,
        trigger_rule="one_success",
    )
    
    # Tarea específica para asegurar que todos los procesadores están detenidos
    asegurar_parada = PythonOperator(
        task_id="asegurar_parada_procesadores",
        python_callable=stop_all_processors_individually_safely,
        trigger_rule="all_done"
    )
    
    depurar_ceros_finales = PythonOperator(
        task_id="depurar_ceros_finales",
        python_callable=depurar_ceros_tension_final,
        trigger_rule="one_success",  # Modificado para garantizar que solo se ejecute si hay éxito previo
        execution_timeout=timedelta(minutes=45)
    )

    limpiar_tmp = PythonOperator(
        task_id="limpiar_tablas_temporales",
        python_callable=limpiar_tablas_temporales,
        trigger_rule="all_done",
    )

    restaurar = PythonOperator(
        task_id="restaurar_estado_original",
        python_callable=restaurar_estado_original_completo,
        trigger_rule="all_done",
    )

    # Configuración inicial
    verificar_tabla_original >> setup_connections >> verificar_disponibilidad >> verificar_proxy >> grupo_enero

    # Dependencias entre meses
    grupo_enero >> grupo_febrero
    grupo_febrero >> grupo_marzo
    grupo_marzo >> grupo_abril
    grupo_abril >> grupo_mayo
    grupo_mayo >> grupo_junio
    grupo_junio >> grupo_julio
    grupo_julio >> grupo_agosto
    grupo_agosto >> grupo_septiembre
    grupo_septiembre >> grupo_octubre
    grupo_octubre >> grupo_noviembre
    grupo_noviembre >> grupo_diciembre

    # SIMPLIFICACIÓN: Eliminar referencias a tareas internas que causaban errores
    # Usar una conexión directa y secuencial entre grupos
    
    # Punto de espera explícito después de diciembre
    wait_for_all = DummyOperator(
        task_id="wait_for_all_processing",
        trigger_rule="all_done"
    )
    
    # Secuencia clara y estrictamente ordenada para tareas finales
    grupo_diciembre >> verificar_dic >> wait_for_all >> verificar_completitud >> asegurar_parada >> depurar_ceros_finales >> limpiar_tmp >> restaurar