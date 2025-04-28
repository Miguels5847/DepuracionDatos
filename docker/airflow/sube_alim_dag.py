"""
Tabla sube_alim
DAG para la integración de datos de potencia utilizando el proxy NiFi

Este DAG se encarga de orquestar el flujo de datos desde la tabla de origen
hasta la tabla de destino utilizando NiFi y Kafka como intermediarios.
Usa un proxy HTTP para comunicarse con NiFi de manera segura.
"""

from datetime import datetime, timedelta
import json
import requests
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
import psycopg2
# Configuración de parámetros
MINI_PROXY_URL = "http://mini_nifi_proxy_centrosur:5001"  # URL del proxy unificada
PROCESSOR_QUERY_DB_ID = "d71edb3c-bade-3de3-2e24-e5a5871f76a7"  # QueryDatabaseTable
PROCESSOR_CONSUME_KAFKA_ID = "cf55fd46-f49a-391b-2e8a-0467fbf85095"  # ConsumeKafka
PROCESSOR_PUT_DB_ID = "cc6e64c5-0011-3f14-607c-d1efd910c66a"  # PutDatabaseRecord
HTTP_TIMEOUT = 60  # Timeout para peticiones HTTP en segundos

# IDs de los grupos
PRODUCER_GROUP_ID = "1ac13d80-306b-3dd6-a4d1-795a4e1ce48e"  # Grupo Productor
CONSUMER_GROUP_ID = "a6c2c72a-fd2a-3aa5-90fe-82ae563cbbd0"  # Grupo Consumidor

# IDs de procesadores a verificar
SPLIT_JSON_ID = "cc6e64c5-0011-3f14-607c-d1efd910c66a"  # PutDatabaseRecord
EVALUATE_JSON_PATH_ID = "ea373288-a8da-3d8c-415e-f1cdf56f804c"  # EvaluateJsonPath
REPLACE_TEXT_ID = "aac7c267-6fbc-3efc-14f0-aeb886435463"  # ReplaceText

# Configuración del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'sube_alim_Dag',
    default_args=default_args,
    description='Pipeline mejorado de integración de datos de sube_alim usando proxy NiFi',
    schedule_interval=None,# Ejecución manual
    catchup=False,
    tags=['centrosur', 'potencia', 'nifi', 'proxy'],
)

# Funciones auxiliares
def verificar_proxy_disponible():
    """Verifica que el proxy NiFi esté disponible"""
    max_intentos = 5
    for intento in range(1, max_intentos + 1):
        try:
            print(f"Verificando disponibilidad del proxy NiFi (intento {intento}/{max_intentos})...")
            response = requests.get(f"{MINI_PROXY_URL}/health", timeout=HTTP_TIMEOUT)
            
            # Registrar respuesta completa para diagnóstico
            print(f"Respuesta del proxy: Status={response.status_code}, Contenido={response.text[:200]}...")
            
            if response.status_code == 200:
                data = response.json()
                # Verificar si el proxy reporta un estado saludable
                if data.get('status') == 'healthy' or data.get('auth_working'):
                    print(f"Proxy NiFi está disponible y reporta estado saludable")
                    print(f"Detalles adicionales: {json.dumps(data)}")
                    return True
                else:
                    print(f"Proxy disponible pero reporta estado: {data.get('status')}, mensaje: {data.get('message')}")
                    # Verificar información de autenticación y conexión con NiFi
                    if 'nifi_connected' in data:
                        print(f"Conexión con NiFi: {data.get('nifi_connected')}")
                    if 'auth_working' in data:
                        print(f"Autenticación funcionando: {data.get('auth_working')}")
                    if data.get('auth_working'):
                        return True
            else:
                print(f"Proxy no disponible (intento {intento}/{max_intentos}): {response.status_code}")
                print(f"Respuesta: {response.text[:200]}...")
        except requests.exceptions.ConnectionError as e:
            print(f"Error de conexión al proxy (intento {intento}/{max_intentos}): {str(e)}")
            print("Esto podría indicar que el servicio proxy no está en ejecución o no es accesible en la red.")
        except requests.exceptions.Timeout as e:
            print(f"Timeout al conectar con el proxy (intento {intento}/{max_intentos}): {str(e)}")
            print("Esto podría indicar que el servicio proxy está sobrecargado o no responde.")
        except Exception as e:
            print(f"Error al verificar proxy (intento {intento}/{max_intentos}): {str(e)}")
        
        if intento < max_intentos:
            print(f"Esperando 5 segundos antes de reintentar...")
            time.sleep(5)
    
    raise Exception("El proxy NiFi no está disponible después de múltiples intentos")

def search_and_update_processor_ids(**kwargs):
    """Busca los procesadores por nombre y guarda sus IDs en XCom"""
    results = {}
    
    # Lista de procesadores a buscar
    processors_to_find = [
        {"name": "SplitJson", "current_id": SPLIT_JSON_ID, "var_name": "SPLIT_JSON_ID"},
        {"name": "EvaluateJsonPath", "current_id": EVALUATE_JSON_PATH_ID, "var_name": "EVALUATE_JSON_PATH_ID"},
        {"name": "ReplaceText", "current_id": REPLACE_TEXT_ID, "var_name": "REPLACE_TEXT_ID"}
    ]
    
    updated_ids = {}
    
    for processor in processors_to_find:
        try:
            # Buscar por nombre del procesador
            response = requests.get(
                f"{MINI_PROXY_URL}/api/flow/search-results?q={processor['name']}",
                timeout=HTTP_TIMEOUT
            )
            
            if response.status_code == 200:
                search_results = response.json()
                
                # Procesar resultados para encontrar el ID correcto
                if 'searchResultsDTO' in search_results and 'processorResults' in search_results['searchResultsDTO']:
                    processor_results = search_results['searchResultsDTO']['processorResults']
                    
                    # Buscar el procesador en el grupo consumidor
                    for result in processor_results:
                        if result.get('groupId') == CONSUMER_GROUP_ID and result.get('name') == processor['name']:
                            # Encontramos el ID correcto
                            new_id = result.get('id')
                            results[processor['var_name']] = {
                                "old_id": processor['current_id'],
                                "new_id": new_id
                            }
                            updated_ids[processor['var_name']] = new_id
                            break
            else:
                results[f"error_{processor['name']}"] = f"Error en búsqueda: {response.status_code}"
        except Exception as e:
            results[f"error_{processor['name']}"] = f"Excepción en búsqueda: {str(e)}"
    
    # Guardar los IDs actualizados en XCom para que otras tareas puedan usarlos
    kwargs['ti'].xcom_push(key='updated_processor_ids', value=updated_ids)
    
    return f"Resultados de búsqueda y actualización de IDs: {json.dumps(results, indent=2)}"

def repair_processor(processor_name, processor_type, group_id, position_x, position_y, properties=None):
    """Elimina y recrea un procesador con la configuración proporcionada"""
    results = {}
    
    # Paso 1: Buscar el procesador actual por nombre
    try:
        search_response = requests.get(
            f"{MINI_PROXY_URL}/api/flow/search-results?q={processor_name}",
            timeout=HTTP_TIMEOUT
        )
        
        if search_response.status_code == 200:
            search_results = search_response.json()
            processor_id = None
            
            # Buscar el procesador en el grupo correcto
            if 'searchResultsDTO' in search_results and 'processorResults' in search_results['searchResultsDTO']:
                for result in search_results['searchResultsDTO']['processorResults']:
                    if result.get('groupId') == group_id and result.get('name') == processor_name:
                        processor_id = result.get('id')
                        break
            
            if processor_id:
                # Paso 2: Eliminar el procesador existente
                delete_response = requests.delete(
                    f"{MINI_PROXY_URL}/api/processors/{processor_id}?version=-1",
                    timeout=HTTP_TIMEOUT
                )
                
                if delete_response.status_code == 200:
                    results["delete"] = f"Procesador {processor_name} eliminado correctamente"
                else:
                    results["delete_error"] = f"Error al eliminar procesador: {delete_response.status_code}"
                    return results
            
            # Paso 3: Crear un nuevo procesador
            create_data = {
                "component": {
                    "name": processor_name,
                    "type": processor_type,
                    "position": {
                        "x": position_x,
                        "y": position_y
                    },
                    "properties": properties or {}
                },
                "revision": {
                    "version": 0
                }
            }
            
            create_response = requests.post(
                f"{MINI_PROXY_URL}/api/process-groups/{group_id}/processors",
                json=create_data,
                timeout=HTTP_TIMEOUT
            )
            
            if create_response.status_code == 201:
                new_processor = create_response.json()
                results["create"] = f"Procesador {processor_name} creado correctamente con ID: {new_processor.get('id')}"
                results["new_processor_id"] = new_processor.get('id')
            else:
                results["create_error"] = f"Error al crear procesador: {create_response.status_code} - {create_response.text}"
        else:
            results["search_error"] = f"Error en búsqueda: {search_response.status_code}"
    except Exception as e:
        results["exception"] = f"Excepción: {str(e)}"
    
    return results

def repair_split_json():
    """Repara el procesador SplitJson con la configuración correcta"""
    return repair_processor(
        "SplitJson", 
        "org.apache.nifi.processors.standard.SplitJson",
        CONSUMER_GROUP_ID,
        -1552, 104,  # Usar las mismas coordenadas del archivo JSON
        {
            "JsonPath Expression": "$[*]",  # Cambiado de $.* a $[*] para manejar arrays correctamente
            "Null Value Representation": "empty string", 
            "Max String Length": "5 MB"
        }
    )

def repair_evaluate_json_path():
    """Repara el procesador EvaluateJsonPath con la configuración correcta"""
    # Extraemos las propiedades del JSON original pero nos aseguramos que los nombres de campos estén correctos
    return repair_processor(
        "EvaluateJsonPath", 
        "org.apache.nifi.processors.standard.EvaluateJsonPath",
        CONSUMER_GROUP_ID,
        -112, -56,  # Usar las mismas coordenadas del archivo JSON
        {
            "Destination": "flowfile-attribute",
            "potencia_reactiva_real": "$.potencia_reactiva_real",
            "fecha": "$.fecha",
            "Max String Length": "20 MB",
            "potencia_activa_real": "$.potencia_activa_real",
            "hora": "$.hora",
            "Return Type": "auto-detect",
            "Null Value Representation": "empty string",
            "alimentador": "$.alimentador",  # Aseguramos que el nombre es correcto
            "Path Not Found Behavior": "warn",
            "dia_semana": "$.dia_semana"
        }
    )

def get_processor_status(processor_id):
    """Obtiene el estado actual de un procesador a través del mini-proxy"""
    try:
        response = requests.get(
            f"{MINI_PROXY_URL}/api/processors/{processor_id}", 
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            processor_data = response.json()
            return processor_data
        else:
            return {"error": f"Error al obtener estado del procesador: {response.status_code}", "raw_response": response.text}
    except Exception as e:
        return {"error": f"Error al consultar procesador: {str(e)}"}

def stop_processor(processor_id):
    """Detiene un procesador específico en NiFi"""
    try:
        # Primero obtenemos el estado actual y la revisión
        processor_data = get_processor_status(processor_id)
        
        # Verificar si el procesador existe
        if isinstance(processor_data, dict) and 'error' in processor_data:
            print(f"WARNING: El procesador {processor_id} no existe o hay un error: {processor_data['error']}")
            return f"Warning: Problema con procesador {processor_id}: {processor_data['error']}"
            
        # Extraemos la información necesaria
        current_state = processor_data.get('component', {}).get('state')
        revision = processor_data.get('revision', {})
        
        print(f"Estado actual del procesador {processor_id} para detener: {current_state}")
        
        if current_state == 'STOPPED' or current_state == 'DISABLED':
            return f"Procesador {processor_id} ya está detenido o deshabilitado"
        
        # Si no se pudo obtener el estado, devolver mensaje
        if not current_state:
            return f"No se pudo obtener el estado del procesador {processor_id} para detener"
            
        # Preparamos los datos para actualizar el estado
        update_data = {
            'revision': revision,
            'component': {
                'id': processor_id,
                'state': 'STOPPED'
            }
        }
        
        # Enviamos la solicitud para cambiar el estado
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{processor_id}",
            json=update_data,
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            return f"Procesador {processor_id} detenido correctamente"
        else:
            return f"Error al detener procesador: {response.status_code}"
    except Exception as e:
        return f"Excepción al detener procesador: {str(e)}"

def enable_controller_services_in_group(group_id):
    """Habilita todos los servicios de controlador en un grupo de procesadores"""
    try:
        # Obtener información del grupo
        response = requests.get(
            f"{MINI_PROXY_URL}/api/process-groups/{group_id}/controller-services",
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            services_data = response.json()
            if 'controllerServices' in services_data:
                results = {}
                for service in services_data['controllerServices']:
                    service_id = service.get('id')
                    service_name = service.get('component', {}).get('name')
                    
                    # Obtener el estado actual y revisión
                    service_detail = requests.get(
                        f"{MINI_PROXY_URL}/api/controller-services/{service_id}",
                        timeout=HTTP_TIMEOUT
                    )
                    
                    if service_detail.status_code == 200:
                        service_detail_data = service_detail.json()
                        revision = service_detail_data.get('revision', {})
                        
                        # Si no está habilitado, habilitarlo
                        if service_detail_data.get('component', {}).get('state') != 'ENABLED':
                            enable_data = {
                                'revision': revision,
                                'state': 'ENABLED'
                            }
                            
                            enable_response = requests.put(
                                f"{MINI_PROXY_URL}/api/controller-services/{service_id}/run-status",
                                json=enable_data,
                                timeout=HTTP_TIMEOUT
                            )
                            
                            if enable_response.status_code == 200:
                                results[service_name] = "Habilitado correctamente"
                            else:
                                results[service_name] = f"Error al habilitar: {enable_response.status_code}"
                        else:
                            results[service_name] = "Ya está habilitado"
                
                return f"Resultado de habilitación de servicios: {json.dumps(results, indent=2)}"
            else:
                return "No se encontraron servicios de controlador en el grupo"
        else:
            return f"Error al obtener servicios de controlador: {response.status_code}"
    except Exception as e:
        return f"Excepción al habilitar servicios de controlador: {str(e)}"

def start_nifi_processor(processor_id):
    """Inicia un procesador específico en NiFi a través del mini-proxy"""
    try:
        # Primero obtenemos el estado actual y la revisión
        processor_data = get_processor_status(processor_id)
        
        # Verificar si el procesador existe
        if isinstance(processor_data, dict) and 'error' in processor_data:
            print(f"ERROR: El procesador {processor_id} no existe o hay un error: {processor_data['error']}")
            return f"Error: Problema con procesador {processor_id}: {processor_data['error']}"
            
        # Extraemos la información necesaria
        current_state = processor_data.get('component', {}).get('state')
        revision = processor_data.get('revision', {})
        processor_name = processor_data.get('component', {}).get('name', 'Desconocido')
        
        print(f"Estado actual del procesador {processor_id} ({processor_name}): {current_state}")
        
        if current_state == 'RUNNING':
            return f"Procesador {processor_id} ({processor_name}) ya está en estado RUNNING"
        
        # Si no se pudo obtener el estado, mostrar más información
        if not current_state:
            print(f"No se pudo obtener el estado del procesador. Datos recibidos: {json.dumps(processor_data)}")
            return f"No se pudo iniciar el procesador {processor_id}: estado desconocido"
            
        # Preparamos los datos para actualizar el estado
        update_data = {
            'revision': revision,
            'component': {
                'id': processor_id,
                'state': 'RUNNING'
            }
        }
        
        # Enviamos la solicitud para cambiar el estado
        response = requests.put(
            f"{MINI_PROXY_URL}/api/processors/{processor_id}",
            json=update_data,
            timeout=HTTP_TIMEOUT
        )
        
        print(f"Respuesta al intentar iniciar procesador: Código {response.status_code}")
        
        if response.status_code == 200:
            # Verificamos que el cambio de estado fue efectivo
            time.sleep(2)  # Esperar un poco para que el cambio se aplique
            new_status = get_processor_status(processor_id)
            new_state = new_status.get('component', {}).get('state')
            print(f"Estado NUEVO del procesador {processor_id}: {new_state}")
            
            if new_state == 'RUNNING':
                return f"Procesador {processor_id} ({processor_name}) iniciado correctamente y verificado"
            else:
                return f"Procesador {processor_id} ({processor_name}) enviada solicitud pero estado actual es {new_state}"
        else:
            return f"Error al iniciar procesador: {response.status_code} - {response.text[:200]}"
    except Exception as e:
        return f"Error al iniciar el procesador NiFi: {str(e)}"

def stop_producer_processors():
    """Detiene todos los procesadores del flujo productor"""
    results = {}
    
    # Lista de IDs de procesadores del productor (en orden inverso para detenerlos correctamente)
    producer_processors = [
        {"id": "58a17b60-0196-1000-a634-9a549634cd57", "name": "PutDatabaseRecord"},
        {"id": "8c41a4d7-f907-367a-5fd2-6ef8ea57d5d2", "name": "PublishKafka"},
        {"id": "d182bc33-6e77-3854-5ff1-4dac01c124c7", "name": "ConvertRecord"},
        {"id": "0c9f373a-d024-31e0-6507-06cdd09686d9", "name": "QueryDatabaseTable"},
    ]
    
    # Detener todos los procesadores
    for processor in producer_processors:
        try:
            stop_result = stop_processor(processor["id"])
            results[f"stop_{processor['name']}"] = stop_result
            time.sleep(1)
        except Exception as e:
            results[f"stop_{processor['name']}"] = f"Error: {str(e)}"
    
    return f"Resultados de detención de procesadores productores: {json.dumps(results, indent=2)}"


def start_producer_processors():
    """Inicia todos los procesadores del flujo productor en orden de dependencia"""
    results = {}
    
    # Habilitar servicios de controlador en el grupo productor
    controller_services_result = enable_controller_services_in_group(PRODUCER_GROUP_ID)
    results["habilitar_servicios_productor"] = controller_services_result
    
    # Lista de IDs de procesadores del productor 
    producer_processors = [
        {"id": "0c9f373a-d024-31e0-6507-06cdd09686d9", "name": "QueryDatabaseTable"},
        {"id": "d182bc33-6e77-3854-5ff1-4dac01c124c7", "name": "ConvertRecord"},
        {"id": "8c41a4d7-f907-367a-5fd2-6ef8ea57d5d2", "name": "PublishKafka"},
        {"id": "4e371b80-40d2-3db0-3d2c-858eb2748471", "name": "PutdatabaseRecord"},
    ]
    
    # Primero detener todos los procesadores para asegurar un inicio limpio
    for processor in reversed(producer_processors):
        try:
            stop_result = stop_processor(processor["id"])
            results[f"stop_{processor['name']}"] = stop_result
            time.sleep(1)
        except Exception as e:
            results[f"stop_{processor['name']}"] = f"Error: {str(e)}"
    
    # Esperar un poco para que todos se detengan
    time.sleep(5)
    
    # Luego iniciar todos los procesadores en el orden correcto
    for processor in producer_processors:
        try:
            start_result = start_nifi_processor(processor["id"])
            results[f"start_{processor['name']}"] = start_result
            time.sleep(3)
        except Exception as e:
            results[f"start_{processor['name']}"] = f"Error: {str(e)}"
    
    return f"Resultados de reinicio de procesadores productores: {json.dumps(results, indent=2)}"

def start_consumer_processors(**kwargs):
    """Inicia todos los procesadores del flujo consumidor en orden de dependencia"""
    ti = kwargs['ti']
    updated_ids = ti.xcom_pull(task_ids='diagnose_processors', key='updated_processor_ids')
    
    # Utilizar los IDs actualizados si están disponibles
    split_json_id = updated_ids.get('SPLIT_JSON_ID', SPLIT_JSON_ID) if updated_ids else SPLIT_JSON_ID
    evaluate_json_path_id = updated_ids.get('EVALUATE_JSON_PATH_ID', EVALUATE_JSON_PATH_ID) if updated_ids else EVALUATE_JSON_PATH_ID
    replace_text_id = updated_ids.get('REPLACE_TEXT_ID', REPLACE_TEXT_ID) if updated_ids else REPLACE_TEXT_ID
    
    results = {}
    
    # Habilitar servicios de controlador en el grupo consumidor
    controller_services_result = enable_controller_services_in_group(CONSUMER_GROUP_ID)
    results["habilitar_servicios_consumidor"] = controller_services_result
    
    # Lista de procesadores del consumidor (tomados del JSON)
    consumer_processors = [
        {"id": "28c8c5ef-4956-352e-8e58-83cdf3e65e0a", "name": "ConsumeKafka"},
        {"id": evaluate_json_path_id, "name": "EvaluateJsonPath"},
        {"id": replace_text_id, "name": "ReplaceText"},
        {"id": "209408b7-91c7-3058-b0ba-070fb49f2759", "name": "PutDatabaseRecord"}
    ]
    
    # Primero detener todos los procesadores para asegurar un inicio limpio
    for processor in reversed(consumer_processors):
        try:
            stop_result = stop_processor(processor["id"])
            results[f"stop_{processor['name']}"] = stop_result
            time.sleep(1)
        except Exception as e:
            results[f"stop_{processor['name']}"] = f"Error: {str(e)}"
    
    # Esperar un poco para que todos se detengan
    time.sleep(5)
    
    # Luego iniciar todos los procesadores en el orden correcto
    for processor in consumer_processors:
        try:
            start_result = start_nifi_processor(processor["id"])
            results[f"start_{processor['name']}"] = start_result
            time.sleep(3)
        except Exception as e:
            results[f"start_{processor['name']}"] = f"Error: {str(e)}"
    
    # Detener los procesadores del productor después de iniciar los consumidores
    print("Iniciando detención de procesadores productores...")
    stop_producers_result = stop_producer_processors()
    results["stop_producers"] = stop_producers_result
    
    return f"Resultados de reinicio de procesadores consumidores y detención de productores: {json.dumps(results, indent=2)}"
def contar_registros_tabla(**kwargs):
    """Detiene los procesadores del productor y consumidor, y verifica los registros en la tabla de destino"""
    
    # Primero detener los procesadores del productor 
    print("Asegurando que los procesadores productores estén detenidos...")
    stop_result_productor = stop_producer_processors()
    print(stop_result_productor)
    
    # Detener también los procesadores consumidores
    print("Deteniendo procesadores consumidores...")
    ti = kwargs.get('ti', None)
    updated_ids = ti.xcom_pull(task_ids='diagnose_processors', key='updated_processor_ids') if ti else None
    
    # Utilizar los IDs actualizados si están disponibles
    split_json_id = updated_ids.get('SPLIT_JSON_ID', SPLIT_JSON_ID) if updated_ids else SPLIT_JSON_ID
    evaluate_json_path_id = updated_ids.get('EVALUATE_JSON_PATH_ID', EVALUATE_JSON_PATH_ID) if updated_ids else EVALUATE_JSON_PATH_ID
    replace_text_id = updated_ids.get('REPLACE_TEXT_ID', REPLACE_TEXT_ID) if updated_ids else REPLACE_TEXT_ID
    
    # Lista de procesadores del consumidor
    consumer_processors = [
        {"id": "209408b7-91c7-3058-b0ba-070fb49f2759", "name": "PutDatabaseRecord"},
        {"id": replace_text_id, "name": "ReplaceText"},
        {"id": evaluate_json_path_id, "name": "EvaluateJsonPath"},
        {"id": "28c8c5ef-4956-352e-8e58-83cdf3e65e0a", "name": "ConsumeKafka"}
    ]
    
    # Detener procesadores consumidores
    for processor in consumer_processors:
        try:
            stop_result = stop_processor(processor["id"])
            print(f"Detenido {processor['name']}: {stop_result}")
            time.sleep(1)
        except Exception as e:
            print(f"Error al detener {processor['name']}: {str(e)}")
    
    # Esperar un poco para asegurar que todas las operaciones DB se completen
    time.sleep(10)
    
    try:
        # Conexión a la base de datos
        print("Conectando a la base de datos PostgreSQL...")
        conn = psycopg2.connect(
            host="postgres_centrosur",
            database="base_prueba",
            user="postgres",
            password="1150040812",
            connect_timeout=30  # Timeout para conexión DB
        )
        
        # Consultar cantidad de registros - Modificado para verificar todas las posibles tablas
        cursor = conn.cursor()
        possible_tables = ['sube_alim_processed', 'SUBE_ALIM_PROCESSED', 'public.sube_alim_processed']
        count = 0
        used_table = None
        
        for table in possible_tables:
            try:
                print(f"Intentando consultar tabla: {table}")
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                used_table = table
                if count > 0:
                    break  # Si encontramos registros, salimos del bucle
            except Exception as e:
                print(f"Error consultando tabla {table}: {str(e)}")
                continue
        
        # Opcional: obtener muestra de datos si hay registros
        sample_data = []
        if count > 0 and used_table:
            print(f"Obteniendo muestra de datos de {used_table}...")
            cursor.execute(f"SELECT * FROM {used_table} LIMIT 5")
            sample_columns = [desc[0] for desc in cursor.description]
            sample_data = cursor.fetchall()
            
            # También mostrar esquemas y tablas disponibles para depuración
            cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")
            all_tables = cursor.fetchall()
            print(f"Todas las tablas disponibles: {all_tables}")
        else:
            # Si no encontramos datos, listar todas las tablas para depuración
            print("No se encontraron registros. Listando todas las tablas...")
            cursor.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('pg_catalog', 'information_schema')")
            all_tables = cursor.fetchall()
            print(f"Tablas disponibles: {all_tables}")
        
        # Cerrar conexión
        cursor.close()
        conn.close()
        
        # Generar mensaje de resultado
        resultado = f"Registros en tabla de destino {used_table or 'sube_alim_processed'}: {count}"
        
        # Añadir muestra de datos si se obtuvo
        if sample_data:
            resultado += f"\n\nMuestra de datos (primeros 5 registros):\n"
            resultado += f"Columnas: {', '.join(sample_columns)}\n"
            for row in sample_data:
                resultado += f"{row}\n"
        
        return resultado
    except Exception as e:
        return f"Error al contar registros: {str(e)}"
def limpiar_colas_nifi():
    """Limpia las colas (connections) en NiFi usando el endpoint simplificado del mini-proxy"""
    print("Iniciando limpieza de colas en NiFi usando endpoint unificado...")
    
    try:
        # Lista de conexiones conocidas para incluir en la solicitud
        connections = [
            # Conexiones del grupo productor
            {"id": "4dcc0bed-9061-3a4c-e930-c375f77615eb", "name": "QueryDB-to-LogAttribute", "group": "Productor"},
            {"id": "723a3709-dabb-3fae-b467-2892da95b9a6", "name": "LogAttribute-to-ConvertRecord", "group": "Productor"},
            {"id": "42f8934e-7af2-39e2-e0ff-8db121f90208", "name": "ConvertRecord-to-LogAttribute", "group": "Productor"},
            {"id": "da3c24c2-2c5b-3fd1-28dd-28d8ecf5b827", "name": "LogAttribute-to-PublishKafka", "group": "Productor"},
            {"id": "1f03f57d-f9b9-38ef-9f1b-d8511b663819", "name": "PublishKafka-to-LogAttribute", "group": "Productor"},
            {"id": "e9cb565f-d631-392c-cd11-e65aa719180f", "name": "LogAttribute-to-success", "group": "Productor"},
            
            # Conexiones del grupo consumidor
            {"id": "3724acc6-adcb-323c-46b2-34915d2c68af", "name": "ConsumeKafka-to-LogAttribute", "group": "Consumidor"},
            {"id": "285c7970-148d-3ef0-7e15-8e4bd0c7e92f", "name": "LogAttribute-to-SplitJson", "group": "Consumidor"},
            {"id": "fdec765f-29a0-3359-7287-99df2f0e7922", "name": "SplitJson-to-LogAttribute", "group": "Consumidor"},
            {"id": "55b03c9a-783e-3234-a008-3fc6e723ec52", "name": "LogAttribute-to-EvaluateJsonPath", "group": "Consumidor"},
            {"id": "15965cd6-7f38-342a-9e16-001a8abff5ed", "name": "EvaluateJsonPath-to-ReplaceText", "group": "Consumidor"},
            {"id": "1999fec5-2a9e-333b-167e-de1bf204e4d7", "name": "ReplaceText-to-LogAttribute", "group": "Consumidor"},
            {"id": "d2f78d49-b967-3e3a-6a3d-2387ef8131a1", "name": "LogAttribute-to-PutDatabaseRecord", "group": "Consumidor"},
            {"id": "f403830e-56b4-3a7f-6148-fb30015edce9", "name": "PutDatabaseRecord-to-success", "group": "Consumidor"},
        ]
        
        # Usar el nuevo endpoint unificado
        response = requests.post(
            f"{MINI_PROXY_URL}/api/clean-all-queues", 
            json={"connections": connections},
            timeout=HTTP_TIMEOUT
        )
        
        if response.status_code == 200:
            result = response.json()
            print(f"Respuesta de limpieza de colas: {json.dumps(result)}")
            return f"Limpieza de colas iniciada correctamente para {len(connections)} conexiones"
        else:
            error_msg = f"Error al limpiar colas: HTTP {response.status_code}"
            print(f"{error_msg}: {response.text[:200]}")
            return error_msg
    except Exception as e:
        error_msg = f"Excepción al limpiar colas: {str(e)}"
        print(error_msg)
        return error_msg
# Definición de tareas
with dag:
    # Verificación inicial del proxy
    verificar_proxy = PythonOperator(
        task_id='verificar_proxy',
        python_callable=verificar_proxy_disponible,
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    
    # Diagnóstico de procesadores
    diagnose_processors = PythonOperator(
        task_id='diagnose_processors',
        python_callable=search_and_update_processor_ids,
        provide_context=True,
    )
    
    # Reparación de SplitJson
    repair_split_json_task = PythonOperator(
        task_id='repair_split_json',
        python_callable=repair_split_json,
    )
    
    # Reparación de EvaluateJsonPath
    repair_evaluate_json_path_task = PythonOperator(
        task_id='repair_evaluate_json_path',
        python_callable=repair_evaluate_json_path,
    )
    
    # Iniciar grupo productor
    start_producer_task = PythonOperator(
        task_id='start_producer_processors',
        python_callable=start_producer_processors,
    )
    
    # Esperar a que se procesen los datos en Kafka
    wait_for_processing = BashOperator(
        task_id='wait_for_processing',
        bash_command='sleep 90',  # Esperar 2 minutos
    )
    
    # Iniciar grupo consumidor
    start_consumer_task = PythonOperator(
        task_id='start_consumer_processors',
        python_callable=start_consumer_processors,
        provide_context=True,
    )
    
    wait_for_consumer_processing = BashOperator(
    task_id='wait_for_consumer_processing',
    bash_command='sleep 90',  # Esperar 3 minutos
    )
    # Verificar resultados
    verificar_resultados = PythonOperator(
        task_id='verificar_resultados',
        python_callable=contar_registros_tabla,
        provide_context=True,  # Agregar este parámetro para pasar kwargs
    )
    # Limpiar colas NiFi
    limpiar_colas = PythonOperator(
        task_id='limpiar_colas_nifi',
        python_callable=limpiar_colas_nifi,
    )

    
    # Definir dependencias del DAG
    verificar_proxy >> diagnose_processors >> repair_split_json_task >> repair_evaluate_json_path_task >> start_producer_task >> wait_for_processing >> start_consumer_task >> wait_for_consumer_processing >> verificar_resultados >> limpiar_colas

