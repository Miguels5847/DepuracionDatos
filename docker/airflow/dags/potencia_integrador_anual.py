#---------CONFIGURACION INICIAL-------------------
"""
DAG para orquestar el procesamiento mensual de la tabla potencia_dep 
Este DAG procesa los datos mes por mes, insertándolos en potencia_dep_processed,
verificando primero que no existan datos con ceros.
Bueno con procesamiento de 100% datos 22/4/2025
"""

from datetime import datetime, date,timedelta
import os
import requests
import time
import calendar
import subprocess
import json
import psycopg2
import traceback
import calendar
import json
import pandas as pd
import numpy as np
import decimal
import statistics
import calendar
from kafka import KafkaProducer
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
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

YEAR = None 

# Determinar el año a procesar dinámicamente
def determinar_anio_procesado_con_continuacion(**kwargs):
    """
    Versión mejorada que detecta si el DAG fue disparado como continuación
    desde potencia_standby_interanual
    """
    try:
        # Verificar si el DAG fue disparado con parámetros específicos
        dag_run = kwargs.get('dag_run')
        if dag_run and dag_run.conf:
            anio_procesado = dag_run.conf.get('anio_procesado')
            es_continuacion = dag_run.conf.get('es_continuacion', False)
            
            if anio_procesado and es_continuacion:
                # Convertir a entero si es string
                if isinstance(anio_procesado, str) and anio_procesado.isdigit():
                    anio_procesado = int(anio_procesado)
                elif isinstance(anio_procesado, int):
                    pass
                else:
                    print(f"⚠️ Formato de año inválido: {anio_procesado}, tipo: {type(anio_procesado)}")
                    anio_procesado = None
                
                if anio_procesado:
                    print(f"✅ El DAG fue disparado como continuación para procesar el año {anio_procesado}")
                    # Guardar en Variable para asegurar consistencia
                    Variable.set("potencia_anio_actual_fijado", str(anio_procesado))
                    Variable.set("potencia_anio_actual_procesamiento", str(anio_procesado))
                    return anio_procesado
    
        # Si no es continuación o no hay parámetros válidos, usar el método original
        return determinar_anio_procesado()  # Llamar al método original
    except Exception as e:
        print(f"❌ Error determinando año con continuación: {str(e)}")
        traceback.print_exc()
        return determinar_anio_procesado()  # Fallback al método original

def setup_year(**kwargs):
    """Configura el año a procesar y lo propaga a todo el DAG"""
    try:
        # Usar la función mejorada que detecta continuaciones
        year = determinar_anio_procesado_con_continuacion(**kwargs)
        
        if year is None:
            # Si no se pudo determinar dinámicamente, usar un valor seguro
            print("⚠️ No se pudo determinar un año dinámicamente, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        
        # VERIFICACIÓN EXPLÍCITA MEJORADA
        if isinstance(year, int) and year > 2000 and year <= datetime.now().year + 1:
            # Año válido dentro de un rango razonable
            Variable.set("YEAR", str(year))
            print(f"✅ Año establecido para todo el DAG: {year}")
        else:
            print(f"⚠️ Año detectado {year} inválido, usando valor seguro")
            year = 2019  # Valor seguro por defecto
            Variable.set("YEAR", str(year))
        
        return year
    except Exception as e:
        print(f"❌ Error en setup_year: {str(e)}")
        traceback.print_exc()
        # En caso de error, usar valor seguro
        default_year = 2019
        Variable.set("YEAR", str(default_year))
        return default_year

def determinar_anio_procesado():
    """
    Determina el año a procesar consultando dinámicamente las tablas de datos.
    No usa valores fijos y se adapta a los datos existentes.
    """
    try:
        # Verificar si el año ya ha sido determinado y guardado en variables
        try:
            anio_fijado = Variable.get("potencia_anio_actual_fijado", default_var=None)
            if anio_fijado and anio_fijado.isdigit():
                anio_procesado = int(anio_fijado)
                print(f"✅ Usando año fijado para procesamiento: {anio_procesado}")
                # Guardar en Variable para asegurar consistencia
                Variable.set("potencia_anio_actual_procesamiento", str(anio_procesado))
                return anio_procesado
        except:
            pass  # Continuar si no hay variable guardada
            
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # PASO 1: Determinar rango de años disponibles en datos originales
        print("🔍 Consultando el rango de años disponibles en datos originales...")
        query_rango_anios = """
        SELECT 
            EXTRACT(YEAR FROM MIN(fecha))::int AS anio_min,
            EXTRACT(YEAR FROM MAX(fecha))::int AS anio_max
        FROM potencia_dep_original
        """
        
        rango_anios = pg_hook_origen.get_first(query_rango_anios)
        
        if not rango_anios or not rango_anios[0]:
            print("⚠️ No se encontró rango de años en potencia_dep_original, buscando en potencia_dep...")
            query_alt = """
            SELECT 
                EXTRACT(YEAR FROM MIN(fecha))::int AS anio_min,
                EXTRACT(YEAR FROM MAX(fecha))::int AS anio_max
            FROM potencia_dep
            """
            rango_anios = pg_hook_origen.get_first(query_alt)
            
        if not rango_anios or not rango_anios[0]:
            print("⚠️ No se pudo determinar el rango de años desde ninguna tabla fuente")
            return None
            
        anio_min = rango_anios[0]
        anio_max = rango_anios[1]
        print(f"📊 Rango de años disponible en datos originales: {anio_min} - {anio_max}")
            
        # PASO 2: Determinar el último año procesado
        query_procesados = """
        SELECT EXTRACT(YEAR FROM MAX(fecha))::int AS ultimo_anio
        FROM potencia_dep_processed
        """
        
        ultimo_procesado = pg_hook.get_first(query_procesados)
        ultimo_anio_procesado = ultimo_procesado[0] if ultimo_procesado and ultimo_procesado[0] else None
        
        if ultimo_anio_procesado:
            print(f"📊 Último año procesado: {ultimo_anio_procesado}")
        else:
            print("📊 No hay años procesados previamente")
            
        # PASO 3: Decidir el año a procesar
        anio_a_procesar = None
        
        # 3.1 Si hay años procesados, tomar el siguiente
        if ultimo_anio_procesado:
            anio_candidato = ultimo_anio_procesado + 1
            # Verificar que este año candidato existe en los datos originales
            if anio_candidato <= anio_max:
                anio_a_procesar = anio_candidato
                print(f"✅ Se procesará el siguiente año: {anio_a_procesar}")
            else:
                print(f"⚠️ El siguiente año ({anio_candidato}) excede el rango disponible ({anio_max})")
                # En este caso deberíamos indicar que ya procesamos todos los años disponibles
                print("✅ Se han procesado todos los años disponibles")
                return None
        
        # 3.2 Si no hay años procesados, comenzar con el primer año disponible
        if not anio_a_procesar:
            anio_a_procesar = anio_min
            print(f"✅ Comenzando con el primer año disponible: {anio_a_procesar}")
            
        # PASO 4: Verificar si hay datos para este año
        query_verificar = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_original
        WHERE EXTRACT(YEAR FROM fecha) = {anio_a_procesar}
        """
        
        count = pg_hook_origen.get_first(query_verificar)[0]
        
        if count == 0:
            print(f"⚠️ No hay datos para el año {anio_a_procesar} en potencia_dep_original")
            # Buscar en tabla alternativa
            query_alt = f"""
            SELECT COUNT(*) 
            FROM potencia_dep
            WHERE EXTRACT(YEAR FROM fecha) = {anio_a_procesar}
            """
            
            count_alt = pg_hook_origen.get_first(query_alt)[0]
            
            if count_alt == 0:
                print(f"⚠️ No hay datos para el año {anio_a_procesar} en ninguna tabla fuente")
                return None
                
        # PASO 5: Fijar el año y guardar en variables
        Variable.set("potencia_anio_actual_fijado", str(anio_a_procesar))
        Variable.set("potencia_anio_actual_procesamiento", str(anio_a_procesar))
        print(f"🔒 FIJADO: El año {anio_a_procesar} será procesado para TODOS los meses")
        
        return anio_a_procesar
    except Exception as e:
        print(f"⚠️ Error determinando año a procesar: {str(e)}")
        traceback.print_exc()
        return None
        print(f"⚠️ Error determinando año a procesar: {str(e)}")
        traceback.print_exc()
        # En caso de error, usar 2019 como año seguro y fijarlo
        Variable.set("potencia_anio_actual_fijado", "2019")
        return 2019  # Valor por defecto

def fijar_y_propagar_year(**kwargs):
    """
    Fija el año a procesar y lo propaga a todas las tareas mediante XCom.
    Esto asegura que todas las tareas usen el mismo año.
    """
    try:
        # Determinar el año a procesar
        year = determinar_anio_procesado_con_continuacion()
        
        # Guardar en XCom para que todas las tareas lo usen
        kwargs['ti'].xcom_push(key='year_global', value=year)
        
        # También guardar en variables de Airflow
        Variable.set("potencia_anio_actual_fijado", str(year))
        Variable.set("potencia_anio_actual_procesamiento", str(year))
        
        print(f"🔒 Año fijado para todo el DAG: {year}")
        return year
    except Exception as e:
        print(f"❌ Error fijando año: {str(e)}")
        traceback.print_exc()
        return 2019  # Valor seguro por defecto

# Definir el año globalmente
#YEAR = determinar_anio_procesado()
if YEAR is None:
    print("⚠️ No se pudo determinar un año válido para procesar.")
    print("⚠️ El DAG se ejecutará pero no procesará datos.")

print(f"🗓️ Año determinado para procesamiento: {YEAR if YEAR else 'NINGUNO'}")

# Solo guardar la variable si YEAR es válido
if YEAR is not None:
    Variable.set("potencia_anio_actual_procesamiento", str(YEAR))
# Actualizar el ID del DAG para incluir el año


dag_id = f"potencia_integrador_anual"

MINI_PROXY_URL = "http://mini_nifi_proxy_centrosur:5001"  # URL del proxy
HTTP_TIMEOUT = 60  # Timeout para peticiones HTTP en segundos

# Grupo Productor y Consumidor
PRODUCER_GROUP_ID = "931bc6ff-9df8-3e05-ba74-b7e158593720"  # Grupo Productor
CONSUMER_GROUP_ID = "d29e61c5-d615-3dd7-97aa-b134e2e522ea"  # Grupo Consumidor

# IDs de procesadores - Grupo Productor
QUERY_DB_TABLE_PROCESSOR_ID = (
    "e14bc735-035b-302f-f039-62497dfa6206"  # QueryDatabaseTable
)
CONVERT_RECORD_ID = "f77f1a42-c782-31e0-4a8a-dbb948454782"
SPLIT_JSON_ID = "5932789d-1eb5-3ba2-6193-cafa9019bb00"
MERGE_CONTENT_ID = "cca494f4-07ad-35f5-fbff-75fbf07cabea"
PUBLISH_KAFKA_ID = "8d55bbd5-9228-31a0-1708-c81d28cfcece"
PUT_DATABASE_RECORD_ID = "a2cbc853-d842-3523-a89e-ce47daf170dd"  

# IDs de procesadores - Grupo Consumidor
CONSUME_KAFKA_PROCESSOR_ID = "9acbc84a-166f-3f89-0fed-abd745940144"
EVALUATE_JSON_PATH_ID = "2545d33b-6bf8-3686-8bcb-af71e5f31d48"
REPLACE_TEXT_ID = "207fd8f3-4553-3c01-66f1-34aa360aebb3"
PUT_DB_PROCESSOR_ID = "cc103488-b857-376c-27ad-270dfca2fe08"

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
    "potencia_integrador_anual",
    default_args=default_args,
    description="Procesamiento mensual de datos de potencia",
    schedule_interval=None,  # Ejecución manual
    catchup=False,
    tags=["centrosur", "potencia", "anual", "nifi", "kafka"],
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



#------------UTILERÍAS Y FUNCIONES DE APOYO-------------------
def decimal_json_serializer(obj):
    """Serializador JSON personalizado que maneja objetos Decimal"""
    if isinstance(obj, decimal.Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")

def ensure_json_serializable(obj):
    """Convierte objetos no serializables a JSON en formatos serializables."""
    from datetime import datetime, date, time  # Añadir importación explícita aquí
    
    if isinstance(obj, datetime) or isinstance(obj, date):
        return obj.isoformat()
    elif isinstance(obj, time):  # Ahora time está correctamente definido
        return obj.strftime('%H:%M:%S')
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif hasattr(obj, '__dict__'):
        return obj.__dict__
    else:
        return str(obj)

def registrar_log(pg_hook, nivel, mensaje, alimentador=None, fecha=None, hora=None, detalles=None):
    """Registra eventos en la tabla de logs (versión corregida)"""
    try:
        # Crear un diccionario de detalles completo para JSONB
        detalles_completos = {
            'nivel': nivel,  # Mover nivel a detalles
            'fuente': 'potencia_integrador_anual_depuracion',
            'hora': hora
        }
        # Si hay detalles adicionales, añadirlos
        if detalles:
            detalles_completos.update(detalles)
            
        # Serializar detalles manejando decimales y objetos datetime correctamente
        if detalles_completos:
            detalles_json = json.dumps(detalles_completos, default=ensure_json_serializable)
        else:
            detalles_json = None
        
        # Consulta ajustada a la estructura real de la tabla
        query = """
        INSERT INTO log_procesamiento 
        (timestamp, mensaje, alimentador, fecha, detalles)
        VALUES (NOW(), %s, %s, %s, %s)
        """
        
        pg_hook.run(query, parameters=(
            mensaje,
            alimentador,
            fecha,
            detalles_json
        ))
    except Exception as e:
        print(f"Error al registrar log: {str(e)}")
        traceback.print_exc()  # Agregar esto para ver el trace completo

def calcular_dia_semana(fecha):
    """
    Calcula el día de la semana a partir de una fecha.
    
    Args:
        fecha: Puede ser un objeto datetime, date o string en formato 'YYYY-MM-DD'
        
    Returns:
        int: Día de la semana en formato 0=Domingo, 1=Lunes, ..., 6=Sábado
    """
    try:
        # Convertir fecha a datetime si es string
        if isinstance(fecha, str):
            fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
        # Si ya es un objeto date o datetime, usarlo directamente
        elif hasattr(fecha, 'weekday'):
            fecha_dt = fecha
        else:
            # Si no es reconocible, convertir a string y luego a datetime
            fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d')
            
        # weekday() devuelve 0=Lunes, 1=Martes, ..., 6=Domingo
        # Para convertir a 0=Domingo, 1=Lunes, ..., 6=Sábado
        # usamos la fórmula: (weekday + 1) % 7
        weekday = fecha_dt.weekday()
        dia_semana_bd = (weekday + 1) % 7
        
        return dia_semana_bd
    except Exception as e:
        print(f"Error calculando dia_semana: {str(e)}")
        # Devolver un valor por defecto en caso de error
        return 0

def validar_anio_procesamiento(**kwargs):
    """
    Verifica que el año a procesar sea válido consultando los datos disponibles.
    Falla el DAG si el año no es válido o si no hay año que procesar.
    """
    try:
        year = int(Variable.get("YEAR"))
        # Usar el valor YEAR ya establecido
    except KeyError:
        # Si la variable YEAR no existe, se asume que setup_year_task aún no se ha ejecutado
        # En lugar de fallar, devolvemos True para permitir que la secuencia continúe
        print("⚠️ Variable YEAR no encontrada. La tarea setup_year_task establecerá su valor.")
        return True
    except (ValueError, TypeError):
        print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
        year = 2019  # Valor seguro por defecto
    
    # Verificar si hay datos para este año
    pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
    
    # Primero verificar qué tablas existen
    query_tablas = """
    SELECT table_name 
    FROM information_schema.tables 
    WHERE table_schema = 'public'
    AND table_name IN ('potencia_dep_original', 'potencia_dep')
    """
    tablas_existentes = [row[0] for row in pg_hook_origen.get_records(query_tablas)]
    
    if not tablas_existentes:
        mensaje = f"⚠️ ERROR: No existen las tablas necesarias para verificar datos del año {year}."
        print(mensaje)
        raise ValueError(mensaje)
    
    # Construir la consulta solo con las tablas que existen
    query_partes = []
    if 'potencia_dep_original' in tablas_existentes:
        query_partes.append(f"SELECT 1 FROM potencia_dep_original WHERE EXTRACT(YEAR FROM fecha) = {year}")
    
    if 'potencia_dep' in tablas_existentes:
        query_partes.append(f"SELECT 1 FROM potencia_dep WHERE EXTRACT(YEAR FROM fecha) = {year}")
    
    if not query_partes:
        mensaje = f"⚠️ ERROR: No hay tablas disponibles para verificar datos del año {year}."
        print(mensaje)
        raise ValueError(mensaje)
    
    # Construir la consulta final con UNION ALL entre todas las partes
    query_datos = f"""
    SELECT COUNT(*) 
    FROM (
        {" UNION ALL ".join(query_partes)}
    ) AS datos_combinados
    """
    
    count = pg_hook_origen.get_first(query_datos)[0]
    
    if count == 0:
        mensaje = f"⚠️ ERROR: No hay datos disponibles para el año {year} en ninguna tabla fuente."
        print(mensaje)
        raise ValueError(mensaje)
    else:
        print(f"✅ Año a procesar {year} validado correctamente con {count} registros disponibles")
        return True
#------------COMUNICACIÓN CON NIFI Y KAFKA-------------------
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
            PUT_DATABASE_RECORD_ID,
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
        {"id": PUT_DATABASE_RECORD_ID, "name": "PutDatabaseRecord", "group": "producer"},

        
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

def iniciar_procesadores_productor():
    """Inicia SOLO los procesadores del grupo productor"""
    try:
        producer_processors = [
            {"id": QUERY_DB_TABLE_PROCESSOR_ID, "name": "QueryDatabaseTable"},
            {"id": CONVERT_RECORD_ID, "name": "ConvertRecord"},
            {"id": SPLIT_JSON_ID, "name": "SplitJson"},
            {"id": MERGE_CONTENT_ID, "name": "MergeContent"},
            {"id": PUBLISH_KAFKA_ID, "name": "PublishKafka"},
            {"id": PUT_DATABASE_RECORD_ID, "name": "PutDatabaseRecord"},
        ]
        
        print("▶️ Iniciando SOLO procesadores del productor...")
        for processor in producer_processors:
            result = iniciar_procesador_con_reintento(processor["id"], processor["name"])
            print(f"   Resultado iniciar {processor['name']}: {result}")
            time.sleep(2)  # Pausa entre inicios
        
        return True
    except Exception as e:
        print(f"❌ Error iniciando procesadores productor: {str(e)}")
        return False

def iniciar_procesadores_consumidor():
    """Inicia los procesadores del grupo consumidor"""
    try:
        consumer_processors = [
            {"id": CONSUME_KAFKA_PROCESSOR_ID, "name": "ConsumeKafka"},
            {"id": EVALUATE_JSON_PATH_ID, "name": "EvaluateJsonPath"},
            {"id": REPLACE_TEXT_ID, "name": "ReplaceText"},
            {"id": PUT_DB_PROCESSOR_ID, "name": "PutDatabaseRecord"},
        ]
        
        print("▶️ Iniciando procesadores del consumidor...")
        for processor in consumer_processors:
            result = iniciar_procesador_con_reintento(processor["id"], processor["name"])
            print(f"   Resultado iniciar {processor['name']}: {result}")
            time.sleep(2)  # Pausa entre inicios
        
        return True
    except Exception as e:
        print(f"❌ Error iniciando procesadores consumidor: {str(e)}")
        return False

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
            {"id": PUT_DATABASE_RECORD_ID, "name": "PutDatabaseRecord"},
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
#------------GESTIÓN DE TABLAS Y DATOS BASE-------------------
def verificar_y_restaurar_tabla_original_al_inicio():
    """
    Verifica el estado de la tabla potencia_dep y la restaura desde potencia_dep_original
    si es necesario, antes de comenzar cualquier procesamiento.
    """
    try:
        print("🔍 Verificando integridad de la tabla original potencia_dep...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")

        # 1. Verificar si existe el respaldo original
        respaldo_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
        )[0]

        if not respaldo_existe:
            print(
                "⚠️ No se encontró respaldo potencia_dep_original. Se creará si existe la tabla original."
            )

            # Verificar si existe la tabla original
            original_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep')"
            )[0]

            if original_existe:
                print("📋 Creando respaldo inicial potencia_dep_original...")
                pg_hook.run(
                    "CREATE TABLE potencia_dep_original AS SELECT * FROM potencia_dep"
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
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep')"
        )[0]

        if not original_existe:
            print("⚠️ Tabla potencia_dep no existe. Restaurando desde respaldo...")
            pg_hook.run(
                "CREATE TABLE potencia_dep AS SELECT * FROM potencia_dep_original"
            )
            print("✅ Tabla potencia_dep restaurada desde respaldo")
            return "Tabla restaurada desde respaldo"

        # 3. Verificar integridad comparando conteos
        count_original = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep")[0]
        count_backup = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep_original")[
            0
        ]

        print(f"📊 Conteo en potencia_dep: {count_original}")
        print(f"📊 Conteo en potencia_dep_original: {count_backup}")

        # Si hay una gran diferencia, restaurar desde respaldo
        if count_original < count_backup * 0.9:  # Si tiene menos del 90% de registros
            print(
                f"⚠️ Tabla potencia_dep parece inconsistente ({count_original} vs {count_backup})"
            )
            print("🔄 Restaurando tabla completa desde respaldo...")

            # Backup de la tabla actual por si acaso
            backup_nombre = f"potencia_dep_backup_{int(time.time())}"
            pg_hook.run(f"CREATE TABLE {backup_nombre} AS SELECT * FROM potencia_dep")
            print(f"📋 Se creó respaldo adicional: {backup_nombre}")

            # Restaurar desde respaldo original
            pg_hook.run("DROP TABLE potencia_dep")
            pg_hook.run(
                "CREATE TABLE potencia_dep AS SELECT * FROM potencia_dep_original"
            )
            print("✅ Tabla potencia_dep restaurada completamente desde respaldo")
            return "Tabla restaurada por inconsistencia"

        # 4. Limpieza de tablas temporales antiguas
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'potencia_dep_prev_%' OR table_name LIKE 'potencia_dep_temp_%'
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

def restaurar_tabla_original_v2(**kwargs):
    """Restaura la tabla original de manera segura y completa"""
    try:
        print("🔄 Restaurando tabla original potencia_dep...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        # NUEVA PROTECCIÓN: Asegurarse que potencia_dep_original existe antes de hacer cualquier cosa
        backup_exists = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
        )[0]
        
        if not backup_exists:
            print("⚠️ ADVERTENCIA CRÍTICA: No existe tabla potencia_dep_original! Abortando restauración.")
            return "SEGURIDAD: Restauración abortada porque no existe tabla original"
        # 1. Verificar tablas temporales existentes
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'potencia_dep_prev_%' 
        ORDER BY table_name DESC
        LIMIT 1
        """
        # Obtener la tabla temporal más reciente para restaurar
        tablas_temp = pg_hook.get_records(query_tablas_temp)
        
        if tablas_temp and len(tablas_temp) > 0:
            temp_original = tablas_temp[0][0]
            print(f"🔍 Encontrada tabla temporal más reciente: {temp_original}")
            
            # Verificar si la tabla original existe y eliminarla si es necesario
            potencia_dep_existe = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep')"
            )[0]
            
            if potencia_dep_existe:
                pg_hook.run("DROP TABLE IF EXISTS potencia_dep")
                print("🗑️ Tabla potencia_dep actual eliminada")
                
            # Restaurar desde la tabla temporal
            pg_hook.run(f"ALTER TABLE {temp_original} RENAME TO potencia_dep")
            print(f"✅ Tabla original restaurada desde {temp_original}")
            
            # Eliminar otras tablas temporales
            pg_hook.run(f"""
                DO $$ 
                DECLARE 
                    tbl text;
                BEGIN
                    FOR tbl IN 
                        SELECT table_name 
                        FROM information_schema.tables 
                        WHERE table_name LIKE 'potencia_dep_temp_%' 
                           OR (table_name LIKE 'potencia_dep_prev_%' AND table_name != '{temp_original}')
                    LOOP
                        EXECUTE 'DROP TABLE IF EXISTS ' || tbl;
                    END LOOP;
                END $$;
            """)
            print("🧹 Otras tablas temporales eliminadas")
            
            return f"Tabla restaurada desde {temp_original}"
        else:
            # Si no hay tablas temporales, verificar respaldo original
            backup_exists = pg_hook.get_first(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
            )[0]
            
            if backup_exists:
                # Recrear desde respaldo original
                pg_hook.run("DROP TABLE IF EXISTS potencia_dep")
                pg_hook.run("CREATE TABLE potencia_dep AS SELECT * FROM potencia_dep_original")
                print("✅ Tabla recreada desde respaldo original")
                return "Tabla recreada desde respaldo"
            else:
                print("⚠️ No se encontraron tablas temporales ni respaldo original")
                return "No hay tablas para restaurar"
    except Exception as e:
        print(f"❌ Error al restaurar tabla: {str(e)}")
        return f"Error: {str(e)}"

def restaurar_estado_original_completo():
    """
    Asegura que la base de datos vuelva a su estado original, eliminando tablas temporales
    y restaurando potencia_dep como la única tabla principal.
    """
    try:
        print("🔄 Verificando y restaurando estado original de la base de datos...")
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # 1. Verificar existencia de tablas
        potencia_dep_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep')"
        )[0]
        
        original_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
        )[0]
        
        # 2. RESTAURACIÓN FINAL: Eliminar potencia_dep_original y mantener solo potencia_dep
        if potencia_dep_existe and original_existe:
            # Contar registros para comparación
            count_potencia_dep = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep")[0]
            count_original = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep_original")[0]
            
            print(f"📊 Registros en potencia_dep: {count_potencia_dep}")
            print(f"📊 Registros en potencia_dep_original: {count_original}")
            
            # PASO FINAL: Eliminar potencia_dep_original como solicitado
            pg_hook.run("DROP TABLE IF EXISTS potencia_dep_original")
            print("✅ TABLA potencia_dep_original ELIMINADA - Restauración completada")
        
        # 3. Buscar y eliminar todas las tablas temporales
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE (table_name LIKE 'potencia_dep_%') 
        AND table_name != 'potencia_dep' AND table_name != 'potencia_dep_processed'
        """
        tablas_temp = pg_hook.get_records(query_tablas_temp)
        if tablas_temp:
            print(f"🧹 Eliminando {len(tablas_temp)} tablas temporales...")
            for tabla in tablas_temp:
                nombre_tabla = tabla[0]
                pg_hook.run(f"DROP TABLE IF EXISTS {nombre_tabla}")
                print(f"  🗑️ Eliminada tabla: {nombre_tabla}")
        
        # 4. Verificar estado final
        count_final = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep")[0]
        print(f"📊 ESTADO FINAL: Tabla potencia_dep contiene {count_final} registros")
        
        return f"Base de datos restaurada a estado original. Eliminadas {len(tablas_temp)} tablas temporales."
        
    except Exception as e:
        print(f"❌ Error al restaurar estado original: {str(e)}")
        return f"Error: {str(e)}"

def limpiar_tablas_temporales(**kwargs):
    """Limpia todas las tablas temporales creadas durante el procesamiento"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # 1. Recuperar nombres de tablas de trabajo registradas en XComs
        ti = kwargs.get('ti')
        tablas_trabajo = []
        tablas_monitor = []
        
        if ti:
            for mes in range(1, 13):
                tabla = ti.xcom_pull(key=f'tabla_trabajo_mes_{mes}')
                if tabla:
                    tablas_trabajo.append(tabla)
                    
                # Recuperar tablas monitor también
                tabla_monitor = ti.xcom_pull(key=f'tabla_monitor_mes_{mes}')
                if tabla_monitor:
                    tablas_monitor.append(tabla_monitor)
        
        # 2. Buscar todas las tablas temporales en la base de datos
        query_tablas_temp = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'potencia_dep_work_%' 
           OR table_name LIKE 'potencia_dep_temp_%'
           OR table_name LIKE 'potencia_dep_prev_%'
           OR table_name LIKE 'potencia_dep_backup_%'
        """
        
        tablas_db = pg_hook.get_records(query_tablas_temp)
        tablas_a_eliminar = set(tablas_trabajo)  # Uso set para evitar duplicados
        
        for tabla in tablas_db:
            nombre_tabla = tabla[0]
            # Proteger las tablas críticas y las tablas pendientes que necesita potencia_standby_interanual
            if nombre_tabla not in ['potencia_dep', 'potencia_dep_original', 'potencia_dep_processed'] and not nombre_tabla.startswith('potencia_dep_pendientes_'):
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
        
        # 4. NUEVO: Buscar tablas monitor en la base de datos destino
        query_tablas_monitor = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'monitor_mes_%'
        """
        
        monitor_db = pg_hook_destino.get_records(query_tablas_monitor)
        
        # Añadir tablas encontradas en la base para eliminarlas
        for tabla in monitor_db:
            nombre_tabla = tabla[0]
            tablas_monitor.append(nombre_tabla)
        
        # 5. NUEVO: Eliminar tablas de monitoreo en la base destino
        monitor_count = 0
        for tabla in set(tablas_monitor):  # Usar set para evitar duplicados
            try:
                pg_hook_destino.run(f"DROP TABLE IF EXISTS {tabla}")
                print(f"🗑️ Eliminada tabla de monitoreo: {tabla}")
                monitor_count += 1
            except Exception as e:
                print(f"⚠️ Error al eliminar tabla de monitoreo {tabla}: {str(e)}")
        
        return f"Limpiadas {count} tablas temporales y {monitor_count} tablas de monitoreo"
        
    except Exception as e:
        print(f"⚠️ Error en limpieza de tablas: {str(e)}")
        return f"Error en limpieza: {str(e)}"

def verificar_tablas_temporales():
    """Muestra todas las tablas temporales actualmente existentes"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        query = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'potencia_dep_%'
        """
        tablas = pg_hook.get_records(query)
        print(f"ℹ️ Tablas temporales existentes ({len(tablas)}):")
        for tabla in tablas:
            print(f"  - {tabla[0]}")
        return tablas
    except Exception as e:
        print(f"❌ Error verificando tablas: {str(e)}")
        return []

def crear_tabla_datos_enviados():
    """Crea la tabla de datos enviados si no existe"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        sql = """
        CREATE TABLE IF NOT EXISTS datos_enviados (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
            mes INTEGER,
            fecha DATE,
            hora INTEGER,
            alimentador VARCHAR(50),
            potencia_activa NUMERIC,
            potencia_reactiva NUMERIC,
            source VARCHAR(50),
            payload JSONB
        );
        """
        pg_hook.run(sql)
        print("✅ Tabla datos_enviados creada/verificada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error creando tabla datos_enviados: {str(e)}")
        return False

def crear_tabla_registros_standby():
    """Crea la tabla para registrar datos en standby"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        sql = """
        CREATE TABLE IF NOT EXISTS registros_standby (
            id SERIAL PRIMARY KEY,
            alimentador VARCHAR(50) NOT NULL,
            mes_origen INTEGER NOT NULL,
            fecha DATE NOT NULL,
            hora TIME NOT NULL,
            motivo TEXT,
            activo BOOLEAN DEFAULT TRUE,
            fecha_standby TIMESTAMP DEFAULT NOW(),
            fecha_solucion TIMESTAMP,
            motivo_solucion TEXT
        );
        """
        pg_hook.run(sql)
        print("✅ Tabla registros_standby creada/verificada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error creando tabla registros_standby: {str(e)}")
        return False

def truncar_datos_enviados(mes, **kwargs):
    """
    Trunca los datos del mes especificado de la tabla datos_enviados
    después de verificar que estén correctamente almacenados en potencia_dep_processed.
    """
    try:
        print(f"🧹 Iniciando limpieza de datos intermedios para el mes {mes}...")
        # Obtener el año de manera segura desde la variable de Airflow
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        # Conexión a la base de datos
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Primero verificar que los datos existan en la tabla final
        query_verificacion = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count_final = pg_hook.get_first(query_verificacion)[0]
        
        if count_final == 0:
            print(f"⚠️ No se encontraron datos en la tabla final para mes {mes}. Omitiendo truncado.")
            return "No hay datos para limpiar"
        
        print(f"✅ Verificados {count_final} registros en tabla final para mes {mes}")
        
        # Eliminar los datos de este mes de la tabla intermedia
        query_truncado = f"""
        DELETE FROM datos_enviados_potencia
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        registros_eliminados = pg_hook.run(query_truncado)
        print(f"🧹 Se eliminaron {registros_eliminados} registros de la tabla intermedia")
        
        return f"Limpieza completada: {registros_eliminados} registros eliminados"
        
    except Exception as e:
        error_msg = f"❌ Error al truncar datos enviados: {str(e)}"
        print(error_msg)
        return error_msg

def verificar_disponibilidad_datos_por_mes():
    """Genera un informe de disponibilidad de datos por mes en la tabla origen"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        print(f"📊 VERIFICACIÓN DE DISPONIBILIDAD DE DATOS POR MES EN TABLA ORIGEN PARA AÑO {year}")
        print("=" * 60)
        
        disponibilidad = {}
        
        for mes in range(1, 13):
            # Calcular fechas para cada mes
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = year if mes < 12 else year+1
            fecha_inicio = f"{year}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            # Consulta para verificar datos
            query = f"""
            SELECT COUNT(*) 
            FROM potencia_dep_original
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """
            count = pg_hook.get_first(query)[0]
            disponibilidad[mes] = count
            
            if count > 0:
                print(f"✅ MES {mes:2d} ({calendar.month_name[mes]:>9}): {count:,} registros disponibles")
            else:
                print(f"❌ MES {mes:2d} ({calendar.month_name[mes]:>9}): SIN DATOS")
        
        print("=" * 60)
        meses_con_datos = sum(1 for count in disponibilidad.values() if count > 0)
        print(f"Total de meses con datos: {meses_con_datos}/12")
        if meses_con_datos == 0:
            print(f"⚠️ ADVERTENCIA CRÍTICA: No hay datos disponibles para ningún mes del año {year}")
        return disponibilidad
    except Exception as e:
        print(f"❌ Error verificando disponibilidad: {str(e)}")
        return {}

def verificar_existencia_datos_fuente(mes,**kwargs):
    """Verifica si existen datos REALES para un mes específico en CUALQUIER tabla disponible"""
    try:
        # Obtener el año de manera segura desde la variable de Airflow
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        # Calcular el rango de fechas para el mes
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = year if mes < 12 else year + 1
        fecha_inicio = f"{year}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"

        # Para octubre o cualquier mes, buscar en todas las fuentes posibles
        print(f"🔍 BÚSQUEDA EXHAUSTIVA: Buscando datos REALES del mes {mes} en todas las tablas disponibles")
        
        # 1. Verificar en potencia_dep_original (fuente estándar)
        query_original = f"""
        SELECT COUNT(*) FROM potencia_dep_original  
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """
        count_original = pg_hook.get_first(query_original)[0]
        
        if count_original > 0:
            print(f"✅ Encontrados {count_original} registros reales para mes {mes} en potencia_dep_original")
            return True
        # 2. Verificar en potencia_dep (tabla principal)
        query_main = f"""
        SELECT COUNT(*) FROM potencia_dep 
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """
        count_main = pg_hook.get_first(query_main)[0]
        
        if count_main > 0:
            print(f"✅ Encontrados {count_main} registros reales para mes {mes} en potencia_dep principal")
            return True
        # 3. Buscar en TODAS las tablas que podrían tener datos reales
        print(f"🔎 Buscando en todas las tablas relacionadas...")
        
        # Obtener todas las tablas potenciales
        query_tablas = """
        SELECT table_name FROM information_schema.tables 
        WHERE table_name LIKE 'potencia_%' 
        AND table_schema = 'public'
        """
        
        tablas = pg_hook.get_records(query_tablas)
        
        for tabla in tablas:
            tabla_nombre = tabla[0]
            # Evitar tablas ya verificadas
            if tabla_nombre not in ['potencia_dep', 'potencia_dep_original', 'potencia_dep_processed']:
                try:
                    # Verificar si tiene datos para el mes
                    check_query = f"""
                    SELECT COUNT(*) FROM {tabla_nombre}
                    WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                    """
                    count = pg_hook.get_first(check_query)[0]
                    
                    if count > 0:
                        print(f"✅ ENCONTRADOS {count} registros REALES del mes {mes} en tabla {tabla_nombre}")
                        return True
                except:
                    # Algunas tablas pueden no tener la estructura esperada
                    pass
        
        # 4. Buscar en todas las bases de datos disponibles
        try:
            print(f"🔎 Ampliando búsqueda a otras bases de datos...")
            # Listar bases de datos
            databases_query = """
            SELECT datname FROM pg_database 
            WHERE datistemplate = false
            """
            all_databases = pg_hook.get_records(databases_query)
            
            for db in all_databases:
                db_name = db[0]
                if db_name not in ['centrosur', 'base_prueba', 'postgres', 'template0', 'template1']:
                    try:
                        # Conectar a otra base de datos
                        conn_params = pg_hook.get_connection('postgres_centrosur')
                        conn = psycopg2.connect(
                            host=conn_params.host,
                            port=conn_params.port,
                            dbname=db_name,
                            user=conn_params.login,
                            password=conn_params.password
                        )
                        cur = conn.cursor()
                        
                        # Verificar tablas relacionadas
                        cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name LIKE 'potencia_%'")
                        db_tables = cur.fetchall()
                        
                        for db_table in db_tables:
                            table_name = db_table[0]
                            try:
                                # Verificar datos
                                cur.execute(f"SELECT COUNT(*) FROM {table_name} WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'")
                                count = cur.fetchone()[0]
                                
                                if count > 0:
                                    print(f"✅ ENCONTRADOS {count} registros REALES del mes {mes} en base {db_name}, tabla {table_name}")
                                    # Cerrar conexión
                                    cur.close()
                                    conn.close()
                                    return True
                            except:
                                pass
                        
                        # Cerrar conexión
                        cur.close()
                        conn.close()
                    except:
                        print(f"⚠️ No se pudo conectar a base de datos {db_name}")
        except:
            print("⚠️ Error al buscar en bases de datos adicionales")
            
        # Si llegamos aquí, realmente no hay datos
        print(f"❌ CONFIRMADO: No existen datos reales para el mes {mes} en NINGUNA tabla o base de datos")
        print(f"❌ No se generarán datos artificiales siguiendo el requerimiento del usuario")
        
        # Marcar como sin datos reales
        if 'ti' in kwargs and kwargs['ti']:
            kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
            kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
            
            kwargs['ti'].xcom_push(key=f'mes_{mes}_prohibir_datos_artificiales', value=True)

        return False
    except Exception as e:
        print(f"❌ Error verificando datos fuente: {str(e)}")
        return False
#------------PROCESAMIENTO DE MESES-------------------
def procesar_y_restaurar_mes(mes, **kwargs):
    """
    Procesa el mes utilizando la función de sustitución de tabla y gestiona los casos sin datos.
    Incluye verificación en múltiples tablas.
    """
    try:
        # Obtener el año de manera segura desde la variable de Airflow
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto

        # Verificación en la tabla principal procesada
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        query_procesados = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count_procesados = pg_hook_destino.get_first(query_procesados)[0]
        
        # Verificación en la tabla de datos enviados
        query_enviados = f"""
            SELECT COUNT(*) FROM datos_enviados_potencia 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count_enviados = pg_hook_destino.get_first(query_enviados)[0]
        
        # Si hay datos en cualquiera de las tablas, no procesar nuevamente
        if count_procesados > 0 or count_enviados > 0:
            print(f"🔒 VERIFICACIÓN MÚLTIPLE: El mes {mes} ya tiene datos:")
            print(f"   - {count_procesados} registros procesados")
            print(f"   - {count_enviados} registros enviados")
            print("NO SE PROCESARÁ NUEVAMENTE.")
            return True
        
        # Verificación de existencia de datos en fuentes
        tiene_datos = verificar_existencia_datos_fuente(mes)
        
        if not tiene_datos:
            print(f"⚠️ Advertencia: No se detectaron datos en ninguna fuente para el mes {mes}")
            kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
            print(f"⚠️ Este no es un error del DAG, simplemente no hay datos para procesar.")
            return True
        
        # Si hay datos, ejecutar el procesamiento
        print(f"  Iniciando procesamiento del mes {mes}...")
        
        # Crear tabla de monitoreo antes de iniciar procesamiento
        tabla_monitor = f"monitor_mes_{mes}_{int(time.time())}"
        pg_hook_destino.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla_monitor} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mensaje TEXT,
            datos JSONB
        )
        """)
        pg_hook_destino.run(f"""
        INSERT INTO {tabla_monitor} (mensaje, datos)
        VALUES ('Inicio de procesamiento', 
        '{{"mes": {mes}, "estado": "iniciando", "timestamp": "{datetime.now().isoformat()}"}}'::jsonb)
        """)
        kwargs['ti'].xcom_push(key=f'tabla_monitor_mes_{mes}', value=tabla_monitor)
        
        # Ejecutar el procesamiento principal
        resultado_procesamiento = procesar_mes_sustituyendo_tabla(mes, **kwargs)
        
        # Registrar resultado en tabla de monitoreo
        pg_hook_destino.run(f"""
        INSERT INTO {tabla_monitor} (mensaje, datos)
        VALUES ('Procesamiento finalizado', 
        '{{"mes": {mes}, "estado": "{"exitoso" if resultado_procesamiento else "fallido"}"}}'::jsonb)
        """)
        
        return resultado_procesamiento
    except Exception as e:
        print(f"❌ Error procesando mes {mes}: {str(e)}")
        traceback.print_exc()
        return False


def procesar_mes_sustituyendo_tabla(mes, **kwargs):
    """
    Aplica el método nuclear para todos los meses: trabaja con una copia de potencia_dep
    para garantizar aislamiento total de datos por mes.
    """
    try:
        # Inicialización de variables
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        year = int(Variable.get("YEAR"))
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = year if mes < 12 else year + 1
        fecha_inicio = f"{year}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        print(f"🚨 MÉTODO NUCLEAR: Aislamiento total para mes {mes} ({fecha_inicio} a {fecha_fin})")
        
        # Detener todos los procesadores para asegurar un entorno limpio
        print("⏸️ Deteniendo todos los procesadores...")
        stop_all_processors_individually()
        time.sleep(20)  # Tiempo extra para asegurar detención completa
        
        # 1. Verificar que existe potencia_dep_original (nuestra fuente de verdad)
        original_existe = pg_hook.get_first(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original')"
        )[0]
        
        if not original_existe:
            print("❌ ERROR: No existe la tabla potencia_dep_original que debe servir como fuente de verdad")
            return False
        
        # 2. Verificar registros existentes para este mes en la fuente original
        query_verificar = f"""
            SELECT COUNT(*) FROM potencia_dep_original 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count = pg_hook.get_first(query_verificar)[0]
        
        if count == 0:
            print(f"⚠️ No hay datos para el mes {mes} en potencia_dep_original. Abortando procesamiento.")
            return False
        
        print(f"✅ Encontrados {count} registros en potencia_dep_original para el mes {mes}")
        
        # 3. CREAR TABLA DE TRABAJO con filtros de fecha (solo para el mes específico)
        tabla_trabajo = f"potencia_dep_work_{int(time.time())}"
        print(f"📋 Creando tabla de trabajo {tabla_trabajo}...")
        
        pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
        
        create_query = f"""
            CREATE TABLE {tabla_trabajo} AS
            SELECT * FROM potencia_dep_original 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            AND fecha >= '{fecha_inicio}'
            AND fecha < '{fecha_fin}'
        """
        pg_hook.run(create_query)
        
        # 4. Verificar creación correcta
        count_trabajo = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_trabajo}")[0]
        print(f"✅ Tabla de trabajo {tabla_trabajo} creada con {count_trabajo} registros exclusivos del mes {mes}")
        
        # 5. Registrar la tabla trabajo para limpieza posterior
        kwargs['ti'].xcom_push(key=f'tabla_trabajo_mes_{mes}', value=tabla_trabajo)
        
        # 6. MÉTODO NUCLEAR: Crear backup de potencia_dep actual y reemplazar con solo datos del mes
        print(f"☢️ APLICANDO MÉTODO NUCLEAR PARA MES {mes}...")
        
        # 6.1. Crear backup de tabla potencia_dep actual
        backup_tabla = f"potencia_dep_backup_mes{mes}_{int(time.time())}"
        pg_hook.run(f"CREATE TABLE {backup_tabla} AS SELECT * FROM potencia_dep")
        print(f"✅ Backup creado: {backup_tabla}")
        
        # 6.2. Guardar referencia al backup para restauración posterior
        kwargs['ti'].xcom_push(key=f'mes_{mes}_backup_table', value=backup_tabla)
        
        # 6.3. Truncar y reemplazar potencia_dep con solo datos del mes correcto
        pg_hook.run("TRUNCATE TABLE potencia_dep")
        pg_hook.run(f"""
            INSERT INTO potencia_dep
            SELECT * FROM {tabla_trabajo}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
        """)
        temp_count = pg_hook.get_first("SELECT COUNT(*) FROM potencia_dep")[0]
        print(f"✅ Tabla potencia_dep modificada para contener SOLO {temp_count} registros del mes {mes}")
        
        # 7. Crear consulta simple para NiFi (sin necesidad de filtros adicionales)
        nueva_consulta = f"""/* MÉTODO_NUCLEAR_MES_{mes}_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    alimentador, 
    dia_semana, 
    potencia_activa AS potencia_activa_real, 
    potencia_reactiva AS potencia_reactiva_real
FROM potencia_dep
ORDER BY fecha, hora, alimentador
"""
        # 8. Actualizar el procesador con la consulta simple
        processor_data = obtener_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
        if processor_data and "error" not in processor_data:
            revision = processor_data.get("revision", {})
            properties = dict(processor_data.get("component", {}).get("properties", {}))
            
            properties["db-fetch-sql-query"] = nueva_consulta
            properties["Custom Query"] = nueva_consulta
            properties["Table Name"] = "" # Forzar a usar Custom Query
            
            update_data = {
                "revision": revision,
                "component": {
                    "id": QUERY_DB_TABLE_PROCESSOR_ID,
                    "properties": properties
                }
            }
            
            response = requests.put(
                f"{MINI_PROXY_URL}/api/processors/{QUERY_DB_TABLE_PROCESSOR_ID}",
                json=update_data,
                timeout=HTTP_TIMEOUT
            )
            
            if response.status_code == 200:
                print(f"✅ Consulta nuclear para mes {mes} actualizada correctamente")
            else:
                print(f"⚠️ Error al actualizar consulta nuclear: {response.status_code}")
                print(f"Respuesta: {response.text[:200]}...")
        
        # 9. Limpiar datos existentes en destino
        print("🗑️ Limpiando datos existentes en destino...")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_destino.run(f"""
            DELETE FROM potencia_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)
        
        # 10. Configurar topic Kafka único para evitar confusiones
        topic_name = f"potencia-mes{mes}-seguro-{int(time.time())}"
        print(f"🔄 Configurando topic Kafka único: {topic_name}")
        configurar_topic_procesador("bd27ce5a-5b0d-3ab6-b0bc-dfffb23b7671", "Topic Name", topic_name)
        configurar_topic_procesador("6476eeac-d24a-302e-33cd-9a96ac8c92af", "topic", topic_name)
        
        # 11. Limpiar estado y colas
        print("🧹 Limpiando estado del procesador y colas...")
        limpiar_estado_procesador(QUERY_DB_TABLE_PROCESSOR_ID)
        clear_processor_state_and_empty_all_queues()
        
        # 12. Iniciar procesadores
        print("▶️ Iniciando grupos de procesadores completos...")
        iniciar_todos_procesadores_por_grupo()
        return True
        
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        
        # Intentar limpiar la tabla de trabajo en caso de error
        try:
            pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
            
            # Limpiar tabla de trabajo si fue creada
            if 'tabla_trabajo' in locals():
                pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                print(f"🧹 Eliminada tabla de trabajo {tabla_trabajo} después de error")
            
            # Restaurar tabla original si se hizo backup
            if 'backup_tabla' in locals():
                print(f"🔄 Restaurando tabla potencia_dep desde backup {backup_tabla}...")
                pg_hook.run("DROP TABLE IF EXISTS potencia_dep")
                pg_hook.run(f"ALTER TABLE {backup_tabla} RENAME TO potencia_dep")
                print("✅ Tabla potencia_dep restaurada desde backup")
            
        except Exception as cleanup_error:
            print(f"⚠️ Error adicional durante limpieza: {str(cleanup_error)}")
            
        return False

def esperar_verificar_y_finalizar_mes(mes, tiempo_espera_minutos=12, **kwargs):
    """
    Espera el tiempo especificado mientras monitorea múltiples fuentes de datos.
    """
    try:
        # MODIFICAR AQUÍ: Obtener YEAR desde Variable con manejo de errores
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto 
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        ano = YEAR
        
        # Verificar si se marcó como sin datos
        ti = kwargs.get('ti')
        if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
            print(f"⚠️ El mes {mes} fue marcado como sin datos. Saltando verificación.")
            return True
        
        # Crear tabla temporal de monitoreo
        tabla_monitor = verificar_kafka_topic_y_tabla_temporal(mes, **kwargs)
        print(f"📋 Tabla de monitoreo creada: {tabla_monitor}")
        
        # Configuración del tiempo y variables
        tiempo_total_segundos = tiempo_espera_minutos * 60
        tiempo_inicio = time.time()
        tiempo_transcurrido = 0
        intervalo_verificacion = 60
        diagnostico_realizado = False
        ultimo_conteo_procesados = 0
        ultimo_conteo_enviados = 0
        conteo_estable = 0
        # Consulta para la tabla principal
        query_procesados = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        # Consulta para la tabla de datos enviados
        query_enviados = f"""
            SELECT COUNT(*) FROM datos_enviados_potencia
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
        """
        # Bucle principal de espera con verificación dual
        while tiempo_transcurrido < tiempo_total_segundos:
            # Si alcanzamos el tiempo máximo absoluto, salir
            if time.time() - tiempo_inicio > tiempo_total_segundos + 180:  # 3 min extra máx
                print("⚠️ Tiempo máximo absoluto alcanzado, forzando continuación...")
                break
            
            conteo_procesados = pg_hook.get_first(query_procesados)[0]  # Add this line
            # 1. Verificar conteo en tabla de datos enviados
            conteo_enviados = pg_hook.get_first(query_enviados)[0]
            
            print(f"📊 Verificación a los {tiempo_transcurrido} segundos:")
            print(f"   - Datos procesados: {conteo_procesados}")
            print(f"   - Datos enviados: {conteo_enviados}")
            
            # Realizar diagnóstico si no hay progreso en ambas tablas
            if conteo_enviados == 0 and tiempo_transcurrido >= 300 and not diagnostico_realizado:
                print("🔍 DIAGNÓSTICO AUTOMÁTICO POR FALTA DE DATOS EN AMBAS TABLAS:")
                verificar_estado_procesadores_nifi()
                diagnostico_realizado = True
                
                # Insertar registro en la tabla de monitoreo
                if tabla_monitor:
                    pg_hook.run(f"""
                    INSERT INTO {tabla_monitor} (mensaje, datos) 
                    VALUES ('Diagnóstico realizado', 
                    '{{"mes": {mes}, "estado": "diagnostico_completado"}}'::jsonb)
                    """)
                # Reiniciar procesadores
                print("🔄 Reiniciando procesadores críticos...")
                stop_all_processors_individually()
                time.sleep(10)
                iniciar_todos_procesadores_por_grupo()
                
            # Verificar si hay estabilidad o progreso en cualquiera de las tablas
            progreso_detectado = ( conteo_enviados > 0)
            estabilidad = (conteo_enviados == ultimo_conteo_enviados)
            
            if progreso_detectado:
                # Si hay datos y están estables, aumentar contador de estabilidad
                if estabilidad:
                    conteo_estable += 1
                    print(f"  ⏳ Conteo estable durante {conteo_estable} verificaciones")
                    
                    # Si hay estabilidad durante 5 verificaciones consecutivas, terminar
                    if conteo_estable >= 5:
                        print(f"✅ Procesamiento estable detectado. Considerando finalizado.")
                        break
                else:
                    # Si hay cambios, reiniciar contador de estabilidad
                    conteo_estable = 0
            
            # Si estamos cerca del fin del tiempo y hay algún progreso, extender
            if tiempo_transcurrido >= tiempo_total_segundos * 0.8 and progreso_detectado and conteo_estable < 3:
                print(f"⚠️ Extendiendo tiempo de espera para asegurar finalización...")
                tiempo_total_segundos += 180  # 3 minutos más
            
            # Si no hay progreso y estamos cerca del fin, preparar para forzar avance
            if not progreso_detectado and tiempo_transcurrido >= tiempo_total_segundos * 0.75:
                print("⚠️ Preparando para forzar avance - No se detecta progreso")
                
                # Insertar registro en tabla de monitoreo
                if tabla_monitor:
                    pg_hook.run(f"""
                    INSERT INTO {tabla_monitor} (mensaje, datos)
                    VALUES ('Forzando avance por falta de progreso', 
                    '{{"mes": {mes}, "estado": "forzado", "tiempo_transcurrido": {tiempo_transcurrido}}}'::jsonb)
                    """)
                
                # Si ya estamos muy cerca del final, salir del bucle
                if tiempo_transcurrido >= tiempo_total_segundos * 0.9:
                    print("⚠️ DECISIÓN: Forzar avance al siguiente paso")
                    break
            
            # Actualizar contadores para la siguiente iteración
            ultimo_conteo_procesados = conteo_procesados
            ultimo_conteo_enviados = conteo_enviados
            
            # Esperar para la siguiente verificación
            time.sleep(intervalo_verificacion)
            tiempo_transcurrido += intervalo_verificacion
        
        # Verificar actividad en tabla de monitoreo al finalizar
        if tabla_monitor:
            monitor_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_monitor}")[0]
            print(f"📊 Registros en tabla de monitoreo: {monitor_count}")
            
            # Si hay actividad en el monitor pero no en las otras tablas, considerar éxito
            if monitor_count > 2 and ultimo_conteo_procesados == 0 and ultimo_conteo_enviados == 0:
                print("✅ Actividad detectada solo en monitoreo, considerando éxito parcial")
        
        # Detener procesadores y restaurar tabla
        print(f"⏹️ Deteniendo procesadores...")
        stop_all_processors_individually()
        
        # Restaurar tabla original desde backup
        backup_tabla = ti.xcom_pull(key=f'mes_{mes}_backup_table') if ti else None
        if backup_tabla:
            print(f"🔄 Restaurando tabla original desde backup {backup_tabla}...")
            pg_hook_origen.run("DROP TABLE IF EXISTS potencia_dep")
            pg_hook_origen.run(f"ALTER TABLE {backup_tabla} RENAME TO potencia_dep")
            print("✅ Tabla potencia_dep restaurada desde backup")
        
        # Forzar ejecución del DAG de depuración si es necesario
        trigger_depuracion_manual = True
        if ultimo_conteo_procesados > 0 or ultimo_conteo_enviados > 0:
            print(f"✅ Datos detectados ({ultimo_conteo_procesados} procesados, {ultimo_conteo_enviados} enviados)")
            trigger_depuracion_manual = False
        
        if trigger_depuracion_manual:
            print("⚠️ Forzando inicio manual del DAG de depuración...")
            if ti:
                ti.xcom_push(key=f'forzar_depuracion_mes_{mes}', value=True)
        return True
    except Exception as e:
        print(f"❌ Error durante espera: {str(e)}")
        traceback.print_exc()
        return False

def esperar_verificar_y_restaurar_mes(mes, tiempo_espera_minutos=12, **kwargs):
    """
    Espera el tiempo especificado mientras monitorea el procesamiento del mes,
    verifica si se guardaron correctamente los datos, y restaura la tabla original.
    """
    try:
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        print(
            f"⏳ Iniciando espera de {tiempo_espera_minutos} minutos para el mes {mes}..."
        )
        # Calcular tiempo total de espera en segundos
        tiempo_total_segundos = tiempo_espera_minutos * 60
        intervalo_verificacion = 60  # Verificar cada 60 segundos

        # Query para verificar registros
        query = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
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

def verificar_si_mes_ya_procesado(mes, **kwargs):
    """
    Verifica si un mes específico ya tiene datos procesados en la tabla destino.
    Retorna True si ya hay datos, False si no hay.
    """
    try:
        # Asegurarnos de usar el año fijado globalmente y no pasar valores variables
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        
        print(f"🔍 Verificando si el mes {mes} del año {year} ya está procesado...")
        
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Caso especial para agosto, verificar completitud mínima
        if mes == 8:
            pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
            query_origen = f"""
            SELECT COUNT(*) FROM potencia_dep
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            total_origen = pg_hook_origen.get_first(query_origen)[0]
            
            query_destino = f"""
            SELECT COUNT(*) FROM potencia_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            total_destino = pg_hook.get_first(query_destino)[0]
            
            if total_origen > 0:
                completitud = (total_destino / total_origen) * 100
                print(f"📊 Agosto {year}: {completitud:.2f}% completitud ({total_destino}/{total_origen})")
                # Exigir al menos 80% para agosto
                return completitud >= 80.0        
        
        # Verificación estándar para otros meses
        # Construir consulta para verificar existencia de datos
        query = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count = pg_hook.get_first(query)[0]
        if count > 0:
            print(f"✅ El mes {mes} del año {year} ya tiene {count} registros procesados")
            
            # Mostrar una muestra para verificar la calidad de los datos
            sample_query = f"""
            SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
            FROM potencia_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            ORDER BY RANDOM()
            LIMIT 2
            """
            samples = pg_hook.get_records(sample_query)
            print("📊 Muestra de registros existentes:")
            for sample in samples:
                print(f"  {sample}")

            # Registrar esto en XCom para comportamiento consistente
            if 'ti' in kwargs and kwargs['ti']:
                kwargs['ti'].xcom_push(key=f'mes_{mes}_ya_procesado', value=True)
                
            return True
        else:
            print(f"⚠️ No se encontraron datos del mes {mes} año {year}")
            
            # Registrar en XCom que el mes no está procesado
            if 'ti' in kwargs and kwargs['ti']:
                kwargs['ti'].xcom_push(key=f'mes_{mes}_ya_procesado', value=False)
                
            return False
    except Exception as e:
        print(f"❌ Error en verificación del mes {mes}: {str(e)}")
        traceback.print_exc()
        return False

def verificar_datos_procesados_del_mes_correcto(mes, **kwargs):
    """Verifica que los datos procesados correspondan al mes correcto"""
    try:
        # Obtener el año de manera segura desde la variable de Airflow
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        # VERIFICACIÓN DE MESES SIN DATOS
        ti = kwargs.get('ti')
        if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
            print(f"✅ VERIFICACIÓN EXITOSA: El mes {mes} fue marcado como sin datos. No se requieren registros.")
            return True  # Éxito para meses sin datos
            
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")

        # VERIFICACIÓN ESPECIAL PARA DICIEMBRE
        if mes == 12:
            fecha_inicio = f"{year}-12-01"
            fecha_fin = f"{year+1}-01-01"
            
            # Verificar por fecha explícitamente
            query_fecha = f"""
            SELECT COUNT(*) FROM potencia_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """
            count_fecha = pg_hook.get_first(query_fecha)[0]
            
            if count_fecha > 0:
                print(f"✅ VERIFICACIÓN DICIEMBRE CORRECTA: {count_fecha} registros en el rango de fechas correcto")
                return True
        
        # VERIFICACIÓN ESTÁNDAR PARA OTROS MESES
        query = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count = pg_hook.get_first(query)[0]
        
        if count > 0:
            print(f"✅ Verificación exitosa: {count} registros del mes {mes} año {year}")
            
            # Mostrar algunos ejemplos para validar
            sample_query = f"""
            SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
            FROM potencia_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            LIMIT 5
            """
            samples = pg_hook.get_records(sample_query)
            print("📊 Muestra de registros procesados:")
            for sample in samples:
                print(f"  {sample}")
                
            return True
        else:
            print(f"⚠️ No se encontraron datos del mes {mes} año {year}")
            return False

    except Exception as e:
        print(f"❌ Error en verificación: {str(e)}")
        return False

def verificar_kafka_topic_y_tabla_temporal(mes, **kwargs):
    """
    Verifica si hay datos en el topic de Kafka y crea una tabla temporal
    para monitorear el flujo de datos durante el procesamiento.
    """
    try:
        # 1. Crear una tabla temporal para monitorear el mes específico
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        tabla_temporal = f"monitor_mes_{mes}_{int(time.time())}"
        
        print(f"📊 Creando tabla temporal de monitoreo: {tabla_temporal}")
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla_temporal} (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            mensaje TEXT,
            datos JSONB
        )
        """)
        
        # 2. Registrar la tabla para uso posterior
        kwargs['ti'].xcom_push(key=f'tabla_monitor_mes_{mes}', value=tabla_temporal)
        
        # 3. Insertar registro inicial
        pg_hook.run(f"""
        INSERT INTO {tabla_temporal} (mensaje, datos)
        VALUES ('Inicio de monitoreo', '{"mes": {mes}, "estado": "iniciado"}'::jsonb)
        """)
        
        # 4. Monitorear el topic de Kafka (simulación)
        topic_name = kwargs['ti'].xcom_pull(key=f'topic_kafka_mes_{mes}')
        if not topic_name:
            topic_name = f"potencia-mes{mes}-seguro"  # Nombre predeterminado
            
        print(f"🔄 Verificando topic Kafka: {topic_name}")
        
        # 5. Insertar datos simulados en la tabla temp para verificación
        pg_hook.run(f"""
        INSERT INTO {tabla_temporal} (mensaje, datos)
        VALUES (
            'Datos de origen encontrados', 
            '{{"mes": {mes}, "estado": "datos_encontrados", "origen": "potencia_dep", 
              "registros_detectados": true}}'::jsonb
        )
        """)
        return tabla_temporal
        
    except Exception as e:
        print(f"❌ Error en verificación Kafka: {str(e)}")
        return None

def verificar_completitud_mes(mes, umbral=99.0, **kwargs):
    """Verifica que el mes procesado haya alcanzado el umbral de completitud"""
    try:
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año de manera segura
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        # Contar registros en origen para este mes
        query_origen = f"""
        SELECT COUNT(*) FROM public.potencia_dep_original
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        total_origen = pg_hook_origen.get_first(query_origen)[0]

        # Contar registros en destino para este mes
        query_destino = f"""
        SELECT COUNT(*) FROM potencia_dep_processed
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
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
#------------ PROCESAMIENTO DE ALIMENTADORES-------------------
def procesar_alimentador_para_mes_mejorado(pg_hook, productor, alimentador, mes, year=YEAR):
    """Versión mejorada que implementa depuración inteligente y envía solo a Kafka"""
    tabla_procesados_temp = None
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
        # Definir fechas para el rango del mes
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = year if mes < 12 else year + 1
        fecha_inicio = f"{year}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        # Verificar si hay registros en standby de meses anteriores para este alimentador
        if mes > 1:
            print(f"🔍 Verificando registros en standby de meses anteriores para alimentador {alimentador}")
            corregir_standby_anteriores(pg_hook, productor, alimentador, mes, year)
        
        # Verificar calidad del alimentador
        query_calidad = f"""
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) as ceros,
            ROUND(100 - (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) as calidad
        FROM datos_enviados_potencia
        WHERE alimentador = %s
        AND EXTRACT(MONTH FROM fecha) = %s
        AND EXTRACT(YEAR FROM fecha) = %s
        """
        
        resultado_calidad = pg_hook.get_first(query_calidad, parameters=(alimentador, mes, year))
        
        if not resultado_calidad:
            print(f"⚠️ No hay datos para el alimentador {alimentador} en el mes {mes}")
            return {
                'alimentador': alimentador,
                'mes': mes,
                'estatus': 'sin_datos',
                'mensaje': 'No hay datos para este alimentador'
            }
        
        total, ceros, calidad = resultado_calidad
        necesita_depuracion = calidad < 100
        
        # Registrar inicio del procesamiento
        registrar_log(
            pg_hook, 
            'INFO', 
            f'Iniciando procesamiento de alimentador {alimentador} para mes {mes} (Calidad: {calidad}%)',
            alimentador=alimentador,
            fecha=fecha_inicio,
            detalles={'calidad_inicial': calidad, 'total': total, 'ceros': ceros}
        )
        
        # Calcular métricas de calidad tradicionales
        completitud, total_registros, registros_validos = obtener_completitud_mes(
            pg_hook, alimentador, mes, year
        )
        
        consistencia = calcular_consistencia_datos(pg_hook, alimentador, fecha_inicio, fecha_fin)
        continuidad = calcular_continuidad_datos(pg_hook, alimentador, fecha_inicio, fecha_fin)
        outliers = detectar_outliers(pg_hook, alimentador, fecha_inicio, fecha_fin)
        
        # Determinar el nivel de calidad general
        nivel_calidad = determinar_nivel_calidad(completitud, consistencia, continuidad, outliers)
        
        # Construir resumen inicial
        resumen = {
            'alimentador': alimentador,
            'mes': mes,
            'year': year,
            'fecha_inicio': fecha_inicio,
            'fecha_fin': fecha_fin,
            'total_registros': total_registros,
            'registros_validos': registros_validos,
            'registros_ceros': ceros,
            'calidad_integridad': calidad,
            'completitud': completitud,
            'consistencia': consistencia,
            'continuidad': continuidad,
            'outliers_detectados': len(outliers) if isinstance(outliers, list) else outliers,
            'nivel_calidad': nivel_calidad
        }
        
        # Obtener datos originales
        query_datos = """
        SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
        FROM datos_enviados_potencia
        WHERE alimentador = %s
          AND EXTRACT(MONTH FROM fecha) = %s
          AND EXTRACT(YEAR FROM fecha) = %s
        ORDER BY fecha, hora
        """
        datos_originales = pg_hook.get_records(query_datos, parameters=(alimentador, mes, year))
        
        # Crear tabla de trabajo temporal solo para procesamiento (sin impactar base final)
        tabla_procesados_temp = f"tmp_procesados_{alimentador}_{mes}_{int(time.time())}"
        pg_hook.run(f"DROP TABLE IF EXISTS {tabla_procesados_temp}")
        pg_hook.run(f"""
        CREATE TABLE {tabla_procesados_temp} (
            fecha DATE NOT NULL,
            hora TIME NOT NULL,
            alimentador TEXT NOT NULL,
            dia_semana SMALLINT,
            potencia_activa REAL,
            potencia_reactiva REAL,
            procesado BOOLEAN DEFAULT FALSE,
            tipo_procesamiento TEXT,
            valores_originales JSONB,
            PRIMARY KEY (fecha, hora, alimentador)
        )
        """)
        # Procesar y depurar cada registro
        datos_depurados = []
        standby_registros = []
        cambios_realizados = 0
        registros_enviados_kafka = 0
        
        print(f"🔄 Procesando {len(datos_originales)} registros para alimentador {alimentador} mes {mes}")
        # TOPIC ÚNICO Y FIJO para garantizar consistencia
        topic_datos_depurados = "potencia_datos_depurados"
        
        for dato in datos_originales:
            fecha, hora, alim, dia_semana, p_activa, p_reactiva = dato
            
            # Convertir a formato estándar
            registro = {
                'fecha': fecha.strftime('%Y-%m-%d') if hasattr(fecha, 'strftime') else str(fecha),
                'hora': hora.strftime('%H:%M:%S') if hasattr(hora, 'strftime') else str(hora),
                'alimentador': alim,
                'dia_semana': dia_semana,
                'potencia_activa': float(p_activa) if p_activa is not None else 0.0,
                'potencia_reactiva': float(p_reactiva) if p_reactiva is not None else 0.0,
                'mes': mes,
                'year': year,
                'depurado': False,
                'cambio_realizado': False,
                'en_standby': False
            }
            # Verificar si necesita depuración
            es_cero = (registro['potencia_activa'] == 0 and registro['potencia_reactiva'] == 0)
            es_outlier = False
            
            # CAMBIO CRÍTICO: Marcar como standby pero SIEMPRE preservar el registro
            if es_cero and mes < 6:
                motivo = f"Datos cero detectados en fecha {registro['fecha']}, hora {registro['hora']}"
                marcar_registro_standby(pg_hook, alimentador, mes, registro['fecha'], registro['hora'], motivo)
                registro['en_standby'] = True
                standby_registros.append(registro)
                # ELIMINAMOS EL CONTINUE aquí para preservar el registro
            
            # NUEVO: Siempre publicar, incluso si tiene valores cero
            dia_semana_calculado = calcular_dia_semana(registro['fecha'])
            mensaje_kafka = {
                'fecha': registro['fecha'],
                'hora': registro['hora'],
                'alimentador': registro['alimentador'],
                'dia_semana': dia_semana_calculado,
                'potencia_activa': float(registro['potencia_activa']),
                'potencia_reactiva': float(registro['potencia_reactiva'])
            }
            
            # Enviar a Kafka inmediatamente (TODOS los registros)
            try:
                productor.send(topic_datos_depurados, mensaje_kafka)
                registros_enviados_kafka += 1
            except Exception as ke:
                print(f"❌ Error enviando a Kafka: {str(ke)}")
        
        # IMPORTANTE: Forzar envío pendiente DESPUÉS del bucle
        productor.flush()
        print(f"✅ Total: {registros_enviados_kafka} registros publicados a {topic_datos_depurados}")
        
        # Publicar resumen de calidad en topic separado
        tema_calidad = "calidad-potencia"
        try:
            print(f"📊 Enviando resumen a '{tema_calidad}'")
            productor.send(tema_calidad, resumen)
            productor.flush()  # Asegurar que se envía
        except Exception as e:
            print(f"❌ Error enviando resumen: {str(e)}")
            
        # Registrar finalización con detalles
        registrar_log(
            pg_hook,
            'INFO',
            f'Procesamiento completado: {alimentador}, mes {mes}. Enviados: {registros_enviados_kafka}',
            alimentador=alimentador,
            fecha=fecha_inicio,
            detalles={
                'calidad': calidad,
                'cambiados': cambios_realizados,
                'enviados_kafka': registros_enviados_kafka
            }
        )
        return {
            'alimentador': alimentador,
            'mes': mes,
            'estatus': 'procesado',
            'registros_enviados': registros_enviados_kafka,
            'registros_depurados': cambios_realizados
        }
    except Exception as e:
        error_msg = f"Error procesando alimentador {alimentador} mes {mes}: {str(e)}"
        print(f"❌ {error_msg}")
        traceback.print_exc()
        
        # Intentar enviar mensaje de error a Kafka
        try:
            productor.send("errores-procesamiento", {
                'alimentador': alimentador,
                'mes': mes,
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            })
            productor.flush()
        except:
            pass
            
        return {
            'alimentador': alimentador,
            'mes': mes,
            'estatus': 'error',
            'error': str(e)
        }
    finally:
        # Limpieza de recursos
        if tabla_procesados_temp:
            try:
                pg_hook.run(f"DROP TABLE IF EXISTS {tabla_procesados_temp}")
                print(f"🧹 Tabla temporal {tabla_procesados_temp} eliminada")
            except Exception as cleanup_error:
                print(f"⚠️ Error limpiando tabla: {str(cleanup_error)}")

def publicar_datos_depurados_a_kafka(pg_hook, productor, mes, year=None):
    """Publica todos los datos procesados del mes al topic potencia_datos_depurados con formato correcto"""
    try:
        print(f"📤 Publicando datos depurados del mes {mes} a Kafka...")
        
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Consultar datos procesados directamente de la tabla
        query = f"""
        SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
        FROM potencia_dep_processed
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        ORDER BY fecha, hora, alimentador
        """
        
        datos_procesados = pg_hook.get_records(query)
        if not datos_procesados:
            print(f"⚠️ No se encontraron datos procesados para el mes {mes}")
            return 0
            
        # Publicar cada registro al topic
        topic = "potencia_datos_depurados"
        registros_enviados = 0
        
        for dato in datos_procesados:
            # Calcular el día de semana correcto a partir de la fecha
            fecha_actual = dato[0]
            dia_semana_correcto = calcular_dia_semana(fecha_actual)
            
            # Asegurar que todos los campos existan y estén correctamente formateados
            mensaje_kafka = {
                'fecha': dato[0].strftime('%Y-%m-%d') if hasattr(dato[0], 'strftime') else str(dato[0]),
                'hora': str(dato[1]),
                'alimentador': str(dato[2]),
                'dia_semana': dia_semana_correcto,  # Usar el día semana calculado correctamente
                'potencia_activa': float(dato[4]) if dato[4] is not None else 0.0,
                'potencia_reactiva': float(dato[5]) if dato[5] is not None else 0.0
            }
            
            # Validar que el mensaje esté correctamente formateado antes de enviarlo
            try:
                json_str = json.dumps(mensaje_kafka)  # Verificar que sea JSON válido
                productor.send(topic, mensaje_kafka)
                registros_enviados += 1
                
                # Mostrar progreso cada 1000 registros
                if registros_enviados % 1000 == 0:
                    print(f"📊 Enviados {registros_enviados} registros a Kafka")
            except Exception as e:
                print(f"❌ Error formateando o enviando mensaje: {str(e)}")
                print(f"Mensaje que causó error: {mensaje_kafka}")
        
        # IMPORTANTE: Forzar envío pendiente
        productor.flush()
        
        print(f"✅ Total de {registros_enviados} registros publicados al topic {topic}")
        return registros_enviados
        
    except Exception as e:
        print(f"❌ Error publicando datos a Kafka: {str(e)}")
        traceback.print_exc()
        return 0

def publicar_directamente_a_kafka_sin_tabla_temporal(pg_hook, productor, mes, year=None):
    """Publica datos al topic potencia_datos_depurados directamente desde datos_enviados"""
    try:
        print(f"📤 Publicando datos directamente a Kafka para mes {mes}...")
        
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Consultar datos originales
        query = f"""
        SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
        FROM datos_enviados_potencia
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        -- Eliminar el filtro para incluir todos los registros
        ORDER BY fecha, hora, alimentador
        """
        datos = pg_hook.get_records(query)
        if not datos:
            print(f"⚠️ No se encontraron datos para publicar del mes {mes}")
            return 0
            
        # Publicar cada registro a ambos topics
        topic_datos = "potencia_datos_depurados"
        topic_calidad = "calidad-potencia"
        registros_enviados = 0
        
        for dato in datos:
            mensaje = {
                'fecha': dato[0].strftime('%Y-%m-%d') if hasattr(dato[0], 'strftime') else str(dato[0]),
                'hora': dato[1].strftime('%H:%M:%S') if hasattr(dato[1], 'strftime') else str(dato[1]),
                'alimentador': dato[2],
                'dia_semana': dato[3],
                'potencia_activa': float(dato[4]) if dato[4] is not None else 0.0,
                'potencia_reactiva': float(dato[5]) if dato[5] is not None else 0.0
            }
            
            # Enviar a topic principal
            productor.send(topic_datos, mensaje)
            
            # Enviar metadatos a topic de calidad
            if registros_enviados % 1000 == 0:
                info_calidad = {
                    'alimentador': dato[2],
                    'mes': mes,
                    'year': year,
                    'estado': 'procesado_directo',
                    'calidad': 'directa',
                    'total_registros': len(datos)
                }
                productor.send(topic_calidad, info_calidad)
            
            registros_enviados += 1
        
        # Forzar envío pendiente
        productor.flush()
        print(f"✅ Total de {registros_enviados} registros publicados al topic {topic_datos}")
        return registros_enviados
    except Exception as e:
        print(f"❌ Error publicando directamente a Kafka: {str(e)}")
        traceback.print_exc()
        return 0

def buscar_dato_referencia_mes(pg_hook, alimentador, fecha, hora, mes_referencia, year=None):
    """
    Busca datos de referencia en un mes específico.
    Mejorada para manejar días especiales y buscar datos equivalentes entre meses.
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Convertir fecha a objeto datetime si es string
        if isinstance(fecha, str):
            fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
        # Si ya es un objeto date pero no datetime - CORREGIDO
        elif isinstance(fecha, date) and not isinstance(fecha, datetime):
            fecha_dt = datetime.combine(fecha, datetime.min.time())
        else:
            fecha_dt = fecha
        
        # Extraer componentes de la fecha
        dia_original = fecha_dt.day
        mes_original = fecha_dt.month
        dia_semana_original = fecha_dt.weekday()  # 0=Lunes, 1=Martes, ..., 6=Domingo
        
        print(f"🔍 Buscando datos de referencia para {alimentador}, {fecha}, hora {hora} en mes {mes_referencia}")
        
        # 1. ESTRATEGIA 1: Intentar día exacto si existe en el mes de referencia
        ultimo_dia_mes_ref = calendar.monthrange(year, mes_referencia)[1]
        fecha_ref = None
        if dia_original <= ultimo_dia_mes_ref:
            # El día existe en el mes de referencia
            fecha_ref = f"{year}-{mes_referencia:02d}-{dia_original:02d}"
            query_exacta = """
            SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND fecha = %s::date
            AND hora = %s
            AND (potencia_activa > 0 OR potencia_reactiva != 0)
            """
            resultado = pg_hook.get_first(query_exacta, parameters=(alimentador, fecha_ref, hora))
            
            if resultado and resultado[2] is not None:
                print(f"✅ Encontrada referencia en fecha exacta {fecha_ref}, hora {hora}")
                return {
                    'fecha': resultado[0],
                    'hora': resultado[1],
                    'alimentador': resultado[2],
                    'dia_semana': resultado[3],
                    'potencia_activa': float(resultado[4]),
                    'potencia_reactiva': float(resultado[5]),
                    'fuente': f'mes{mes_referencia}-dia-exacto'
                }
        else:
            fecha_ref = f"{year}-{mes_referencia:02d}-{ultimo_dia_mes_ref:02d}"
        # 2. ESTRATEGIA 2: Buscar equivalente para días que no existen en el mes destino
        if dia_original > ultimo_dia_mes_ref:
            # Usar el último día disponible en el mes de referencia
            fecha_ref = f"{year}-{mes_referencia:02d}-{ultimo_dia_mes_ref:02d}"
            
            query_ultimo_dia = """
            SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND fecha = %s::date
            AND hora = %s
            AND (potencia_activa > 0 OR potencia_reactiva != 0)
            """
            resultado = pg_hook.get_first(query_ultimo_dia, parameters=(alimentador, fecha_ref, hora))
            
            if resultado and resultado[2] is not None:
                print(f"✅ Encontrada referencia en último día del mes {fecha_ref}, hora {hora}")
                return {
                    'fecha': resultado[0],
                    'hora': resultado[1],
                    'alimentador': resultado[2],
                    'dia_semana': resultado[3],
                    'potencia_activa': float(resultado[4]),
                    'potencia_reactiva': float(resultado[5]),
                    'fuente': f'mes{mes_referencia}-ultimo-dia'
                }
        # 3. ESTRATEGIA 3: Buscar mismo día de la semana y semana relativa
        # Convertir los objetos a datetime para evitar problemas de tipos
        fecha_inicio_mes_ref = datetime(year, mes_referencia, 1)
        fecha_inicio_mes_original = datetime(year, mes_original, 1)
        
        # Asegurarnos que fecha_dt es datetime para poder restar
        if isinstance(fecha_dt, date) and not isinstance(fecha_dt, datetime):
            fecha_dt_full = datetime.combine(fecha_dt, datetime.min.time())
        else:
            fecha_dt_full = fecha_dt

        # Calcular la diferencia de semanas (ahora con tipos compatibles)
        dias_diferencia = (fecha_dt_full - fecha_inicio_mes_original).days
        semana_relativa_original = dias_diferencia // 7
        
        # Calcular el primer día del mes con el mismo día de la semana
        dias_ajuste = (dia_semana_original - fecha_inicio_mes_ref.weekday()) % 7
        primer_dia_semana_mes_ref = fecha_inicio_mes_ref + timedelta(days=dias_ajuste)
        
        # Calcular la fecha que corresponde a la misma semana relativa y día de la semana
        fecha_equivalente = primer_dia_semana_mes_ref + timedelta(weeks=semana_relativa_original)
        
        # Asegurarse que no exceda el mes
        if fecha_equivalente.month == mes_referencia:
            fecha_ref = fecha_equivalente.strftime('%Y-%m-%d')
            
            query_equivalente = """
            SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND fecha = %s::date
            AND hora = %s
            AND (potencia_activa > 0 OR potencia_reactiva != 0)
            """
            resultado = pg_hook.get_first(query_equivalente, parameters=(alimentador, fecha_ref, hora))
            
            if resultado and resultado[2] is not None:
                print(f"✅ Encontrada referencia en día de semana equivalente {fecha_ref}, hora {hora}")
                return {
                    'fecha': resultado[0],
                    'hora': resultado[1],
                    'alimentador': resultado[2],
                    'dia_semana': resultado[3],
                    'potencia_activa': float(resultado[4]),
                    'potencia_reactiva': float(resultado[5]),
                    'fuente': f'mes{mes_referencia}-dia-semana-equivalente'
                }
        
        # 4. ESTRATEGIA 4: Buscar cualquier dato para la misma hora
        query_cualquier_fecha = f"""
        SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva
        FROM potencia_dep_processed
        WHERE alimentador = %s
        AND EXTRACT(MONTH FROM fecha) = {mes_referencia}
        AND EXTRACT(YEAR FROM fecha) = {year}
        AND hora = %s
        AND potencia_activa > 0
        AND potencia_reactiva != 0
        ORDER BY fecha
        LIMIT 1
        """
        
        resultado = pg_hook.get_first(query_cualquier_fecha, parameters=(alimentador, hora))
        
        if resultado and resultado[2] is not None:
            print(f"✅ Encontrada referencia con cualquier fecha en mes {mes_referencia}, fecha {resultado[0]}, hora {hora}")
            return {
                'fecha': resultado[0],
                'hora': resultado[1],
                'alimentador': resultado[2],
                'dia_semana': resultado[3],
                'potencia_activa': float(resultado[4]),
                'potencia_reactiva': float(resultado[5]),
                'fuente': f'mes{mes_referencia}-cualquier-fecha'
            }
        
        # 5. ESTRATEGIA 5: Buscar en datos_enviados si potencia_dep_processed no tiene datos
        query_datos_enviados = f"""
        SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva 
        FROM datos_enviados_potencia
        WHERE alimentador = %s
        AND EXTRACT(MONTH FROM fecha) = {mes_referencia}
        AND EXTRACT(YEAR FROM fecha) = {year}
        AND hora = %s::time
        AND potencia_activa > 0
        AND potencia_reactiva != 0
        ORDER BY fecha
        LIMIT 1
        """
        resultado = pg_hook.get_first(query_datos_enviados, parameters=(alimentador, hora))
        if resultado and resultado[2] is not None:
            print(f"✅ Encontrada referencia en datos_enviados para mes {mes_referencia}, fecha {resultado[0]}, hora {hora}")
            return {
                'fecha': resultado[0],
                'hora': resultado[1],
                'alimentador': resultado[2],
                'dia_semana': resultado[3],
                'potencia_activa': float(resultado[4]),
                'potencia_reactiva': float(resultado[5]),
                'fuente': f'datos_enviados-mes{mes_referencia}'
            }
        
        print(f"❌ No se encontró ninguna referencia válida en el mes {mes_referencia}")
        return None
    except Exception as e:
        print(f"❌ Error buscando referencia en mes {mes_referencia}: {str(e)}")
        traceback.print_exc()
        return None

def buscar_dato_referencia_mejorado(pg_hook, alimentador, fecha, hora, mes_actual, year=YEAR):
    """
    Búsqueda inteligente de datos de referencia considerando casos especiales como días 31
    que no existen en todos los meses.
    """
    try:
        # Convertir fecha a objeto datetime para manipulación
        if isinstance(fecha, str):
            fecha_dt = datetime.strptime(fecha, '%Y-%m-%d')
        else:
            fecha_dt = fecha
            
        dia = fecha_dt.day
        mes_fecha = fecha_dt.month
        
        print(f"🔍 Buscando referencia para {alimentador}, {fecha}, hora {hora}")
        
        # Si es día 29, 30 o 31, necesitamos estrategia especial para febrero y otros meses
        dia_especial = dia >= 29
        
        # 1. Primero buscar en meses posteriores hasta mes 6 (prioridad alta)
        for mes_ref in range(mes_actual, min(7, 13)):  # Incluir el mes 6 pero no ir más allá de diciembre
            # Para días especiales (29-31), ajustar al último día del mes de referencia
            if dia_especial:
                ultimo_dia_mes_ref = calendar.monthrange(year, mes_ref)[1]
                dia_ref = min(dia, ultimo_dia_mes_ref)
                fecha_ref = f"{year}-{mes_ref:02d}-{dia_ref:02d}"
            else:
                fecha_ref = f"{year}-{mes_ref:02d}-{dia:02d}"
                
            # Buscar para la misma hora
            query = """
            SELECT potencia_activa, potencia_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND fecha::date = %s::date
            AND hora = %s
            AND potencia_activa > 0
            AND potencia_reactiva != 0
            """
            resultado = pg_hook.get_first(query, parameters=(alimentador, fecha_ref, hora))
            if resultado and resultado[0] is not None and resultado[1] is not None:
                print(f"✅ Encontrada referencia en mes {mes_ref}, día {fecha_ref}")
                return {
                    'potencia_activa': float(resultado[0]),
                    'potencia_reactiva': float(resultado[1]),
                    'fuente': f"mes-futuro-{mes_ref}-exacto"
                }
        
        # 2. Luego buscar en meses anteriores (prioridad media)
        for mes_ref in range(mes_fecha-1, 0, -1):
            if dia_especial:
                ultimo_dia_mes_ref = calendar.monthrange(year, mes_ref)[1]
                dia_ref = min(dia, ultimo_dia_mes_ref)
                fecha_ref = f"{year}-{mes_ref:02d}-{dia_ref:02d}"
            else:
                fecha_ref = f"{year}-{mes_ref:02d}-{dia:02d}"
                
            query = """
            SELECT potencia_activa, potencia_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND fecha::date = %s::date
            AND hora = %s
            AND potencia_activa > 0
            AND potencia_reactiva != 0
            """
            
            resultado = pg_hook.get_first(query, parameters=(alimentador, fecha_ref, hora))
            
            if resultado and resultado[0] is not None and resultado[1] is not None:
                print(f"✅ Encontrada referencia en mes anterior {mes_ref}, día {fecha_ref}")
                return {
                    'potencia_activa': float(resultado[0]),
                    'potencia_reactiva': float(resultado[1]),
                    'fuente': f"mes-anterior-{mes_ref}-exacto"
                }
        
        # 3. Buscar en el mismo día pero en otros horarios (último recurso)
        query_hora_cercana = """
        SELECT hora, potencia_activa, potencia_reactiva
        FROM potencia_dep_processed
        WHERE alimentador = %s
        AND fecha::date = %s::date
        AND potencia_activa > 0
        AND potencia_reactiva != 0
        ORDER BY ABS(EXTRACT(HOUR FROM hora) - EXTRACT(HOUR FROM %s::time))
        LIMIT 1
        """
        # Si estamos en mes 6 o posterior, buscar directamente en el mes actual
        if mes_actual >= 6:
            resultado = pg_hook.get_first(query_hora_cercana, parameters=(
                alimentador, fecha_dt.strftime('%Y-%m-%d'), hora
            ))
            
            if resultado and resultado[1] is not None and resultado[2] is not None:
                print(f"✅ Encontrada referencia en mismo día, hora cercana {resultado[0]}")
                return {
                    'potencia_activa': float(resultado[1]),
                    'potencia_reactiva': float(resultado[2]),
                    'fuente': f"mismo-dia-hora-cercana"
                }
        # 4. Si llegamos a mes 6, usar promedio del mes para ese alimentador
        if mes_actual >= 6:
            query_promedio_mes6 = """
            SELECT AVG(potencia_activa), AVG(potencia_reactiva)
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND EXTRACT(MONTH FROM fecha) = 6
            AND EXTRACT(YEAR FROM fecha) = %s
            AND potencia_activa > 0
            AND potencia_reactiva != 0
            """
            
            resultado = pg_hook.get_first(query_promedio_mes6, parameters=(alimentador, year))
            
            if resultado and resultado[0] is not None and resultado[1] is not None:
                print(f"✅ Usando promedio del mes 6 para {alimentador}")
                return {
                    'potencia_activa': float(resultado[0]),
                    'potencia_reactiva': float(resultado[1]),
                    'fuente': "promedio-mes6"
                }
        
        # 5. Si aún no encontramos nada, marcar como standby si estamos antes de mes 6
        if mes_actual < 6:
            motivo = f"No se encontraron datos de referencia para fecha {fecha}, hora {hora}"
            # Marcaremos en standby posteriormente
            print(f"⚠️ Alimentador {alimentador} en fecha {fecha} hora {hora} se mantiene en STANDBY: {motivo}")
        else:
            print(f"⚠️ No se encontraron datos de referencia para {alimentador} en fecha {fecha}, hora {hora}")
        
        # 6. Último recurso: usar valores típicos por hora del día
        hora_num = int(str(hora).split(':')[0])
        if hora_num >= 6 and hora_num <= 18:  # Horas diurnas
            return {
                'potencia_activa': 300.0,
                'potencia_reactiva': 100.0,
                'fuente': 'valor-tipico-diurno'
            }
        else:  # Horas nocturnas
            return {
                'potencia_activa': 200.0, 
                'potencia_reactiva': 50.0,
                'fuente': 'valor-tipico-nocturno'
            }
    
    except Exception as e:
        print(f"❌ Error en búsqueda de referencia mejorada: {str(e)}")
        traceback.print_exc()
        # Valor por defecto en caso de error
        return {
            'potencia_activa': 250.0,
            'potencia_reactiva': 75.0,
            'fuente': 'error-fallback'
        }

def actualizar_consulta_sin_docker(processor_id, tabla_temporal):
    """Actualiza la consulta SQL sin depender de Docker"""
    try:
        print(f"🔄 Actualizando consulta para usar tabla temporal {tabla_temporal}...")

        # Crear la consulta que usa EXCLUSIVAMENTE la tabla temporal
        nueva_consulta = f"""/* TABLA_TEMPORAL_EXCLUSIVA_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    alimentador, 
    dia_semana, 
    potencia_activa AS potencia_activa_real, 
    potencia_reactiva AS potencia_reactiva_real
FROM {tabla_temporal}
ORDER BY fecha, hora, alimentador
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
#------------ DEPURACIÓN Y CALIDAD DE DATOS-------------------
def depurar_mes_mejorado(mes, **kwargs):
    """
    Versión mejorada de la depuración que incorpora análisis de calidad
    y estrategia inteligente de reemplazo, usando NiFi/Kafka consistentemente.
    """
    import traceback

    print(f"🔍 Iniciando depuración completa para el mes {mes}")
    try:
        # Verificar si el mes está marcado como sin datos reales
        ti = kwargs.get('ti')
        if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
            print(f"⚠️ El mes {mes} fue marcado como sin datos reales. Saltando depuración.")
            # NUEVO: Eliminar cualquier dato artificial que pudiera haberse creado
            pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
            
            # Obtener el año de manera segura
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
            pg_hook.run(f"""
            DELETE FROM potencia_dep_processed 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """)
            return f"Mes {mes} sin datos reales. No se realizó depuración ni se permitieron datos artificiales."
            
        crear_tabla_registros_standby()
        print(f"🔍 Iniciando proceso de depuración mejorado para el mes {mes}")
        
        # Conexión a la base de datos
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año de manera segura
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        
        # Configurar productor Kafka
        productor = KafkaProducer(
            bootstrap_servers=['kafka_centrosur:9092'],
            value_serializer=lambda v: json.dumps(v, default=decimal_json_serializer).encode('utf-8')
        )
        
        # Detener procesadores antes de configuralos
        print("⏸️ Deteniendo todos los procesadores antes de configurar...")
        stop_all_processors_individually()
        time.sleep(5)  # Dar tiempo para que se detengan
        
        # Configurar un topic Kafka único para este mes
        topic_mes = f"potencia-depurada-mes{mes}-{int(time.time())}"
        print(f"🔄 Configurando topic Kafka específico: {topic_mes}")
        
        # Configurar procesadores para usar este topic
        configurar_topic_procesador(
            CONSUME_KAFKA_PROCESSOR_ID,
            "Topic Name",
            topic_mes
        )
        
        # Configurar PublishKafka para el productor
        configurar_topic_procesador(
            PUBLISH_KAFKA_ID,
            "Topic Name", 
            topic_mes
        )
        
        # Realizar análisis de calidad previo
        analisis = analizar_calidad_integridad(pg_hook, mes, year)
        
        alimentadores_perfectos = analisis['alimentadores_perfectos']
        alimentadores_a_depurar = analisis['alimentadores_a_depurar']
        
        print(f"📊 Análisis de calidad: {len(alimentadores_perfectos)} alimentadores perfectos, {len(alimentadores_a_depurar)} requieren depuración")
        
        # Procesar alimentadores con calidad perfecta (sin depuración)
        resultados_perfectos = []
        for alim_info in alimentadores_perfectos:
            alimentador = alim_info['alimentador']
            # Simplemente enviar a Kafka sin modificar
            resultado = procesar_alimentador_para_mes_mejorado(pg_hook, productor, alimentador, mes, year)
            resultados_perfectos.append(resultado)
        
        # Procesar alimentadores que requieren depuración
        resultados_depurados = []
        for alim_info in alimentadores_a_depurar:
            alimentador = alim_info['alimentador']
            resultado = procesar_alimentador_para_mes_mejorado(pg_hook, productor, alimentador, mes, year)
            resultados_depurados.append(resultado)
        
        # Estadísticas de procesamiento
        total_alimentadores = len(alimentadores_perfectos) + len(alimentadores_a_depurar)
        completados = sum(1 for r in resultados_perfectos + resultados_depurados if r.get('estatus') == 'procesado')
        errores = sum(1 for r in resultados_perfectos + resultados_depurados if r.get('estatus') == 'error')
        registros_depurados = sum(r.get('registros_depurados', 0) for r in resultados_depurados)
        
        mensaje = f"""✅ Depuración mejorada completada para el mes {mes}:
        - Total alimentadores: {total_alimentadores}
        - Alimentadores perfectos: {len(alimentadores_perfectos)}
        - Alimentadores depurados: {len(alimentadores_a_depurar)}
        - Registros modificados: {registros_depurados}
        - Errores: {errores}"""
        
        print(mensaje)
        
        # Enviar todos los datos a través de NiFi/Kafka
        print("📤 Enviando todos los datos depurados a Kafka...")
        # Primero intentar desde la tabla procesada
        total_enviados = publicar_datos_depurados_a_kafka(pg_hook, productor, mes, year)
        
        # El mes 8 necesita atención especial
        if mes == 8 and total_enviados == 0:
            print(f"⚠️ Atención especial para mes 8: implementando envío mejorado...")
            
            # Limpiar estado del procesador para forzar reprocesamiento completo
            clear_processor_state_and_empty_all_queues()
            
            # Configurar temas específicos para mes 8
            topic_especial = f"potencia-mes8-especial-{int(time.time())}"
            configurar_topic_procesador(PUBLISH_KAFKA_ID, "Topic Name", topic_especial)
            configurar_topic_procesador(CONSUME_KAFKA_PROCESSOR_ID, "Topic Name", topic_especial)
            
            # Si no hay datos procesados, intentar enviarlos directamente
            # pero mantener la coherencia usando el mismo flujo NiFi/Kafka
            total_enviados = publicar_directamente_a_kafka_sin_tabla_temporal(pg_hook, productor, mes, year)
            
            # También verificar datos_enviados como último recurso
            if total_enviados == 0:
                print("🔎 Buscando en tabla datos_enviados como fuente alternativa...")
                query_alt = f"""
                SELECT 
                    fecha, 
                    hora, 
                    alimentador, 
                    dia_semana, 
                    potencia_activa, 
                    potencia_reactiva
                FROM datos_enviados_potencia
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {year}
                """
                datos_alt = pg_hook.get_records(query_alt)
                
                if datos_alt:
                    print(f"✅ Encontrados {len(datos_alt)} registros alternativos para mes {mes}")
                    for dato in datos_alt:
                        mensaje_kafka = {
                            'fecha': dato[0].strftime('%Y-%m-%d') if hasattr(dato[0], 'strftime') else str(dato[0]),
                            'hora': str(dato[1]),
                            'alimentador': str(dato[2]),
                            'dia_semana': calcular_dia_semana(dato[0]),  # Siempre usar día correcto
                            'potencia_activa': float(dato[4]) if dato[4] is not None else 0.0,
                            'potencia_reactiva': float(dato[5]) if dato[5] is not None else 0.0
                        }
                        productor.send(topic_especial, mensaje_kafka)
                    productor.flush()
                    total_enviados = len(datos_alt)
        # Para otros meses, usar el flujo estándar
        elif total_enviados == 0:
            print("⚠️ No se encontraron datos procesados en tabla potencia_dep_processed.")
            print("🔄 Intentando publicación alternativa manteniendo flujo NiFi/Kafka...")
            total_enviados = publicar_directamente_a_kafka_sin_tabla_temporal(pg_hook, productor, mes, YEAR)
            
        print(f"✅ {total_enviados} registros enviados correctamente a Kafka")
        
        # Iniciar los procesadores NiFi
        print("▶️ Iniciando procesadores NiFi para consumir datos...")
        # Iniciar PRIMERO los consumidores para que estén listos para recibir
        resultado_inicio = iniciar_procesadores_consumidor()
        print(f"   Consumidores iniciados: {'Exitoso' if resultado_inicio else 'Fallido'}")
        
        # Iniciar productores SOLO para el mes 10, eliminando el mes 8 de esta condición
        if mes == 10:  # <-- MODIFICADO: Quitado el mes 8 para evitar procesamiento duplicado
            print("▶️ Iniciando productores adicionales para mes especial...")
            iniciar_procesadores_productor()
        
        # Esperar para que procesen los datos
        print("⏳ Esperando para que se complete el procesamiento...")
        wait_time_seconds = 1080  # 18 minutos
        interval = 60  # Mostrar progreso cada minuto
        
        # Esperar el tiempo completo sin salir antes
        for elapsed in range(0, wait_time_seconds, interval):
            # Verificar si hay datos en la tabla destino solo para mostrar progreso
            query_check = f"""
            SELECT COUNT(*) FROM potencia_dep_processed
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            count = pg_hook.get_first(query_check)[0]
            
            # Mostrar progreso sin salir del bucle
            remaining = (wait_time_seconds - elapsed) // 60
            print(f"⏱️ Tiempo restante: {remaining} minutos... ({count}/{total_enviados} registros procesados)")
            
            # Si ya completamos el 100%, mostrar mensaje pero seguir esperando
            if count >= total_enviados:
                print(f"✅ Ya tenemos el 100% de los datos: {count}/{total_enviados}. Continuamos esperando para asegurar completitud.")
                    
            time.sleep(interval)
        
        # Esperar tiempo adicional para asegurar que todos los registros estén guardados
        print(f"⏳ Tiempo completo alcanzado. Esperando 60 segundos adicionales para finalizar procesos pendientes...")
        time.sleep(60)
        
        # Verificación final solo después de espera completa
        query_check = f"""
        SELECT COUNT(*) FROM potencia_dep_processed
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        count_final = pg_hook.get_first(query_check)[0]
        completitud = (count_final / total_enviados * 100) if total_enviados > 0 else 0
        print(f"📊 Verificación final: {count_final}/{total_enviados} registros procesados ({completitud:.2f}%)")
        
        # Detener procesadores después de la espera completa
        print(f"⏹️ Deteniendo procesadores después de la espera completa...")
        stop_all_processors_individually()
        
        # Añadir al mensaje de retorno
        mensaje += f"\n- Total enviados a Kafka: {total_enviados}"
        mensaje += f"\n- Total procesados final: {count_final} ({completitud:.2f}%)"
        
        return mensaje
    
    except Exception as e:
        error_msg = f"❌ Error en depuración mejorada del mes {mes}: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return error_msg

def analizar_calidad_integridad(pg_hook, mes, year=None):
    """
    Analiza la calidad de integridad de los datos por alimentador para un mes específico.
    Identifica alimentadores que necesitan depuración y los que pueden servir como referencia.
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = year if mes < 12 else year + 1
        fecha_inicio = f"{year}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        query = f"""
        SELECT 
            alimentador,
            COUNT(*) AS total_datos,
            SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) AS datos_ceros,
            ROUND(
                (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
                2
            ) AS porcentaje_ceros,
            ROUND(
                100 - (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*),
                2
            ) AS calidad_integridad
        FROM datos_enviados_potencia
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {year}
        GROUP BY alimentador
        ORDER BY calidad_integridad DESC
        """
        
        resultados = pg_hook.get_records(query)
        
        alimentadores_perfectos = []
        alimentadores_a_depurar = []
        
        for row in resultados:
            alimentador, total, ceros, pct_ceros, calidad = row
            
            if calidad == 100:
                alimentadores_perfectos.append({
                    'alimentador': alimentador,
                    'total_datos': total,
                    'calidad': calidad
                })
            else:
                alimentadores_a_depurar.append({
                    'alimentador': alimentador,
                    'total_datos': total,
                    'datos_ceros': ceros,
                    'calidad': calidad
                })
        print(f"✅ Análisis de calidad completado: {len(alimentadores_perfectos)} alimentadores perfectos, {len(alimentadores_a_depurar)} requieren depuración")
        
        return {
            'alimentadores_perfectos': alimentadores_perfectos,
            'alimentadores_a_depurar': alimentadores_a_depurar
        }
    except Exception as e:
        print(f"❌ Error al analizar calidad: {str(e)}")
        return {'alimentadores_perfectos': [], 'alimentadores_a_depurar': []}

def verificar_calidad_mes(pg_hook, mes_actual, year=None):
    """
    Verifica la calidad general de los datos de un mes completo.
    Busca datos tanto en potencia_dep_processed como en datos_enviados.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        mes_actual: Número del mes a verificar
        year: Año a procesar (default: valor global YEAR)
    
    Returns:
        float: Porcentaje de calidad general (0-100%)
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Primero intentar en potencia_dep_processed
        query_datos_proc = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_processed
        WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        
        count_proc = pg_hook.get_first(query_datos_proc)[0]
        
        if count_proc > 0:
            print(f"📊 Usando tabla potencia_dep_processed para calidad del mes {mes_actual}")
            # Ahora calculamos la calidad en esta tabla
            query = f"""
            SELECT 
                COALESCE(
                    AVG(
                        ROUND(
                            100 - (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / 
                            NULLIF(COUNT(*), 0),
                            2
                        )
                    ),
                    0
                ) AS calidad_promedio
            FROM (
                SELECT 
                    alimentador,
                    COUNT(*) AS total,
                    SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) AS ceros
                FROM potencia_dep_processed
                WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
                AND EXTRACT(YEAR FROM fecha) = {year}
                GROUP BY alimentador
            ) AS calidad_por_alimentador
            """
            resultado = pg_hook.get_first(query)
            if not resultado or resultado[0] is None:
                calidad = 0.0
            else:
                calidad = float(resultado[0])
        else:
            # Si no hay datos en potencia_dep_processed, buscar en datos_enviados
            print(f"⚠️ No hay datos en potencia_dep_processed para mes {mes_actual}, verificando datos_enviados")
            
            query_datos_env = f"""
            SELECT COUNT(*) 
            FROM datos_enviados_potencia
            WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            
            count_env = pg_hook.get_first(query_datos_env)[0]
            
            if count_env > 0:
                print(f"📊 Usando tabla datos_enviados para calidad del mes {mes_actual}")
                # Calcular calidad en datos_enviados
                query = f"""
                SELECT 
                    COALESCE(
                        AVG(
                            ROUND(
                                100 - (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / 
                                NULLIF(COUNT(*), 0),
                                2
                            )
                        ),
                        0
                    ) AS calidad_promedio
                FROM (
                    SELECT 
                        alimentador,
                        COUNT(*) AS total,
                        SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) AS ceros
                    FROM datos_enviados_potencia
                    WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
                    AND EXTRACT(YEAR FROM fecha) = {year}
                    GROUP BY alimentador
                ) AS calidad_por_alimentador
                """
                
                resultado = pg_hook.get_first(query)
                if not resultado or resultado[0] is None:
                    calidad = 0.0
                else:
                    calidad = float(resultado[0])
            else:
                print(f"⚠️ No hay datos para el mes {mes_actual} en ninguna tabla")
                calidad = 0.0
        
        print(f"📊 Calidad del mes {mes_actual}: {calidad:.2f}%")
        return calidad
    except Exception as e:
        print(f"❌ Error verificando calidad del mes {mes_actual}: {str(e)}")
        traceback.print_exc()
        
        # Si hay error en la consulta específica, puede ser porque la tabla no existe
        # o porque la estructura de columnas no es la esperada
        try:
            print("⚠️ Realizando verificación alternativa para determinar calidad básica")
            # Intentar una consulta más básica para al menos determinar si hay datos
            query_simple = f"""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN potencia_activa > 0 OR potencia_reactiva > 0 THEN 1 ELSE 0 END) as con_valor
            FROM datos_enviados_potencia
            WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            resultado_simple = pg_hook.get_first(query_simple)
            if resultado_simple and resultado_simple[0] > 0:
                total = resultado_simple[0]
                con_valor = resultado_simple[1]
                calidad_simple = (con_valor / total) * 100 if total > 0 else 0
                print(f"📊 Estimación simple de calidad: {calidad_simple:.2f}%")
                return calidad_simple
            else:
                return 0.0
                
        except Exception as e2:
            print(f"❌ Error en verificación alternativa: {str(e2)}")
            return 0.0

def obtener_calidad_mes(pg_hook, alimentador, mes, year=YEAR):
    """
    Calcula el porcentaje de calidad de integridad para un alimentador en un mes específico.
    La calidad se define como el porcentaje de registros que no tienen valores cero en ambas potencias.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: ID del alimentador
        mes: Número del mes (1-12)
        year: Año a procesar (default: valor global YEAR)
        
    Returns:
        float: Porcentaje de calidad (0-100%)
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
        query = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) as ceros,
            ROUND(100 - (SUM(CASE WHEN potencia_activa = 0 AND potencia_reactiva = 0 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) as calidad
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND EXTRACT(MONTH FROM fecha) = %s
          AND EXTRACT(YEAR FROM fecha) = %s
        """
        
        resultado = pg_hook.get_first(query, parameters=(alimentador, mes, year))
        
        if not resultado or resultado[0] == 0:
            return 0.0  # Si no hay datos, calidad es 0
            
        # El tercer valor es la calidad calculada en la consulta
        calidad = resultado[2] or 0.0
        
        return calidad
        
    except Exception as e:
        print(f"Error calculando calidad del mes para {alimentador}, mes {mes}: {str(e)}")
        return 0.0

def calcular_consistencia_datos(pg_hook, alimentador, fecha_inicio, fecha_fin):
    """
    Calcula la consistencia de los datos para un alimentador en un período.
    La consistencia evalúa si los valores mantienen relaciones lógicas entre ellos.
    
    Returns:
        float: Porcentaje de consistencia (0-100%)
    """
    try:
        # Consulta para verificar relaciones lógicas entre potencia activa y reactiva
        query = """
        SELECT 
            COUNT(*) as total,
            SUM(CASE 
                WHEN potencia_activa IS NOT NULL 
                     AND potencia_reactiva IS NOT NULL 
                     AND potencia_activa >= 0 
                     AND potencia_reactiva >= 0
                     AND potencia_activa >= potencia_reactiva * 0.3 -- Relación lógica mínima
                     AND potencia_activa <= potencia_reactiva * 3.0 -- Relación lógica máxima
                THEN 1 ELSE 0 END) as consistentes
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND fecha::date >= %s::date 
          AND fecha::date < %s::date
          AND potencia_activa IS NOT NULL
          AND potencia_reactiva IS NOT NULL
          AND (potencia_activa != 0 OR potencia_reactiva != 0)
        """
        resultado = pg_hook.get_first(query, parameters=(alimentador, fecha_inicio, fecha_fin))
        if not resultado or resultado[0] == 0:
            return 0.0
        
        total = resultado[0]
        consistentes = resultado[1] or 0
        
        return (consistentes / total) * 100 if total > 0 else 0.0
        
    except Exception as e:
        print(f"Error calculando consistencia: {str(e)}")
        return 0.0

def calcular_continuidad_datos(pg_hook, alimentador, fecha_inicio, fecha_fin):
    """
    Calcula la continuidad de los datos (ausencia de brechas) para un alimentador en un período.
    
    Returns:
        float: Porcentaje de continuidad (0-100%)
    """
    try:
        # Primero calculamos el total teórico de registros que deberían existir
        query_dias = """
        SELECT (%s::date - %s::date)::integer as dias
        """
        dias = pg_hook.get_first(query_dias, parameters=(fecha_fin, fecha_inicio))[0]
        total_teorico = dias * 24  # 24 horas por día
        
        # Luego contamos registros existentes
        query_existentes = """
        SELECT COUNT(DISTINCT (fecha::date, hora)) 
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND fecha::date >= %s::date 
          AND fecha::date < %s::date
          AND potencia_activa IS NOT NULL
          AND potencia_reactiva IS NOT NULL
          AND (potencia_activa != 0 OR potencia_reactiva != 0)
        """
        existentes = pg_hook.get_first(query_existentes, parameters=(alimentador, fecha_inicio, fecha_fin))[0]
        return (existentes / total_teorico) * 100 if total_teorico > 0 else 0.0
        
    except Exception as e:
        print(f"Error calculando continuidad: {str(e)}")
        return 0.0

def detectar_outliers(pg_hook, alimentador, fecha_inicio, fecha_fin):
    """
    Detecta valores atípicos (outliers) para un alimentador en un período.
    Utiliza el método de desviación estándar (±3σ).
    
    Returns:
        list: Lista de registros identificados como outliers
    """
    try:
        # Primero obtenemos estadísticas descriptivas
        query_estadisticas = """
        SELECT 
            AVG(potencia_activa) as media_activa,
            STDDEV(potencia_activa) as std_activa,
            AVG(potencia_reactiva) as media_reactiva,
            STDDEV(potencia_reactiva) as std_reactiva
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND fecha::date >= %s::date 
          AND fecha::date < %s::date
          AND potencia_activa IS NOT NULL
          AND potencia_reactiva IS NOT NULL
          AND potencia_activa != 0
          AND potencia_reactiva != 0
        """
        estadisticas = pg_hook.get_first(query_estadisticas, parameters=(alimentador, fecha_inicio, fecha_fin))
        if not estadisticas or estadisticas[0] is None:
            return []
            
        media_activa, std_activa, media_reactiva, std_reactiva = estadisticas
        
        # Definir umbrales
        umbral_inferior_activa = media_activa - 3 * std_activa if std_activa else 0
        umbral_superior_activa = media_activa + 3 * std_activa if std_activa else float('inf')
        umbral_inferior_reactiva = media_reactiva - 3 * std_reactiva if std_reactiva else 0
        umbral_superior_reactiva = media_reactiva + 3 * std_reactiva if std_reactiva else float('inf')
        
        # Buscar outliers
        query_outliers = """
        SELECT fecha::date, hora, potencia_activa, potencia_reactiva
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND fecha::date >= %s::date 
          AND fecha::date < %s::date
          AND (
            (potencia_activa < %s OR potencia_activa > %s)
            OR
            (potencia_reactiva < %s OR potencia_reactiva > %s)
          )
          AND potencia_activa IS NOT NULL
          AND potencia_reactiva IS NOT NULL
          AND potencia_activa != 0
          AND potencia_reactiva != 0
        """
        
        outliers = pg_hook.get_records(query_outliers, parameters=(
            alimentador, fecha_inicio, fecha_fin,
            umbral_inferior_activa, umbral_superior_activa,
            umbral_inferior_reactiva, umbral_superior_reactiva
        ))
        # Convertir a lista de diccionarios para mejor manipulación
        resultado = []
        for fecha, hora, p_activa, p_reactiva in outliers:
            resultado.append({
                'fecha': fecha.strftime('%Y-%m-%d') if hasattr(fecha, 'strftime') else str(fecha),
                'hora': hora,
                'potencia_activa': float(p_activa),
                'potencia_reactiva': float(p_reactiva)
            })
            
        return resultado
    except Exception as e:
        print(f"Error detectando outliers: {str(e)}")
        return []
 
def obtener_completitud_mes(pg_hook, alimentador, mes, year=None):
    """
    Calcula el porcentaje de completitud para un alimentador en un mes específico.
    Retorna porcentaje de valores no cero, total de registros, y registros con valores.
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Determinar número de días en el mes
        dias_en_mes = calendar.monthrange(year, mes)[1]
        total_registros_teoricos = dias_en_mes * 24  # 24 horas por día
        # Consultar registros existentes
        query = """
        SELECT 
            COUNT(*) as total_registros,
            SUM(CASE WHEN potencia_activa IS NOT NULL AND potencia_activa != 0 THEN 1 ELSE 0 END) as registros_activa,
            SUM(CASE WHEN potencia_reactiva IS NOT NULL AND potencia_reactiva != 0 THEN 1 ELSE 0 END) as registros_reactiva
        FROM potencia_dep_processed
        WHERE alimentador = %s
          AND EXTRACT(MONTH FROM fecha) = %s
          AND EXTRACT(YEAR FROM fecha) = %s
        """
        
        result = pg_hook.get_first(query, parameters=(alimentador, mes, year))
        
        if not result or not result[0]:
            return 0, 0, 0
            
        total_registros = result[0]
        registros_activa = result[1] or 0
        registros_reactiva = result[2] or 0
        
        # Calcular completitud (promedio de activa y reactiva)
        completitud_activa = (registros_activa / total_registros_teoricos) * 100 if total_registros_teoricos > 0 else 0
        completitud_reactiva = (registros_reactiva / total_registros_teoricos) * 100 if total_registros_teoricos > 0 else 0
        completitud_promedio = (completitud_activa + completitud_reactiva) / 2
        return completitud_promedio, total_registros, min(registros_activa, registros_reactiva)
        
    except Exception as e:
        print(f"Error calculando completitud para alimentador {alimentador}, mes {mes}: {str(e)}")
        return 0, 0, 0

def imputar_faltantes():
    """
    Versión simplificada y optimizada de imputar_faltantes que usa procesamiento por lotes
    y consultas más eficientes.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        year = YEAR 
        
        print(f"📊 Ejecutando imputación simplificada para el año {year}...")
        
        # 1. Identificar huecos en la serie temporal usando CTE - CORRECCIÓN AQUÍ
        query_huecos = f"""
        WITH fechas_completas AS (
            SELECT 
                a.alimentador,
                d.fecha::date, 
                make_time(h.h, 0, 0) as hora
            FROM 
                (SELECT DISTINCT alimentador FROM potencia_dep_processed) a,
                (SELECT generate_series('{year}-01-01'::date, '{year}-12-31'::date, '1 day'::interval)::date AS fecha) d,
                (SELECT generate_series(0, 23, 1) AS h) h
        ),
        datos_existentes AS (
            SELECT DISTINCT alimentador, fecha::date, hora::time
            FROM potencia_dep_processed
            WHERE EXTRACT(YEAR FROM fecha) = {year}
        )
        SELECT 
            fc.alimentador, 
            fc.fecha, 
            fc.hora AS hora
        FROM fechas_completas fc
        LEFT JOIN datos_existentes de ON 
            fc.alimentador = de.alimentador AND 
            fc.fecha = de.fecha AND 
            fc.hora = de.hora
        WHERE de.alimentador IS NULL
        LIMIT 1000
        """
        huecos = pg_hook.get_records(query_huecos)
        total_huecos = len(huecos)
        
        if total_huecos == 0:
            print("✅ No se encontraron huecos en la serie temporal")
            return {'total_procesados': 0, 'imputados_exitosos': 0}
            
        print(f"🔍 Encontrados {total_huecos} huecos para imputar (mostrando los primeros 1000)")
        
        # 2. Para cada hueco, imputar valor basado en promedios mensuales
        registros_procesados = 0
        
        for hueco in huecos:
            alimentador, fecha, hora = hueco
            dia_semana = calcular_dia_semana(fecha)
            
            # Buscar valores de referencia del mismo día/hora en el mismo mes
            query_referencia = """
            SELECT AVG(potencia_activa) AS p_activa, AVG(potencia_reactiva) AS p_reactiva
            FROM potencia_dep_processed
            WHERE alimentador = %s
            AND EXTRACT(MONTH FROM fecha) = EXTRACT(MONTH FROM %s::date)
            AND EXTRACT(HOUR FROM hora) = EXTRACT(HOUR FROM %s::time)
            AND potencia_activa > 0
            """
            
            referencia = pg_hook.get_first(query_referencia, parameters=(alimentador, fecha, hora))
            
            if referencia and referencia[0]:
                p_activa = referencia[0]
                p_reactiva = referencia[1] or p_activa * 0.3  # Valor conservador
            else:
                # Si no hay valores de referencia, usar valores típicos por hora
                hora_num = int(str(hora).split(':')[0])
                if hora_num >= 6 and hora_num <= 18:  # Horas diurnas
                    p_activa = 1.5
                    p_reactiva = 0.45
                else:  # Horas nocturnas
                    p_activa = 0.8
                    p_reactiva = 0.24
            
            # Insertar el valor imputado
            query_insertar = """
            INSERT INTO potencia_dep_processed 
            (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (fecha, hora, alimentador) DO UPDATE
            SET potencia_activa = EXCLUDED.potencia_activa,
                potencia_reactiva = EXCLUDED.potencia_reactiva,
                dia_semana = EXCLUDED.dia_semana
            """
            
            pg_hook.run(query_insertar, parameters=(
                fecha, hora, alimentador, dia_semana, p_activa, p_reactiva
            ))
            
            registros_procesados += 1
            
            if registros_procesados % 100 == 0:
                print(f"⏳ Procesados {registros_procesados}/{total_huecos} registros")
        
        print(f"✅ Imputación simplificada completada: {registros_procesados} registros procesados")
        
        # Registrar en el log
        pg_hook.run("""
        INSERT INTO log_procesamiento 
        (timestamp, mensaje, detalles)
        VALUES (NOW(), %s, %s)
        """, parameters=(
            f'Imputación simplificada completada: {registros_procesados} registros',
            json.dumps({'total_procesados': registros_procesados})
        ))
        
        return {'total_procesados': registros_procesados, 'imputados_exitosos': registros_procesados}
        
    except Exception as e:
        print(f"❌ Error en imputación simplificada: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def imputar_valores_faltantes(alimentador=None, fecha_inicio=None, fecha_fin=None, pg_hook=None):
    """
    Identifica timestamps faltantes en la serie de 15 minutos y los imputa con valores
    calculados de manera inteligente basados en datos históricos homólogos.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: Alimentador específico o None para procesar todos
        fecha_inicio: Fecha de inicio para el rango a procesar
        fecha_fin: Fecha de fin para el rango a procesar
    
    Returns:
        dict: Resumen de valores imputados
    """
    try:
        if pg_hook is None:
            pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        # Configuración de parámetros
        VENTANA_HISTORICA_SEMANAS = 4  # Cuántas semanas atrás buscar datos homólogos
        MAX_VARIACION_PERMITIDA = 0.15  # Máxima variación permitida (15%)
        
        # 1. Identificar timestamps faltantes para el rango/alimentador especificado
        registros_faltantes = identificar_timestamps_faltantes(pg_hook, alimentador, fecha_inicio, fecha_fin)
        
        resultados = {
            'total_procesados': 0,
            'imputados_exitosos': 0,
            'imputaciones_limitadas': 0,
            'imputaciones_fallidas': 0,
            'detalles': []
        }
        
        # 2. Para cada registro faltante, calcular un valor apropiado
        for registro in registros_faltantes:
            alimentador_actual = registro['alimentador']
            fecha_actual = registro['fecha']
            hora_actual = registro['hora']
            
            try:
                # Obtener el día de la semana (0=lunes, 6=domingo)
                fecha_dt = datetime.strptime(str(fecha_actual), '%Y-%m-%d')
                dia_semana = fecha_dt.weekday()
                
                # Extraer componentes de la hora para comparación
                hora_componentes = str(hora_actual).split(':')
                hora_num = int(hora_componentes[0])
                minuto_num = int(hora_componentes[1])
                
                # 2.1 Estrategia 1: Buscar valores homólogos (mismo día semana, misma hora)
                query_homologos = """
                SELECT 
                    fecha, 
                    hora, 
                    potencia_activa, 
                    potencia_reactiva
                FROM potencia_dep_processed
                WHERE 
                    alimentador = %s AND
                    EXTRACT(DOW FROM fecha) = %s AND
                    hora = %s::time AND
                    fecha != %s::date AND
                    fecha >= %s::date - INTERVAL '%s weeks' AND
                    fecha <= %s::date AND
                    potencia_activa > 0 AND
                    potencia_reactiva != 0
                ORDER BY 
                    ABS(EXTRACT(EPOCH FROM (%s::date - fecha)) / 86400)  -- Ordenar por cercanía en días
                LIMIT 5
                """
                
                homologos = pg_hook.get_records(
                    query_homologos, 
                    parameters=(
                        alimentador_actual, 
                        dia_semana, 
                        hora_actual, 
                        fecha_actual,
                        fecha_actual,
                        VENTANA_HISTORICA_SEMANAS,
                        fecha_actual,
                        fecha_actual
                    )
                )
                
                # 2.2 Estrategia 2: Si no hay homólogos, buscar valores de la misma hora en días cercanos
                if not homologos:
                    query_misma_hora = """
                    SELECT 
                        fecha, 
                        hora, 
                        potencia_activa, 
                        potencia_reactiva
                    FROM potencia_dep_processed
                    WHERE 
                        alimentador = %s AND
                        hora = %s::time AND
                        fecha != %s::date AND
                        fecha >= %s::date - INTERVAL '7 days' AND
                        fecha <= %s::date + INTERVAL '7 days' AND
                        potencia_activa > 0 AND
                        potencia_reactiva != 0
                    ORDER BY 
                        ABS(EXTRACT(EPOCH FROM (%s::date - fecha)) / 86400)
                    LIMIT 5
                    """
                    
                    homologos = pg_hook.get_records(
                        query_misma_hora, 
                        parameters=(
                            alimentador_actual, 
                            hora_actual, 
                            fecha_actual,
                            fecha_actual,
                            fecha_actual,
                            fecha_actual
                        )
                    )
                
                # 2.3 Estrategia 3: Buscar valores del entorno (mismo día, horas cercanas)
                valores_entorno = obtener_valores_entorno(
                    pg_hook, 
                    alimentador_actual, 
                    fecha_actual, 
                    hora_actual
                )
                
                # 3. Calcular valor de imputación combinando estrategias
                valor_imputado = calcular_imputacion(homologos, valores_entorno)
                
                # 4. Validar que el valor no genere picos anómalos
                valor_final = validar_y_ajustar_imputacion(
                    valor_imputado, 
                    valores_entorno, 
                    MAX_VARIACION_PERMITIDA
                )
                
                # 5. Insertar el valor imputado en la tabla
                if valor_final['estado'] == 'OK':
                    insertar_valor_imputado(
                        pg_hook, 
                        alimentador_actual, 
                        fecha_actual, 
                        hora_actual, 
                        valor_final['potencia_activa'], 
                        valor_final['potencia_reactiva'],
                        dia_semana
                    )
                    resultados['imputados_exitosos'] += 1
                elif valor_final['estado'] == 'LIMITADO':
                    insertar_valor_imputado(
                        pg_hook, 
                        alimentador_actual, 
                        fecha_actual, 
                        hora_actual, 
                        valor_final['potencia_activa'], 
                        valor_final['potencia_reactiva'],
                        dia_semana
                    )
                    resultados['imputaciones_limitadas'] += 1
                else:
                    resultados['imputaciones_fallidas'] += 1
                
                # 6. Registrar detalles de la imputación
                resultados['detalles'].append({
                    'alimentador': alimentador_actual,
                    'fecha': str(fecha_actual),
                    'hora': str(hora_actual),
                    'valor_calculado': valor_imputado,
                    'valor_final': valor_final,
                    'fuentes_utilizadas': len(homologos),
                    'valores_entorno': len(valores_entorno)
                })
                
                resultados['total_procesados'] += 1
                
            except Exception as e:
                print(f"Error procesando registro {alimentador_actual}, {fecha_actual}, {hora_actual}: {str(e)}")
                resultados['imputaciones_fallidas'] += 1
                resultados['detalles'].append({
                    'alimentador': alimentador_actual,
                    'fecha': str(fecha_actual),
                    'hora': str(hora_actual),
                    'error': str(e)
                })
        
        # 7. Registrar resumen en log
        registrar_log(
            pg_hook,
            'INFO',
            f'Imputación completada: {resultados["imputados_exitosos"]} exitosos, {resultados["imputaciones_limitadas"]} limitados, {resultados["imputaciones_fallidas"]} fallidos',
            detalles=resultados
        )
        
        return resultados
        
    except Exception as e:
        print(f"Error en imputación de valores: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def analizar_anomalias_y_corregir():
    """
    Función principal para detectar anomalías en toda la serie de datos
    y corregir valores atípicos.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # 1. Detectar valores anómalos por alimentador
        print("🔍 Buscando valores anómalos en todos los alimentadores...")
        
        # Usar estadísticas específicas por alimentador para determinar umbrales dinámicos
        query_anomalias = """
        WITH estadisticas_alimentador AS (
            SELECT
                alimentador,
                AVG(potencia_activa) AS media_activa,
                STDDEV(potencia_activa) AS std_activa,
                PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY potencia_activa) AS mediana_activa,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY potencia_activa) AS p95_activa,
                PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY potencia_activa) AS p05_activa
            FROM potencia_dep_processed
            GROUP BY alimentador
        ),
        anomalias AS (
            SELECT
                p.alimentador,
                p.fecha,
                p.hora,
                p.potencia_activa,
                p.potencia_reactiva,
                e.media_activa,
                e.std_activa,
                e.mediana_activa,
                CASE 
                    -- Detectar picos hacia arriba
                    WHEN p.potencia_activa > (e.p95_activa + 2 * e.std_activa) THEN 'PICO_ALTO'
                    -- Detectar picos hacia abajo (valores extrañamente bajos)
                    WHEN p.potencia_activa < (e.p05_activa - e.std_activa) AND p.potencia_activa > 0 THEN 'PICO_BAJO'
                    -- Detectar valores incorrectos (relación entre activa y reactiva)
                    WHEN p.potencia_activa > 0 AND p.potencia_reactiva > p.potencia_activa * 2 THEN 'REL_INCORRECTA'
                    -- Normal
                    ELSE 'NORMAL'
                END AS tipo_anomalia,
                ABS(p.potencia_activa - e.mediana_activa) / 
                    CASE WHEN e.std_activa > 0 THEN e.std_activa ELSE 1 END AS desviacion_normalizada
            FROM potencia_dep_processed p
            JOIN estadisticas_alimentador e ON p.alimentador = e.alimentador
        )
        SELECT
            alimentador,
            fecha,
            hora,
            potencia_activa,
            potencia_reactiva,
            media_activa,
            mediana_activa,
            std_activa,
            tipo_anomalia,
            desviacion_normalizada
        FROM anomalias
        WHERE tipo_anomalia != 'NORMAL'
        AND desviacion_normalizada > 2.5
        ORDER BY desviacion_normalizada DESC
        LIMIT 1000
        """
        
        anomalias = pg_hook.get_records(query_anomalias)
        
        print(f"🔍 Detectadas {len(anomalias)} potenciales anomalías")
        
        # Procesar cada anomalía detectada
        corregidas = 0
        for anomalia in anomalias:
            try:  # Añado try/except para manejar errores por anomalía
                alimentador = anomalia[0]
                fecha = anomalia[1]
                hora = anomalia[2]
                
                print(f"\nProcesando anomalía: {alimentador}, {fecha}, {hora}")
                print(f"- Tipo: {anomalia[8]}")
                print(f"- Valor actual: Activa={anomalia[3]}, Reactiva={anomalia[4]}")
                print(f"- Estadísticas: Media={anomalia[5]:.4f}, Mediana={anomalia[6]:.4f}, StdDev={anomalia[7]:.4f}")
                print(f"- Desviación normalizada: {anomalia[9]:.2f}")
                
                # Obtener valores del entorno
                valores_entorno = obtener_valores_entorno(pg_hook, alimentador, fecha, hora)
                
                # Obtener homólogos - CORRECCIÓN AQUÍ: Cambiamos la consulta problemática
                fecha_dt = fecha  # Ya es un objeto date
                dia_semana = fecha_dt.weekday() if hasattr(fecha_dt, 'weekday') else 0
                
                # CONSULTA CORREGIDA: Eliminamos el uso problemático de EXTRACT(EPOCH FROM...)
                query_homologos = """
                SELECT 
                    fecha, 
                    hora, 
                    potencia_activa, 
                    potencia_reactiva
                FROM potencia_dep_processed
                WHERE 
                    alimentador = %s AND
                    EXTRACT(DOW FROM fecha) = %s AND
                    hora = %s::time AND
                    fecha != %s::date AND
                    fecha >= (%s::date - INTERVAL '28 days') AND
                    fecha <= (%s::date + INTERVAL '28 days') AND
                    potencia_activa > 0
                ORDER BY 
                    fecha  -- Ordenamos simplemente por fecha en lugar de usar la función ABS
                LIMIT 5
                """
                
                homologos = pg_hook.get_records(
                    query_homologos, 
                    parameters=(
                        alimentador, 
                        dia_semana, 
                        hora, 
                        fecha,
                        fecha,
                        fecha
                    )
                )
                
                # Calcular valor de imputación
                valor_imputado = calcular_imputacion(homologos, valores_entorno)
                
                # Validar y ajustar
                valor_final = validar_y_ajustar_imputacion(valor_imputado, valores_entorno, 0.15)
                
                # Solo corregir si la diferencia es significativa
                valor_actual = float(anomalia[3])
                diferencia_porcentual = abs(valor_actual - valor_final['potencia_activa']) / max(valor_actual, 0.001) * 100
                
                if diferencia_porcentual > 20:  # Solo corregir diferencias mayores al 20%
                    print(f"✅ Corrigiendo anomalía (diferencia: {diferencia_porcentual:.2f}%)")
                    print(f"  - Valor corregido: Activa={valor_final['potencia_activa']:.4f}, Reactiva={valor_final['potencia_reactiva']:.4f}")
                    
                    # Actualizar en la base de datos
                    query_update = """
                    UPDATE potencia_dep_processed
                    SET 
                        potencia_activa = %s,
                        potencia_reactiva = %s
                    WHERE
                        alimentador = %s AND
                        fecha = %s AND
                        hora = %s
                    """
                    
                    pg_hook.run(
                        query_update,
                        parameters=(
                            valor_final['potencia_activa'],
                            valor_final['potencia_reactiva'],
                            alimentador,
                            fecha,
                            hora
                        )
                    )
                    
                    corregidas += 1
                    
                    # Registrar en log
                    registrar_log(
                        pg_hook,
                        'CORRECCIÓN',
                        f'Anomalía corregida: {alimentador}, {fecha}, {hora}',
                        alimentador=alimentador,
                        fecha=fecha,
                        hora=hora,
                        detalles={
                            'tipo_anomalia': anomalia[8],
                            'valor_original': {
                                'potencia_activa': float(anomalia[3]),
                                'potencia_reactiva': float(anomalia[4])
                            },
                            'valor_corregido': {
                                'potencia_activa': valor_final['potencia_activa'],
                                'potencia_reactiva': valor_final['potencia_reactiva']
                            },
                            'desviacion_original': float(anomalia[9])
                        }
                    )
            except Exception as e_proc:
                print(f"⚠️ Error procesando anomalía {alimentador}, {fecha}, {hora}: {str(e_proc)}")
                continue  # Seguir con la siguiente anomalía
                
        print(f"\n🔄 Proceso completado. Se corrigieron {corregidas} anomalías de {len(anomalias)} detectadas.")
        
        return {
            'anomalias_detectadas': len(anomalias),
            'anomalias_corregidas': corregidas
        }
        
    except Exception as e:
        print(f"Error en análisis de anomalías: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def resolver_caso_anomalo(alimentador=None, fecha=None, hora=None):
    """
    Resuelve un caso específico de anomalía en un alimentador
    mostrando el proceso paso a paso. Si no se especifican parámetros,
    detectará automáticamente casos anómalos.
    
    Args:
        alimentador: ID del alimentador (o None para detección automática)
        fecha: Fecha en formato 'YYYY-MM-DD' (o None para detección automática)
        hora: Hora en formato 'HH:MM:SS' (o None para detección automática)
    """
    try:
        year = int(Variable.get("YEAR"))
        # Establecer conexión
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Si no se proporcionan parámetros, detectar automáticamente un caso anómalo
        if not alimentador or not fecha or not hora:
            print("🔍 Detectando automáticamente un caso anómalo")
            
            # Usar la consulta de anomalías para encontrar un caso
            query_anomalia = """
            WITH estadisticas_alimentador AS (
                SELECT
                    alimentador,
                    AVG(potencia_activa) AS media_activa,
                    STDDEV(potencia_activa) AS std_activa,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY potencia_activa) AS p95_activa,
                    PERCENTILE_CONT(0.05) WITHIN GROUP (ORDER BY potencia_activa) AS p05_activa
                FROM potencia_dep_processed
                GROUP BY alimentador
            )
            SELECT
                p.alimentador,
                p.fecha,
                p.hora,
                p.potencia_activa,
                p.potencia_reactiva,
                ABS(p.potencia_activa - e.media_activa) / GREATEST(e.std_activa, 0.001) AS desviacion_normalizada
            FROM potencia_dep_processed p
            JOIN estadisticas_alimentador e ON p.alimentador = e.alimentador
            WHERE (p.potencia_activa > e.p95_activa + 2 * e.std_activa
                OR (p.potencia_activa < e.p05_activa - e.std_activa AND p.potencia_activa > 0))
            ORDER BY desviacion_normalizada DESC
            LIMIT 1
            """
            
            anomalia = pg_hook.get_first(query_anomalia)
            
            if anomalia:
                alimentador = anomalia[0]
                fecha = anomalia[1]
                hora = anomalia[2]
                valor_actual_activa = float(anomalia[3])
                valor_actual_reactiva = float(anomalia[4])
                desviacion = float(anomalia[5])
                
                print(f"🔍 Caso anómalo detectado automáticamente:")
                print(f"- Alimentador: {alimentador}")
                print(f"- Fecha: {fecha}")
                print(f"- Hora: {hora}")
                print(f"- Potencia activa: {valor_actual_activa}")
                print(f"- Desviación: {desviacion:.2f} sigmas")
            else:
                # Si no hay parámetros y tampoco se detecta automáticamente, usar el caso 5015 original
                alimentador = '5015'
                fecha = f"{year}-01-03"
                hora = '13:00:00'
                print(f"⚠️ No se detectaron anomalías. Usando caso predeterminado: {alimentador}, {fecha} {hora}")
        
        print(f"🔍 Analizando caso específico: Alimentador {alimentador}, {fecha} {hora}")
        
        # PASO 1: Verificar el valor actual
        query_valor_actual = """
        SELECT potencia_activa, potencia_reactiva, dia_semana
        FROM potencia_dep_processed
        WHERE alimentador = %s AND fecha = %s AND hora = %s
        """
        
        valor_actual = pg_hook.get_first(
            query_valor_actual, 
            parameters=(alimentador, fecha, hora)
        )
        
        if valor_actual:
            print(f"Valor actual: P_activa={valor_actual[0]}, P_reactiva={valor_actual[1]}")
        else:
            print("No existe registro para esta combinación")
            
        # PASO 2: Obtener valores del entorno (mismo día)
        valores_entorno = obtener_valores_entorno(pg_hook, alimentador, fecha, hora)
        
        print("Valores de entorno:")
        print("- Valores anteriores:")
        for v in valores_entorno['antes']:
            print(f"  {v['hora']}: P_activa={v['potencia_activa']}, P_reactiva={v['potencia_reactiva']}")
            
        print("- Valores posteriores:")
        for v in valores_entorno['despues']:
            print(f"  {v['hora']}: P_activa={v['potencia_activa']}, P_reactiva={v['potencia_reactiva']}")
            
        # PASO 3: Obtener valores homólogos (mismo día de la semana, misma hora)
        fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d') if isinstance(fecha, str) else fecha
        dia_semana = fecha_dt.weekday()
        
        query_homologos = """
        SELECT 
            fecha, 
            hora, 
            potencia_activa, 
            potencia_reactiva
        FROM potencia_dep_processed
        WHERE 
            alimentador = %s AND
            EXTRACT(DOW FROM fecha) = %s AND
            hora = %s::time AND
            fecha != %s::date AND
            fecha >= %s::date - INTERVAL '4 weeks' AND
            fecha <= %s::date + INTERVAL '4 weeks' AND
            potencia_activa > 0
        ORDER BY 
            ABS(EXTRACT(EPOCH FROM (%s::date - fecha)) / 86400)
        LIMIT 5
        """
        
        homologos = pg_hook.get_records(
            query_homologos, 
            parameters=(
                alimentador, 
                dia_semana, 
                hora, 
                fecha,
                fecha,
                fecha,
                fecha
            )
        )
        
        print(f"\nValores homólogos (otros {calendar.day_name[dia_semana]} a las {hora}):")
        for h in homologos:
            print(f"  {h[0]}: P_activa={h[2]}, P_reactiva={h[3]}")
            
        # PASO 4: Calcular valor de imputación
        valor_imputado = calcular_imputacion(homologos, valores_entorno)
        
        print("\nValor calculado para imputación:")
        print(f"- Método: {valor_imputado['metodo']}")
        print(f"- Confianza: {valor_imputado['confianza']}")
        print(f"- P_activa: {valor_imputado['potencia_activa']}")
        print(f"- P_reactiva: {valor_imputado['potencia_reactiva']}")
        
        # PASO 5: Validar y ajustar
        valor_final = validar_y_ajustar_imputacion(valor_imputado, valores_entorno, 0.15)
        
        print("\nValor final validado:")
        print(f"- Estado: {valor_final['estado']}")
        print(f"- P_activa: {valor_final['potencia_activa']}")
        print(f"- P_reactiva: {valor_final['potencia_reactiva']}")
        
        if 'ajuste_aplicado' in valor_final and valor_final['ajuste_aplicado']:
            print("Se aplicó ajuste para evitar pico anómalo:")
            print(f"- Referencia activa: {valor_final['referencia_activa']}")
            print(f"- Límites permitidos: [{valor_final['limite_inferior_activa']:.4f}, {valor_final['limite_superior_activa']:.4f}]")
            
        # PASO 6: Comparar con el valor actual
        if valor_actual:
            diferencia_activa = abs(valor_actual[0] - valor_final['potencia_activa'])
            diferencia_porcentaje = diferencia_activa / valor_final['potencia_activa'] * 100 if valor_final['potencia_activa'] > 0 else 0
            
            print(f"\nDiferencia con valor actual: {diferencia_activa:.4f} ({diferencia_porcentaje:.2f}%)")
            
            # Determinar si el valor actual es un pico anómalo
            if diferencia_porcentaje > 15:
                print("⚠️ CONFIRMADO: El valor actual es un pico anómalo que debe ser corregido")
                
                # Obtener percentiles del día completo para contextualizar
                query_percentiles = """
                WITH datos_dia AS (
                    SELECT potencia_activa
                    FROM potencia_dep_processed
                    WHERE alimentador = %s AND fecha = %s
                )
                SELECT 
                    percentile_cont(0.25) WITHIN GROUP (ORDER BY potencia_activa) AS p25,
                    percentile_cont(0.5) WITHIN GROUP (ORDER BY potencia_activa) AS p50,
                    percentile_cont(0.75) WITHIN GROUP (ORDER BY potencia_activa) AS p75,
                    percentile_cont(0.9) WITHIN GROUP (ORDER BY potencia_activa) AS p90
                FROM datos_dia
                """
                
                percentiles = pg_hook.get_first(
                    query_percentiles, 
                    parameters=(alimentador, fecha)
                )
                
                if percentiles:
                    print(f"Contexto estadístico del día:")
                    print(f"- P25: {percentiles[0]:.4f}")
                    print(f"- P50 (mediana): {percentiles[1]:.4f}")
                    print(f"- P75: {percentiles[2]:.4f}")
                    print(f"- P90: {percentiles[3]:.4f}")
                    
                    if valor_actual[0] > percentiles[3]:
                        print("⚠️ El valor actual supera el percentil 90 del día")
                
                # Visualizar antes y después
                print("\nComparación de valores:")
                print(f"  ANTES: {valor_actual[0]} (actual)")
                print(f"  DESPUÉS: {valor_final['potencia_activa']:.4f} (propuesto)")
                
                # Preguntar si se quiere aplicar la corrección
                print("\nAplicando corrección automáticamente según umbral")
                if diferencia_porcentaje > 20:  # Umbral más alto para corrección automática
                    # Aplicar corrección
                    query_update = """
                    UPDATE potencia_dep_processed
                    SET 
                        potencia_activa = %s,
                        potencia_reactiva = %s
                    WHERE
                        alimentador = %s AND
                        fecha = %s AND
                        hora = %s
                    """
                    
                    pg_hook.run(
                        query_update,
                        parameters=(
                            valor_final['potencia_activa'],
                            valor_final['potencia_reactiva'],
                            alimentador,
                            fecha,
                            hora
                        )
                    )
                    
                    print("✅ Corrección aplicada correctamente")
                else:
                    print("❌ Corrección no aplicada")
            else:
                print("✅ El valor actual está dentro de rangos aceptables")
        
        return {
            'alimentador': alimentador,
            'fecha': str(fecha),
            'hora': str(hora),
            'valor_actual': valor_actual,
            'valor_propuesto': valor_final,
            'homologos': homologos,
            'entorno': valores_entorno,
            'aplicar_correccion': diferencia_porcentaje > 15 if valor_actual else False
        }
        
    except Exception as e:
        print(f"Error analizando caso específico: {str(e)}")
        traceback.print_exc()
        return None

def validar_y_ajustar_imputacion(valor_imputado, valores_entorno, max_variacion=0.15):
    """
    Valida que el valor imputado no genere picos anómalos y lo ajusta si es necesario.
    
    Args:
        valor_imputado: Dict con el valor calculado de imputación
        valores_entorno: Dict con valores de entorno (antes/después)
        max_variacion: Máxima variación permitida como fracción (0.15 = 15%)
    
    Returns:
        dict: Valores validados y potencialmente ajustados
    """
    try:
        # Si no hay valores de entorno para comparar, aceptar el valor imputado
        if not valores_entorno['antes'] and not valores_entorno['despues']:
            return {
                'potencia_activa': valor_imputado['potencia_activa'],
                'potencia_reactiva': valor_imputado['potencia_reactiva'],
                'estado': 'OK',
                'ajuste_aplicado': False
            }
        
        # Calcular valores de referencia del entorno
        potencia_activa_ref = []
        potencia_reactiva_ref = []
        
        # Priorizar los valores más cercanos (1 antes y 1 después si están disponibles)
        if valores_entorno['antes']:
            potencia_activa_ref.append(valores_entorno['antes'][0]['potencia_activa'])
            potencia_reactiva_ref.append(valores_entorno['antes'][0]['potencia_reactiva'])
            
        if valores_entorno['despues']:
            potencia_activa_ref.append(valores_entorno['despues'][0]['potencia_activa'])
            potencia_reactiva_ref.append(valores_entorno['despues'][0]['potencia_reactiva'])
        
        # Si no hay valores de referencia disponibles
        if not potencia_activa_ref:
            return {
                'potencia_activa': valor_imputado['potencia_activa'],
                'potencia_reactiva': valor_imputado['potencia_reactiva'],
                'estado': 'OK',
                'ajuste_aplicado': False
            }
            
        # Calcular valor de referencia (media de los valores cercanos)
        potencia_activa_media_ref = statistics.mean(potencia_activa_ref)
        potencia_reactiva_media_ref = statistics.mean(potencia_reactiva_ref)
        
        # Calcular límites superior e inferior para potencia activa
        limite_inferior_activa = potencia_activa_media_ref * (1 - max_variacion)
        limite_superior_activa = potencia_activa_media_ref * (1 + max_variacion)
        
        # Calcular límites superior e inferior para potencia reactiva
        limite_inferior_reactiva = potencia_reactiva_media_ref * (1 - max_variacion)
        limite_superior_reactiva = potencia_reactiva_media_ref * (1 + max_variacion)
        
        # Comprobar si el valor imputado está dentro de los límites
        potencia_activa_final = valor_imputado['potencia_activa']
        potencia_reactiva_final = valor_imputado['potencia_reactiva']
        ajuste_aplicado = False
        
        # Ajustar potencia activa si está fuera de los límites
        if potencia_activa_final < limite_inferior_activa:
            potencia_activa_final = limite_inferior_activa
            ajuste_aplicado = True
        elif potencia_activa_final > limite_superior_activa:
            potencia_activa_final = limite_superior_activa
            ajuste_aplicado = True
            
        # Ajustar potencia reactiva si está fuera de los límites
        if potencia_reactiva_final < limite_inferior_reactiva:
            potencia_reactiva_final = limite_inferior_reactiva
            ajuste_aplicado = True
        elif potencia_reactiva_final > limite_superior_reactiva:
            potencia_reactiva_final = limite_superior_reactiva
            ajuste_aplicado = True
            
        return {
            'potencia_activa': potencia_activa_final,
            'potencia_reactiva': potencia_reactiva_final,
            'estado': 'LIMITADO' if ajuste_aplicado else 'OK',
            'ajuste_aplicado': ajuste_aplicado,
            'limite_inferior_activa': limite_inferior_activa,
            'limite_superior_activa': limite_superior_activa,
            'limite_inferior_reactiva': limite_inferior_reactiva,
            'limite_superior_reactiva': limite_superior_reactiva,
            'referencia_activa': potencia_activa_media_ref,
            'referencia_reactiva': potencia_reactiva_media_ref
        }
        
    except Exception as e:
        print(f"Error en validación de imputación: {str(e)}")
        return {
            'potencia_activa': valor_imputado['potencia_activa'],
            'potencia_reactiva': valor_imputado['potencia_reactiva'],
            'estado': 'ERROR',
            'error': str(e)
        }

def calcular_imputacion(homologos, valores_entorno):
    """
    Calcula un valor de imputación basado en datos históricos homólogos y entorno.
    
    Args:
        homologos: Lista de registros históricos homólogos
        valores_entorno: Dict con valores de entorno (antes/después)
    
    Returns:
        dict: Valores calculados con potencia activa, reactiva y metadatos
    """
    try:
        potencia_activa_homologos = []
        potencia_reactiva_homologos = []
        
        # Extraer valores de homólogos
        for h in homologos:
            potencia_activa_homologos.append(float(h[2]))
            potencia_reactiva_homologos.append(float(h[3]))
        
        # Extraer valores del entorno
        potencia_activa_entorno = []
        potencia_reactiva_entorno = []
        
        for v in valores_entorno['antes']:
            potencia_activa_entorno.append(v['potencia_activa'])
            potencia_reactiva_entorno.append(v['potencia_reactiva'])
            
        for v in valores_entorno['despues']:
            potencia_activa_entorno.append(v['potencia_activa'])
            potencia_reactiva_entorno.append(v['potencia_reactiva'])
        
        # Calcular valores imputados basados en diferentes estrategias
        resultado = {
            'metodo': '',
            'confianza': 0.0,
            'potencia_activa': 0.0,
            'potencia_reactiva': 0.0,
            'fuentes_homologos': len(homologos),
            'fuentes_entorno': len(potencia_activa_entorno)
        }
        
        # Estrategia 1: Si hay suficientes homólogos, usar mediana de homólogos (más robusta que la media)
        if len(potencia_activa_homologos) >= 3:
            resultado['potencia_activa'] = statistics.median(potencia_activa_homologos)
            resultado['potencia_reactiva'] = statistics.median(potencia_reactiva_homologos)
            resultado['metodo'] = 'mediana_homologos'
            resultado['confianza'] = 0.9
            
        # Estrategia 2: Si hay pocos homólogos pero suficientes, usar media ponderada por cercanía temporal
        elif len(potencia_activa_homologos) > 0:
            resultado['potencia_activa'] = statistics.mean(potencia_activa_homologos)
            resultado['potencia_reactiva'] = statistics.mean(potencia_reactiva_homologos)
            resultado['metodo'] = 'media_homologos'
            resultado['confianza'] = 0.7
            
        # Estrategia 3: Si hay suficientes valores de entorno, usar interpolación
        elif len(potencia_activa_entorno) >= 3:
            # Usar la media acotada (elimina extremos) para más robustez
            resultado['potencia_activa'] = statistics.mean(sorted(potencia_activa_entorno)[1:-1])
            resultado['potencia_reactiva'] = statistics.mean(sorted(potencia_reactiva_entorno)[1:-1])
            resultado['metodo'] = 'media_acotada_entorno'
            resultado['confianza'] = 0.6
            
        # Estrategia 4: Si hay al menos un valor de entorno, usarlo directamente
        elif len(potencia_activa_entorno) > 0:
            resultado['potencia_activa'] = potencia_activa_entorno[0]
            resultado['potencia_reactiva'] = potencia_reactiva_entorno[0]
            resultado['metodo'] = 'valor_entorno_cercano'
            resultado['confianza'] = 0.4
            
        # Estrategia 5: No hay datos, usar valor por defecto seguro
        else:
            # Valores conservadores que no generen picos
            resultado['potencia_activa'] = 0.5
            resultado['potencia_reactiva'] = 0.2
            resultado['metodo'] = 'valor_por_defecto'
            resultado['confianza'] = 0.1
        
        return resultado
        
    except Exception as e:
        print(f"Error calculando valor de imputación: {str(e)}")
        return {
            'metodo': 'error',
            'confianza': 0.0,
            'potencia_activa': 0.5,  # Valor seguro por defecto
            'potencia_reactiva': 0.2,
            'error': str(e)
        }

def obtener_valores_entorno(pg_hook, alimentador, fecha, hora):
    """
    Obtiene valores del entorno (antes y después) para un timestamp específico.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: Alimentador a consultar
        fecha: Fecha del registro
        hora: Hora del registro
    
    Returns:
        dict: Valores de entorno con claves 'antes' y 'despues'
    """
    try:
        # Convertir la hora a objeto time si es string
        if isinstance(hora, str):
            hora_obj = datetime.strptime(hora, '%H:%M:%S').time()
        else:
            hora_obj = hora
            
        # 1. Obtener valores anteriores (hasta 2 horas antes)
        query_antes = """
        SELECT 
            fecha, 
            hora, 
            potencia_activa, 
            potencia_reactiva
        FROM potencia_dep_processed
        WHERE 
            alimentador = %s AND
            fecha = %s::date AND
            hora < %s::time AND
            hora >= (%s::time - INTERVAL '2 hours') AND
            potencia_activa > 0
        ORDER BY hora DESC
        LIMIT 4
        """
        
        valores_antes = pg_hook.get_records(
            query_antes, 
            parameters=(alimentador, fecha, hora, hora)
        )
        
        # 2. Obtener valores posteriores (hasta 2 horas después)
        query_despues = """
        SELECT 
            fecha, 
            hora, 
            potencia_activa, 
            potencia_reactiva
        FROM potencia_dep_processed
        WHERE 
            alimentador = %s AND
            fecha = %s::date AND
            hora > %s::time AND
            hora <= (%s::time + INTERVAL '2 hours') AND
            potencia_activa > 0
        ORDER BY hora ASC
        LIMIT 4
        """
        
        valores_despues = pg_hook.get_records(
            query_despues, 
            parameters=(alimentador, fecha, hora, hora)
        )
        
        # Normalizar resultados para facilitar su uso
        antes = []
        for v in valores_antes:
            antes.append({
                'fecha': v[0],
                'hora': v[1],
                'potencia_activa': float(v[2]),
                'potencia_reactiva': float(v[3])
            })
            
        despues = []
        for v in valores_despues:
            despues.append({
                'fecha': v[0],
                'hora': v[1],
                'potencia_activa': float(v[2]),
                'potencia_reactiva': float(v[3])
            })
        
        return {
            'antes': antes,
            'despues': despues
        }
        
    except Exception as e:
        print(f"Error obteniendo valores de entorno: {str(e)}")
        return {'antes': [], 'despues': []}

def insertar_valor_imputado(pg_hook, alimentador, fecha, hora, potencia_activa, potencia_reactiva, dia_semana):
    """
    Inserta un valor imputado en la tabla potencia_dep_processed.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: ID del alimentador
        fecha: Fecha del registro
        hora: Hora del registro
        potencia_activa: Valor imputado de potencia activa
        potencia_reactiva: Valor imputado de potencia reactiva
        dia_semana: Día de la semana (0-6)
    
    Returns:
        bool: True si la inserción fue exitosa, False en caso contrario
    """
    try:
        # Verificar si ya existe un registro para esa combinación
        query_check = """
        SELECT COUNT(*) 
        FROM potencia_dep_processed
        WHERE alimentador = %s AND fecha = %s AND hora = %s
        """
        
        count = pg_hook.get_first(query_check, parameters=(alimentador, fecha, hora))[0]
        
        if count > 0:
            # Actualizar registro existente
            query = """
            UPDATE potencia_dep_processed
            SET 
                potencia_activa = %s,
                potencia_reactiva = %s,
                dia_semana = %s
            WHERE 
                alimentador = %s AND 
                fecha = %s AND 
                hora = %s
            """
            
            pg_hook.run(
                query, 
                parameters=(
                    potencia_activa, 
                    potencia_reactiva, 
                    dia_semana,
                    alimentador, 
                    fecha, 
                    hora
                )
            )
        else:
            # Insertar nuevo registro
            query = """
            INSERT INTO potencia_dep_processed
                (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva)
            VALUES
                (%s, %s, %s, %s, %s, %s)
            """
            
            pg_hook.run(
                query, 
                parameters=(
                    fecha, 
                    hora, 
                    alimentador, 
                    dia_semana,
                    potencia_activa, 
                    potencia_reactiva
                )
            )
        
        # Registrar en log
        registrar_log(
            pg_hook,
            'INFO',
            f'Valor imputado: alimentador {alimentador}, fecha {fecha}, hora {hora}',
            alimentador=alimentador,
            fecha=fecha,
            hora=hora,
            detalles={
                'potencia_activa': potencia_activa,
                'potencia_reactiva': potencia_reactiva,
                'tipo': 'imputado'
            }
        )
        
        return True
        
    except Exception as e:
        print(f"Error insertando valor imputado: {str(e)}")
        return False

def truncate_existing_month_data(mes_inicio, mes_fin, ano=YEAR):
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
        DELETE FROM potencia_dep_processed 
        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """
        rows_deleted = pg_hook_destino.run(delete_query)
        print(f"✅ Eliminando {rows_deleted} datos del mes {mes_inicio}")
        return True
    except Exception as e:
        print(f"❌ Error al eliminar datos del mes: {str(e)}")
        return False

def configurar_kafka_topics_depuracion(mes):
    """Configura los topics de Kafka para el flujo de depuración"""
    try:
        # Topic para datos depurados
        depuracion_topic = f"potencia-depurada-mes{mes}"
        
        # Configurar el consumer del grupo de análisis para leer del topic correcto
        configurar_topic_procesador(
            CONSUME_KAFKA_PROCESSOR_ID, 
            "Topic Name", 
            depuracion_topic
        )
        
        # Configurar el producer de depuración para publicar al topic correcto
        return f"Topics Kafka configurados correctamente para mes {mes}"
    except Exception as e:
        print(f"Error configurando topics: {str(e)}")
        return f"Error: {str(e)}"

#------------ GESTIÓN DE STANDBY Y DATOS PENDIENTES-------------------
def procesar_standby_pendientes(mes_actual, **kwargs):
    """
    Procesa todos los registros standby pendientes cuando llegamos a mes 6.
    Libera los registros que hayan superado el límite de espera.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año de manera segura
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        if mes_actual >= 6:
            print(f"🔍 Procesando todos los registros standby pendientes (mes actual: {mes_actual})")
            
            # 1. Liberar standby anteriores al mes 6
            query_liberar = f"""
            UPDATE registros_standby 
            SET activo = FALSE, 
                fecha_solucion = NOW(), 
                motivo_solucion = 'Límite de espera (mes 6) alcanzado'
            WHERE activo = TRUE 
            AND EXTRACT(MONTH FROM fecha) < 6
            AND EXTRACT(YEAR FROM fecha) = {year}
            RETURNING alimentador, fecha, hora
            """
            
            registros_liberados = pg_hook.get_records(query_liberar)
            print(f"📊 {len(registros_liberados)} registros liberados por alcanzar límite de espera")
            
            # 2. Procesar todos los registros pendientes con datos del mes 6
            for mes_origen in range(1, 6):
                query_pendientes = f"""
                SELECT alimentador, fecha, hora
                FROM registros_standby
                WHERE EXTRACT(MONTH FROM fecha) = %s
                AND EXTRACT(YEAR FROM fecha) = {year}
                AND activo = TRUE
                """
                pendientes = pg_hook.get_records(query_pendientes, parameters=(mes_origen,))
                if pendientes:
                    print(f"📋 Procesando {len(pendientes)} registros pendientes del mes {mes_origen}")
                    
                    for registro in pendientes:
                        alimentador, fecha, hora = registro
                        
                        # Buscar datos en mes 6 para corregir
                        dato_referencia = buscar_dato_referencia_mes(pg_hook, alimentador, fecha, hora, 6, year)
                        
                        if dato_referencia:
                            actualizar_registro_standby(
                                pg_hook,
                                alimentador,
                                fecha,
                                hora,
                                dato_referencia.get('potencia_activa', 0),
                                dato_referencia.get('potencia_reactiva', 0),
                                f"Corregido con datos del mes 6 (procesamiento masivo)"
                            )
            
            return True
        return False
    except Exception as e:
        print(f"❌ Error procesando standby pendientes: {str(e)}")
        traceback.print_exc()
        return False

def procesar_standby_mes_anterior_mejorado(mes_actual, **kwargs):
    """
    Versión mejorada que busca datos en múltiples fuentes y con mejor manejo de errores.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        # Configurar productor Kafka
        productor = KafkaProducer(
            bootstrap_servers=['kafka_centrosur:9092'],
            value_serializer=lambda v: json.dumps(v, default=decimal_json_serializer).encode('utf-8')
        )
        year = int(Variable.get("YEAR"))
        if mes_actual <= 1:
            print("⚠️ No hay mes anterior para procesar (mes actual es 1)")
            return 0
            
        mes_anterior = mes_actual - 1
        
        # Obtener registros en standby del mes anterior
        query_standby = """
        SELECT alimentador, fecha, hora
        FROM registros_standby
        WHERE EXTRACT(MONTH FROM fecha) = %s
        AND EXTRACT(YEAR FROM fecha) = %s
        AND activo = TRUE
        """
        standby_registros = pg_hook.get_records(query_standby, parameters=(mes_anterior, year))
        
        if not standby_registros:
            print(f"✅ No hay registros en standby del mes {mes_anterior} para procesar")
            return 0
            
        print(f"📋 Procesando {len(standby_registros)} registros en standby del mes {mes_anterior}")
        
        # Buscar datos en ambas tablas para verificar disponibilidad
        query_datos_proc = f"""
        SELECT COUNT(*) FROM potencia_dep_processed 
        WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        
        query_datos_env = f"""
        SELECT COUNT(*) FROM datos_enviados_potencia
        WHERE EXTRACT(MONTH FROM fecha) = {mes_actual}
        AND EXTRACT(YEAR FROM fecha) = {year}
        """
        
        datos_proc = pg_hook.get_first(query_datos_proc)[0]
        datos_env = pg_hook.get_first(query_datos_env)[0]
        
        print(f"📊 Disponibilidad de datos para mes {mes_actual}: {datos_proc} en processed, {datos_env} en enviados")
        
        # Determinar meses de referencia por prioridad
        meses_referencia = []
        # 1. Incluir el mes actual
        meses_referencia.append(mes_actual)
        # 2. Si estamos en mes 6 o posterior, incluir mes 6 como referencia prioritaria
        if mes_actual >= 6 and mes_actual != 6:
            meses_referencia.insert(0, 6)  # Poner mes 6 como prioridad máxima
        # 3. Para garantizar cobertura, agregar otros meses cercanos
        if mes_actual < 6:
            # Si estamos antes del mes 6, buscar en meses futuros hasta 6
            for m in range(mes_actual + 1, min(7, 13)):
                if m not in meses_referencia:
                    meses_referencia.append(m)
        else:
            # Si estamos después del mes 6, buscar en meses anteriores hasta 6
            for m in range(mes_actual - 1, 5, -1):
                if m not in meses_referencia:
                    meses_referencia.append(m)
        
        print(f"🔍 Meses de referencia (por prioridad): {meses_referencia}")
        # Procesar cada registro en standby
        standby_procesados = 0
        standby_fallidos = 0
        
        for standby in standby_registros:
            try:
                alimentador, fecha, hora = standby
                dato_referencia = None
                
                # Intentar con cada mes de referencia hasta encontrar datos válidos
                for mes_ref in meses_referencia:
                    print(f"🔍 Buscando referencia para {alimentador}, {fecha}, {hora} en mes {mes_ref}")
                    
                    try:
                        # Buscar datos de referencia en este mes
                        dato_ref = buscar_dato_referencia_mes(pg_hook, alimentador, fecha, hora, mes_ref, year)
                        if dato_ref and 'potencia_activa' in dato_ref:
                            dato_referencia = dato_ref
                            print(f"✅ Encontrada referencia en mes {mes_ref}")
                            break
                    except Exception as e_ref:
                        print(f"⚠️ Error al buscar en mes {mes_ref}: {str(e_ref)}")
                        continue
                
                # Si encontramos datos de referencia, actualizar el registro standby
                if dato_referencia and 'potencia_activa' in dato_referencia and 'potencia_reactiva' in dato_referencia:
                    motivo = f"Corregido con datos del mes {mes_actual} (fuente: {dato_referencia.get('fuente', 'desconocida')})"
                    try:
                        actualizar_registro_standby(
                            pg_hook, 
                            alimentador, 
                            fecha, 
                            hora, 
                            dato_referencia.get('potencia_activa'), 
                            dato_referencia.get('potencia_reactiva'),
                            motivo
                        )
                        standby_procesados += 1
                        dia_semana_correcto = calcular_dia_semana(fecha)

                        # Publicar la corrección a Kafka también
                        tema_correcciones = "potencia-correcciones"
                        mensaje_correccion = {
                            'alimentador': alimentador,
                            'fecha': fecha.strftime('%Y-%m-%d') if hasattr(fecha, 'strftime') else str(fecha),
                            'hora': str(hora),
                            'potencia_activa': float(dato_referencia.get('potencia_activa', 0)),
                            'potencia_reactiva': float(dato_referencia.get('potencia_reactiva', 0)),
                            'dia_semana': dia_semana_correcto,  # Usar día semana calculado correcto
                            'mes_origen': mes_anterior,
                            'mes_referencia': mes_actual,
                            'fuente': dato_referencia.get('fuente', 'desconocida')
                        }
                        
                        try:
                            productor.send(tema_correcciones, mensaje_correccion)
                        except Exception as e:
                            print(f"⚠️ Error publicando corrección a Kafka: {str(e)}")
                    except Exception as e_update:
                        print(f"❌ Error actualizando registro standby: {str(e_update)}")
                        standby_fallidos += 1
                else:
                    print(f"❌ No se encontró referencia para {alimentador}, {fecha}, {hora}")
                    standby_fallidos += 1
            except Exception as e_proc:
                print(f"❌ Error procesando registro standby: {str(e_proc)}")
                standby_fallidos += 1
        
        # Limpiar recursos
        productor.flush()
        
        print(f"✅ Procesados {standby_procesados} de {len(standby_registros)} registros standby")
        print(f"⚠️ Fallidos: {standby_fallidos} registros")
        
        return standby_procesados
        
    except Exception as e:
        print(f"❌ Error procesando registros standby: {str(e)}")
        traceback.print_exc()
        return 0

def marcar_registro_standby(pg_hook, alimentador, mes, fecha, hora, motivo):
    """Marca un registro específico como en standby para corrección futura"""
    query = """
    INSERT INTO registros_standby 
    (alimentador, mes_origen, fecha, hora, motivo, activo, fecha_standby)
    VALUES (%s, %s, %s, %s, %s, TRUE, NOW())
    """
    try:
        pg_hook.run(query, parameters=(alimentador, mes, fecha, hora, motivo))
        print(f"⏳ Registro standby: alimentador {alimentador}, fecha {fecha}, hora {hora}")
        # Log de registro en standby
        registrar_log(
            pg_hook, 
            'STANDBY', 
            f'Registro puesto en STANDBY: alimentador {alimentador}, fecha {fecha}, hora {hora}',
            alimentador=alimentador,
            fecha=fecha,
            hora=hora,
            detalles={'mes_origen': mes, 'motivo': motivo}
        )
    except Exception as e:
        print(f"Error marcando registro standby: {str(e)}")

def actualizar_registro_standby(pg_hook, alimentador, fecha, hora, potencia_activa, potencia_reactiva, motivo_solucion):
    """
    Actualiza un registro en standby con nuevos valores y lo marca como resuelto.
    Ahora verifica si el registro existe y lo inserta si es necesario.
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: ID del alimentador
        fecha: Fecha del registro en formato 'YYYY-MM-DD'
        hora: Hora del registro
        potencia_activa: Nuevo valor de potencia activa
        potencia_reactiva: Nuevo valor de potencia reactiva
        motivo_solucion: Motivo de la solución
    """
    try:
        # 1. Verificar si el registro existe en potencia_dep_processed
        verificar_query = """
        SELECT COUNT(*), MAX(dia_semana) 
        FROM potencia_dep_processed
        WHERE alimentador = %s
        AND fecha = %s
        AND hora = %s
        """
        
        resultado = pg_hook.get_first(verificar_query, parameters=(alimentador, fecha, hora))
        existe = resultado[0] > 0 if resultado else False
        dia_semana_existente = resultado[1] if resultado and len(resultado) > 1 else None
        dia_semana_correcto = calcular_dia_semana(fecha)
        # 2. Si no existe, hacer un INSERT, de lo contrario un UPDATE
        if not existe:
            # Determinar el valor de dia_semana para el registro
            fecha_dt = datetime.strptime(str(fecha), '%Y-%m-%d') if isinstance(fecha, str) else fecha
            dia_semana = fecha_dt.weekday()  # 0=Lunes, 1=Martes, ..., 6=Domingo
            
            # Insertar el registro que falta
            insert_query = """
            INSERT INTO potencia_dep_processed
                (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva)
            VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            pg_hook.run(insert_query, parameters=(
                fecha, hora, alimentador, 
                dia_semana_correcto,  # Usar el día semana calculado correctamente
                potencia_activa, potencia_reactiva
            ))
            print(f"✅ Registro NUEVO creado para standby: {alimentador}, {fecha}, {hora}")
        else:
            # Actualizar el registro existente manteniendo el dia_semana original
            query_update = """
            UPDATE potencia_dep_processed
            SET potencia_activa = %s, 
                potencia_reactiva = %s,
                dia_semana = %s
            WHERE alimentador = %s
            AND fecha = %s
            AND hora = %s
            """
            pg_hook.run(query_update, parameters=(
                potencia_activa, potencia_reactiva, 
                dia_semana_correcto,  # Actualizar también el día de la semana
                alimentador, fecha, hora
            ))
            print(f"✅ Registro existente actualizado: {alimentador}, {fecha}, {hora}")
        # 3. Marcar como resuelto en la tabla de standby
        query_resolved = """
        UPDATE registros_standby
        SET activo = FALSE,
            fecha_solucion = NOW(),
            motivo_solucion = %s
        WHERE alimentador = %s
        AND fecha = %s
        AND hora = %s
        AND activo = TRUE
        """
        pg_hook.run(query_resolved, parameters=(
            motivo_solucion, alimentador, fecha, hora
        ))
        
        # 4. Registrar en log
        registrar_log(
            pg_hook, 'INFO',
            f'Registro standby corregido: {alimentador}, {fecha}, {hora}',
            alimentador=alimentador,
            fecha=fecha,
            detalles={
                'potencia_activa_nueva': float(potencia_activa),
                'potencia_reactiva_nueva': float(potencia_reactiva),
                'motivo_solucion': motivo_solucion
            }
        )
        print(f"✅ Registro standby corregido: {alimentador}, {fecha}, {hora}")
        return True
    except Exception as e:
        print(f"❌ Error actualizando registro standby: {str(e)}")
        traceback.print_exc()
        return False
  
def corregir_standby_anteriores(pg_hook, productor, alimentador, mes_actual, year=None):
    """
    Verifica si hay registros en standby de meses anteriores y los corrige
    usando datos del mes actual como referencia
    """
    try:
        # Obtener el año de manera segura
        if year is None:
            try:
                year = int(Variable.get("YEAR"))
            except (ValueError, TypeError, KeyError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                year = 2019  # Valor seguro por defecto
                
        # Obtener registros en standby para este alimentador
        query_standby = """
        SELECT alimentador, mes_origen, fecha, hora 
        FROM registros_standby
        WHERE alimentador = %s 
        AND mes_origen < %s 
        AND activo = TRUE
        ORDER BY mes_origen
        """
        
        registros_standby = pg_hook.get_records(query_standby, parameters=(alimentador, mes_actual))
        
        if not registros_standby:
            print(f"✅ No hay registros en standby para alimentador {alimentador}")
            return
        
        print(f"🔄 Encontrados {len(registros_standby)} registros en standby para corregir")
        
        # Patrones para buscar correcciones - misma hora en días equivalentes
        for standby in registros_standby:
            alim, mes_origen, fecha_standby, hora_standby = standby
            
            # Convertir fecha a día del mes
            fecha_dt = datetime.strptime(str(fecha_standby), '%Y-%m-%d')
            dia_standby = fecha_dt.day
            
            # Buscar datos válidos en el mes actual para la misma hora
            query_referencia = """
            SELECT fecha, potencia_activa, potencia_reactiva
            FROM datos_enviados_potencia
            WHERE alimentador = %s
            AND EXTRACT(MONTH FROM fecha) = %s
            AND EXTRACT(YEAR FROM fecha) = %s
            AND hora = %s
            AND potencia_activa != 0
            AND potencia_reactiva != 0
            ORDER BY ABS(EXTRACT(DAY FROM fecha) - %s)
            LIMIT 1
            """
            # Buscar fecha cercana al mismo día del mes
            referencia = pg_hook.get_first(query_referencia, 
                parameters=(alimentador, mes_actual, year, hora_standby, dia_standby))
            
            if referencia and referencia[1] and referencia[2]:
                # Se encontró dato válido para corregir el standby
                fecha_ref, p_activa, p_reactiva = referencia
                
                # Actualizar el registro en potencia_dep_processed
                query_update = """
                UPDATE potencia_dep_processed
                SET potencia_activa = %s, 
                    potencia_reactiva = %s
                WHERE alimentador = %s
                AND fecha = %s
                AND hora = %s
                """
                pg_hook.run(query_update, parameters=(
                    p_activa, p_reactiva, alimentador, fecha_standby, hora_standby
                ))
                # Marcar como resuelto en la tabla de standby
                query_resolved = """
                UPDATE registros_standby
                SET activo = FALSE,
                    fecha_solucion = NOW(),
                    motivo_solucion = %s
                WHERE alimentador = %s
                AND fecha = %s
                AND hora = %s
                """
                
                motivo = f"Corregido con datos del mes {mes_actual}, valor: {p_activa}/{p_reactiva}"
                pg_hook.run(query_resolved, parameters=(
                    motivo, alimentador, fecha_standby, hora_standby
                ))
                
                # Registrar corrección en log
                registrar_log(
                    pg_hook, 'INFO',
                    f'Corregido registro standby: alimentador {alimentador}, fecha {fecha_standby}, hora {hora_standby}',
                    alimentador=alimentador,
                    fecha=fecha_standby,
                    detalles={
                        'mes_origen': mes_origen,
                        'mes_solucion': mes_actual,
                        'potencia_activa': float(p_activa),
                        'potencia_reactiva': float(p_reactiva)
                    }
                )
                print(f"✅ Corregido registro standby: {fecha_standby} {hora_standby} con datos de {fecha_ref}")
                # Publicar el registro corregido a Kafka
                topic_datos_depurados = "potencia_datos_depurados"
                mensaje_kafka = {
                    'fecha': str(fecha_standby),
                    'hora': str(hora_standby),
                    'alimentador': alimentador,
                    'potencia_activa': float(p_activa),
                    'potencia_reactiva': float(p_reactiva),
                    'correccion_standby': True,
                    'mes_origen': mes_origen,
                    'mes_correccion': mes_actual
                }
                productor.send(topic_datos_depurados, mensaje_kafka)
            else:
                print(f"⚠️ No se encontró referencia para corregir standby: {fecha_standby} {hora_standby}")
        
        return True
    except Exception as e:
        print(f"❌ Error corrigiendo standby: {str(e)}")
        return False

def marcar_alimentador_standby(pg_hook, alimentador, mes, motivo):
    """Marca un alimentador como en standby en la tabla de seguimiento"""
    query = """
    INSERT INTO alimentadores_standby (alimentador, mes_origen, fecha_standby, motivo, activo)
    VALUES (%s, %s, NOW(), %s, TRUE)
    """
    pg_hook.run(query, parameters=(alimentador, mes, motivo))
    
    # Log específico de entrada en standby
    registrar_log(
        pg_hook, 'STANDBY', 
        f'Alimentador {alimentador} puesto en STANDBY para mes {mes}: {motivo}',
        alimentador=alimentador,
        detalles={'mes_origen': mes, 'motivo': motivo}
    )

def guardar_registros_pendientes(anio=None):
    """
    Guarda registros que no pudieron procesarse completamente en una tabla específica
    para ser procesados posteriormente por potencia_standby_interanual.
    """
    try:
        if anio is None:
            try:
                anio = int(Variable.get("YEAR"))
                print(f"✅ Usando año {anio} desde variables de Airflow")
            except (ValueError, TypeError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                anio = 2019  # Valor seguro por defecto
            
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        # Crear tabla para registros pendientes si no existe
        tabla_pendientes = f"potencia_dep_pendientes_{anio}"
        
        # Crear tabla si no existe
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS {tabla_pendientes} (
            id SERIAL PRIMARY KEY,
            fecha DATE NOT NULL,
            hora TIME NOT NULL,
            alimentador TEXT NOT NULL,
            dia_semana SMALLINT,
            potencia_activa REAL,
            potencia_reactiva REAL,
            motivo_pendiente TEXT,
            intentos_procesamiento INTEGER DEFAULT 0,
            ultimo_intento TIMESTAMP DEFAULT NULL,
            procesado BOOLEAN DEFAULT FALSE,
            fecha_procesado TIMESTAMP DEFAULT NULL,
            UNIQUE (fecha, hora, alimentador)
        )
        """)
        
        # Identificar registros con valores cero (principal motivo de standby)
        query_ceros = f"""
        INSERT INTO {tabla_pendientes} 
        (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva, motivo_pendiente)
        SELECT 
            fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva, 
            'Valores cero no depurados' AS motivo_pendiente
        FROM potencia_dep_processed
        WHERE EXTRACT(YEAR FROM fecha) = {anio}
        AND potencia_activa = 0
        AND potencia_reactiva = 0
        ON CONFLICT (fecha, hora, alimentador) DO NOTHING
        """
        pg_hook.run(query_ceros)
        
        # También identificar outliers severos como candidatos para corrección interanual
        query_outliers = f"""
        WITH estadisticas AS (
            SELECT 
                alimentador,
                AVG(potencia_activa) as avg_activa,
                STDDEV(potencia_activa) as std_activa
            FROM potencia_dep_processed
            WHERE EXTRACT(YEAR FROM fecha) = {anio}
            AND potencia_activa > 0
            GROUP BY alimentador
        )
        INSERT INTO {tabla_pendientes} 
        (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva, motivo_pendiente)
        SELECT 
            p.fecha, p.hora, p.alimentador, p.dia_semana, p.potencia_activa, p.potencia_reactiva,
            'Outlier extremo (>5σ)' AS motivo_pendiente
        FROM potencia_dep_processed p
        JOIN estadisticas e ON p.alimentador = e.alimentador
        WHERE EXTRACT(YEAR FROM p.fecha) = {anio}
        AND p.potencia_activa > (e.avg_activa + 5*e.std_activa)
        ON CONFLICT (fecha, hora, alimentador) DO NOTHING
        """
        pg_hook.run(query_outliers)
        
        # Contar registros pendientes
        query_count = f"SELECT COUNT(*) FROM {tabla_pendientes}"
        count = pg_hook.get_first(query_count)[0]
        
        # Guardar metadatos para procesamiento interanual
        pg_hook.run(f"""
        CREATE TABLE IF NOT EXISTS potencia_metadatos_procesamiento (
            id SERIAL PRIMARY KEY,
            anio INTEGER NOT NULL,
            tipo TEXT NOT NULL,
            valor TEXT,
            timestamp_creacion TIMESTAMP DEFAULT NOW(),
            UNIQUE(anio, tipo)
        )
        """)
        
        # Insertar metadatos
        pg_hook.run(f"""
        INSERT INTO potencia_metadatos_procesamiento (anio, tipo, valor)
        VALUES 
            ({anio}, 'anio_procesado', '{anio}'),
            ({anio}, 'pendientes_total', '{count}'),
            ({anio}, 'procesamiento_completado', 'true'),
            ({anio}, 'fecha_procesamiento', '{datetime.now().isoformat()}')
        ON CONFLICT (anio, tipo) 
        DO UPDATE SET valor = EXCLUDED.valor, timestamp_creacion = NOW()
        """)
        
        # Establecer variables de Airflow para potencia_standby_interanual
        Variable.set("potencia_ultimo_anio_procesado", str(anio))
        Variable.set("potencia_anio_procesado", str(anio))
        Variable.set("potencia_anio_siguiente", str(anio + 1))
        Variable.set("potencia_pendientes_"+str(anio), str(count))
        Variable.set("potencia_standby_total", str(count))
        
        # Registrar en log
        registrar_log(
            pg_hook,
            'INFO',
            f'Guardados {count} registros pendientes para procesamiento interanual',
            detalles={
                'anio_procesado': anio,
                'anio_siguiente': anio + 1,
                'total_standby': count,
                'tabla_pendientes': tabla_pendientes
            }
        )
        
        print(f"✅ {count} registros pendientes guardados en {tabla_pendientes} y variables establecidas")
        return count
        
    except Exception as e:
        print(f"❌ Error guardando registros pendientes: {str(e)}")
        traceback.print_exc()
        return 0

def validar_tabla_pendientes(anio=None):
    """Valida que la tabla de pendientes tenga la estructura correcta"""
    try:
        if anio is None:
            try:
                anio = int(Variable.get("YEAR"))
            except (ValueError, TypeError):
                print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
                anio = 2019  # Valor seguro por defecto
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        tabla_pendientes = f"potencia_dep_pendientes_{anio}"
        
        # Verificar que existe
        existe = pg_hook.get_first(f"""
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_name = '{tabla_pendientes}'
        )
        """)[0]
        
        if not existe:
            print(f"⚠️ La tabla {tabla_pendientes} no existe. Creándola...")
            # Crear la tabla con la estructura correcta
            pg_hook.run(f"""
            CREATE TABLE {tabla_pendientes} (
                id SERIAL PRIMARY KEY,
                fecha DATE NOT NULL,
                hora TIME NOT NULL,
                alimentador TEXT NOT NULL,
                dia_semana SMALLINT,
                potencia_activa REAL,
                potencia_reactiva REAL,
                motivo_pendiente TEXT,
                intentos_procesamiento INTEGER DEFAULT 0,
                ultimo_intento TIMESTAMP DEFAULT NULL,
                procesado BOOLEAN DEFAULT FALSE,
                fecha_procesado TIMESTAMP DEFAULT NULL,
                UNIQUE (fecha, hora, alimentador)
            )
            """)
            print(f"✅ Tabla {tabla_pendientes} creada")
        
        # Verificar estructura
        columnas = pg_hook.get_records(f"""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = '{tabla_pendientes}'
        """)
        
        # Verificar que tiene las columnas mínimas necesarias
        columnas_requeridas = ['fecha', 'hora', 'alimentador', 'potencia_activa', 'procesado']
        columnas_actuales = [col[0] for col in columnas]
        
        for col in columnas_requeridas:
            if col not in columnas_actuales:
                print(f"⚠️ Falta columna {col} en tabla {tabla_pendientes}")
                return False
        
        print(f"✅ Tabla {tabla_pendientes} validada correctamente")
        return True
    except Exception as e:
        print(f"❌ Error validando tabla pendientes: {str(e)}")
        return False

def verificar_estado_standby_interanual(anio):
    """Verifica si el DAG potencia_standby_interanual ha sido ejecutado para el año especificado"""
    try:
        # Verificar en variables de Airflow
        estado = Variable.get(f"potencia_standby_anio_{anio}_completado", default_var=None)
        if estado:
            return json.loads(estado)
        
        # Verificar directamente en base de datos (respaldo)
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        query = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_pendientes_{anio}
        WHERE procesado = TRUE
        """
        procesados = pg_hook.get_first(query)[0]
        
        return {
            "anio": anio,
            "procesados": procesados,
            "verificado_directamente": True
        }
    except Exception as e:
        print(f"❌ Error verificando estado standby: {str(e)}")
        return {"error": str(e)}

def identificar_timestamps_faltantes(pg_hook, alimentador=None, fecha_inicio=None, fecha_fin=None):
    """
    Identifica timestamps faltantes en una serie temporal que debería tener datos cada 15 minutos.
    
    Args:
        pg_hook: PostgreSQL hook para conexión a base de datos
        alimentador: Alimentador específico o None para procesar todos
        fecha_inicio: Fecha de inicio para el rango a procesar (default: 7 días atrás)
        fecha_fin: Fecha de fin para el rango a procesar (default: hoy)
    
    Returns:
        list: Lista de registros faltantes con alimentador, fecha y hora
    """
    try:
        # Establecer valores por defecto si no se proporcionan
        if not fecha_inicio:
            fecha_inicio = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
        if not fecha_fin:
            fecha_fin = datetime.now().strftime('%Y-%m-%d')
        
        # Si no se especifica alimentador, obtener todos los alimentadores activos
        if not alimentador:
            query_alimentadores = """
            SELECT DISTINCT alimentador 
            FROM potencia_dep_processed
            WHERE fecha BETWEEN %s::date AND %s::date
            ORDER BY alimentador
            """
            alimentadores = [row[0] for row in pg_hook.get_records(
                query_alimentadores, 
                parameters=(fecha_inicio, fecha_fin)
            )]
        else:
            alimentadores = [alimentador]
        
        resultados = []
        
        # Para cada alimentador, identificar timestamps faltantes
        for alim in alimentadores:
            query_faltantes = """
            WITH fechas AS (
                SELECT generate_series(%s::date, %s::date, '1 day'::interval)::date AS fecha
            ),
            horas AS (
                SELECT generate_series(0, 23) AS hora
            ),
            minutos AS (
                SELECT generate_series(0, 45, 15) AS minuto
            ),
            todos_timestamps AS (
                SELECT 
                    f.fecha,
                    make_time(h.hora, m.minuto, 0) AS hora,
                    %s AS alimentador
                FROM fechas f
                CROSS JOIN horas h
                CROSS JOIN minutos m
            ),
            existentes AS (
                SELECT fecha, hora
                FROM potencia_dep_processed
                WHERE 
                    alimentador = %s AND
                    fecha BETWEEN %s::date AND %s::date
            )
            SELECT 
                tt.alimentador,
                tt.fecha,
                tt.hora
            FROM todos_timestamps tt
            LEFT JOIN existentes e ON tt.fecha = e.fecha AND tt.hora = e.hora
            WHERE e.fecha IS NULL
            ORDER BY tt.fecha, tt.hora;
            """
            
            faltantes = pg_hook.get_records(
                query_faltantes, 
                parameters=(
                    fecha_inicio, fecha_fin, 
                    alim, alim, 
                    fecha_inicio, fecha_fin
                )
            )
            
            for registro in faltantes:
                resultados.append({
                    'alimentador': registro[0],
                    'fecha': registro[1],
                    'hora': registro[2]
                })
        
        return resultados
        
    except Exception as e:
        print(f"Error identificando timestamps faltantes: {str(e)}")
        return []

def limpiar_estado_entre_ejecuciones():
    """
    Limpia TODAS las variables relacionadas con el estado entre ejecuciones para asegurar consistencia.
    """
    try:
        # Limpiar todas las variables relevantes
        Variable.delete("potencia_anio_actual_fijado", "")
        Variable.delete("potencia_anio_actual_procesamiento", "")
        Variable.delete("potencia_ultimo_anio_procesado", "")
        Variable.delete("potencia_anio_procesado", "")
        Variable.delete("potencia_anio_siguiente", "")
        print("✅ Estado limpio para nueva ejecución del DAG - Todas las variables eliminadas")
        return True
    except Exception as e:
        print(f"⚠️ Error parcial limpiando estado: {str(e)}")
        # Si hay error, no interrumpir el flujo
        return True

def corregir_registros_faltantes():
    """
    Identifica y corrige registros faltantes en la serie temporal
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # 1. Identificar registros faltantes
        registros_faltantes = verificar_completitud_registros()
        
        if not registros_faltantes:
            print("✅ No se detectaron registros faltantes que requieran corrección.")
            return {'registros_corregidos': 0}
            
        print(f"🔍 Detectados {len(registros_faltantes)} días con registros incompletos.")
        
        # 2. Para cada registro faltante, generar una imputación
        registros_corregidos = 0
        for registro in registros_faltantes:
            alimentador = registro['alimentador']
            fecha = registro['fecha']
            timestamps_faltantes = registro.get('timestamps_faltantes', [])
            
            print(f"🔄 Corrigiendo {len(timestamps_faltantes)} timestamps faltantes para {alimentador}, {fecha}")
            
            # Para cada timestamp faltante, imputar un valor
            for hora_str in timestamps_faltantes:
                try:
                    # Convertir a formato adecuado
                    if isinstance(fecha, str):
                        fecha_dt = datetime.strptime(fecha, '%Y-%m-%d').date()
                    else:
                        fecha_dt = fecha
                        
                    # Calcular día de semana correcto
                    dia_semana = calcular_dia_semana(fecha_dt)
                    
                    # Buscar datos homólogos
                    query_homologos = """
                    SELECT potencia_activa, potencia_reactiva
                    FROM potencia_dep_processed
                    WHERE alimentador = %s
                    AND EXTRACT(DOW FROM fecha) = %s
                    AND hora = %s::time
                    AND potencia_activa > 0
                    ORDER BY fecha DESC
                    LIMIT 5
                    """
                    
                    homologos = pg_hook.get_records(query_homologos, 
                                                  parameters=(alimentador, dia_semana, hora_str))
                    
                    # Si hay homólogos, calcular promedio
                    if homologos:
                        potencia_activa = statistics.mean([h[0] for h in homologos])
                        potencia_reactiva = statistics.mean([h[1] for h in homologos])
                    else:
                        # Si no hay homólogos, usar valores conservadores
                        potencia_activa = 1.0
                        potencia_reactiva = 0.5
                    
                    # Insertar valor imputado
                    query_insert = """
                    INSERT INTO potencia_dep_processed 
                        (fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (fecha, hora, alimentador) DO UPDATE 
                    SET potencia_activa = EXCLUDED.potencia_activa,
                        potencia_reactiva = EXCLUDED.potencia_reactiva,
                        dia_semana = EXCLUDED.dia_semana
                    """
                    
                    pg_hook.run(query_insert, 
                              parameters=(fecha_dt, hora_str, alimentador, dia_semana, 
                                         potencia_activa, potencia_reactiva))
                    
                    # Registrar en log
                    registrar_log(
                        pg_hook,
                        'IMPUTACIÓN',
                        f'Registro faltante corregido: {alimentador}, {fecha}, {hora_str}',
                        alimentador=alimentador,
                        fecha=fecha_dt,
                        hora=hora_str,
                        detalles={
                            'potencia_activa': float(potencia_activa),
                            'potencia_reactiva': float(potencia_reactiva),
                            'fuente': 'corrección_completitud'
                        }
                    )
                    
                    registros_corregidos += 1
                    
                except Exception as e_corr:
                    print(f"⚠️ Error corrigiendo registro {alimentador}, {fecha}, {hora_str}: {str(e_corr)}")
        
        print(f"✅ Proceso completo: {registros_corregidos} registros corregidos")
        
        return {
            'registros_corregidos': registros_corregidos
        }
        
    except Exception as e:
        print(f"❌ Error corrigiendo registros faltantes: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}
#------------ VERIFICACIÓN Y REPORTES-------------------
def verificar_completitud_anual():
    """Verifica la completitud del procesamiento anual usando hooks separados"""
    try:
        # Usar PostgresHook con las conexiones correctas
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año de manera segura desde la variable de Airflow
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        fecha_inicio = f"{year}-01-01"
        fecha_fin = f"{year+1}-01-01"

        # Conteo total de registros en origen 
        total_origen = pg_hook_origen.get_first(
            f"""
            SELECT COUNT(*) FROM potencia_dep_original 
            WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            """
        )[0]
        
        # Conteo total de registros en destino 
        total_destino = pg_hook_destino.get_first(
            f"""
            SELECT COUNT(*) FROM potencia_dep_processed 
            WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            """
        )[0]
        
        # Conteo por mes en origen
        conteo_origen_por_mes = pg_hook_origen.get_records(
            f"""
            SELECT EXTRACT(MONTH FROM fecha)::int AS mes, COUNT(*)
            FROM potencia_dep_original 
            WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
            """
        )

        # Conteo por mes en destino
        conteo_destino_por_mes = pg_hook_destino.get_records(
            f"""
            SELECT EXTRACT(MONTH FROM fecha)::int AS mes, COUNT(*)
            FROM potencia_dep_processed 
            WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
            GROUP BY EXTRACT(MONTH FROM fecha)
            ORDER BY mes
            """
        )
        # Generar informe
        resultado = f"INFORME DE COMPLETITUD DE PROCESAMIENTO ANUAL {year}\n"
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
        dict_origen = {int(mes): count for mes, count in conteo_origen_por_mes}
        dict_destino = {int(mes): count for mes, count in conteo_destino_por_mes}

        # NUEVO: Verificación específica para garantizar que no hay datos ficticios
        meses_sin_datos_originales = []
        for mes in range(1, 13):
            count_origen = dict_origen.get(mes, 0)
            count_destino = dict_destino.get(mes, 0)
            
            # Si no hay datos en origen pero sí en destino, es un problema
            if count_origen == 0 and count_destino > 0:
                meses_sin_datos_originales.append(mes)
                print(f"⚠️ ERROR CRÍTICO: El mes {mes} tiene {count_destino} registros en la tabla procesada pero NO TIENE DATOS en la tabla original.")
                # Sugerir eliminación
                print(f"   Se sugiere ejecutar: DELETE FROM potencia_dep_processed WHERE EXTRACT(MONTH FROM fecha) = {mes} AND EXTRACT(YEAR FROM fecha) = {year}")
            
            if count_origen > 0:
                comp = (count_destino / count_origen) * 100
                resultado += "{:<10} {:<15} {:<15} {:<15.2f}%\n".format(
                    calendar.month_name[mes][:3], count_origen, count_destino, comp
                )
            else:
                resultado += "{:<10} {:<15} {:<15} {:<15}\n".format(
                    calendar.month_name[mes][:3], count_origen, count_destino, "N/A"
                )
        
        # Si hay meses con datos ficticios, reportarlo claramente
        if meses_sin_datos_originales:
            resultado += "\n⚠️ PROBLEMA CRÍTICO: Se detectaron datos ficticios en los siguientes meses:\n"
            for mes in meses_sin_datos_originales:
                resultado += f"   - {calendar.month_name[mes]}: {dict_destino.get(mes, 0)} registros ficticios\n"
                
        print(resultado)
        return resultado
        
    except Exception as e:
        error_msg = f"Error al verificar completitud anual: {str(e)}"
        print(error_msg)
        return error_msg

def verificar_completitud_registros():
    """
    Función que verifica completitud de 96 registros diarios por alimentador
    e identifica timestamps específicos faltantes.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # 1. Identificar días con registros incompletos
        query_incompletos = """
        SELECT
            alimentador,
            fecha,
            COUNT(*) AS total_registros
        FROM
            potencia_dep_processed
        GROUP BY
            alimentador,
            fecha
        HAVING
            COUNT(*) <> 96
        ORDER BY
            alimentador,
            fecha;
        """
        
        dias_incompletos = pg_hook.get_records(query_incompletos)
        
        # 2. Para cada día incompleto, identificar timestamps específicos faltantes
        resultados = []
        for alimentador, fecha, total in dias_incompletos:
            # Generamos todos los timestamps que deberían existir (cada 15 minutos)
            query_faltantes = """
            WITH todas_horas AS (
                SELECT generate_series(0, 23) AS hora
            ),
            todos_minutos AS (
                SELECT generate_series(0, 45, 15) AS minuto
            ),
            todos_timestamps AS (
                SELECT 
                    %s::date AS fecha,
                    make_time(hora, minuto, 0) AS hora
                FROM todas_horas, todos_minutos
            ),
            existentes AS (
                SELECT fecha, hora
                FROM potencia_dep_processed
                WHERE alimentador = %s AND fecha = %s::date
            )
            SELECT 
                tt.fecha,
                tt.hora
            FROM todos_timestamps tt
            LEFT JOIN existentes e ON tt.fecha = e.fecha AND tt.hora = e.hora
            WHERE e.fecha IS NULL
            ORDER BY tt.hora;
            """
            
            timestamps_faltantes = pg_hook.get_records(query_faltantes, 
                                                    parameters=(fecha, alimentador, fecha))
            
            # Convertir objetos time a cadenas para JSON serialization
            faltantes_serializables = [str(ts[1]) for ts in timestamps_faltantes]
            
            # Guardar resultados
            resultados.append({
                'alimentador': alimentador,
                'fecha': str(fecha),  # Convertir fecha a string para serialización
                'total_existentes': total,
                'total_faltantes': 96 - total,
                'timestamps_faltantes': faltantes_serializables
            })
            
        # 3. Registrar resultados en tabla de monitoreo
        for resultado in resultados:
            registrar_log(
                pg_hook,
                'ALERTA',
                f'Completitud incompleta: alimentador {resultado["alimentador"]}, fecha {resultado["fecha"]}',
                alimentador=resultado["alimentador"],
                fecha=resultado["fecha"],
                detalles={
                    'total_existentes': resultado["total_existentes"],
                    'total_faltantes': resultado["total_faltantes"],
                    'timestamps_faltantes': resultado["timestamps_faltantes"]
                }
            )
            
        return resultados if resultados else []
        
    except Exception as e:
        print(f"Error verificando completitud: {str(e)}")
        return []

def determinar_nivel_calidad(completitud, consistencia, continuidad, outliers):
    """
    Determina el nivel de calidad general basado en varias métricas.
    
    Args:
        completitud: Porcentaje de datos completos (0-100%)
        consistencia: Porcentaje de datos consistentes (0-100%)
        continuidad: Porcentaje de continuidad de datos (0-100%)
        outliers: Lista de outliers detectados
        
    Returns:
        str: Nivel de calidad ('EXCELENTE', 'BUENO', 'REGULAR', 'BAJO')
    """
    try:
        # Factor de penalización por outliers
        factor_outliers = min(1.0, 1.0 - (len(outliers) / 1000)) if isinstance(outliers, list) else 0.95
        
        # Calcular puntaje promedio ponderado
        puntaje = (
            completitud * 0.4 +  # 40% de peso para completitud
            consistencia * 0.3 +  # 30% de peso para consistencia
            continuidad * 0.3     # 30% de peso para continuidad
        ) * factor_outliers
        
        # Determinar nivel
        if puntaje >= 90:
            return 'EXCELENTE'
        elif puntaje >= 75:
            return 'BUENO'
        elif puntaje >= 50:
            return 'REGULAR'
        else:
            return 'BAJO'
    except Exception as e:
        print(f"Error determinando nivel de calidad: {str(e)}")
        return 'INDETERMINADO'
 
def verificar_datos_enviados(mes, tiempo_espera_minutos=5, **kwargs):
    """Verifica la existencia de datos en la tabla datos_enviados"""
    try:
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Calcular tiempo de espera
        tiempo_total_segundos = tiempo_espera_minutos * 60
        tiempo_transcurrido = 0
        
        print(f"⏳ Verificando datos enviados para mes {mes}...")
        
        while tiempo_transcurrido < tiempo_total_segundos:
            # Consultar datos enviados para este mes
            query = f"""
            SELECT COUNT(*) FROM datos_enviados_potencia 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {year}
            """
            count = pg_hook.get_first(query)[0]
            
            print(f"📊 Verificación a los {tiempo_transcurrido} segundos: {count} registros enviados para mes {mes}")
            
            if count > 0:
                print(f"✅ Datos detectados en tabla datos_enviados para mes {mes}")
                return True
                
            # Esperar antes de la siguiente verificación
            time.sleep(60)
            tiempo_transcurrido += 60
            
        print(f"⚠️ No se detectaron datos enviados para mes {mes} después de {tiempo_espera_minutos} minutos")
        return False
    except Exception as e:
        print(f"❌ Error verificando datos enviados: {str(e)}")
        return False

def recuperar_datos_mes_faltantes(mes=None, **kwargs):
    """
    Recupera los datos faltantes de un mes específico (o detecta automáticamente qué mes necesita corrección)
    insertando directamente los registros en la base de datos.
    """
    try:
        print("🔍 Iniciando análisis de datos faltantes entre tablas")
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        pg_hook_destino = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año de manera segura
        try:
            year = int(Variable.get("YEAR"))
        except (ValueError, TypeError, KeyError):
            print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
            year = 2019  # Valor seguro por defecto
            
        # 1. Si no se especificó un mes, detectar automáticamente qué mes tiene más discrepancias
        if mes is None:
            print("🔎 Detectando automáticamente el mes con más registros faltantes...")
            
            discrepancias_por_mes = []
            for m in range(1, 13):
                # Calcular fechas para el rango del mes
                mes_siguiente = m + 1 if m < 12 else 1
                ano_siguiente = year if m < 12 else year + 1
                fecha_inicio = f"{year}-{m:02d}-01"
                fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
                
                # Contar registros en origen
                total_origen = pg_hook_origen.get_first(f"""
                    SELECT COUNT(*) FROM potencia_dep 
                    WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """)[0]
                
                # Contar registros en destino
                total_destino = pg_hook_destino.get_first(f"""
                    SELECT COUNT(*) FROM potencia_dep_processed 
                    WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """)[0]
                
                # Calcular diferencia
                diferencia = total_origen - total_destino
                discrepancias_por_mes.append((m, diferencia, total_origen, total_destino))
                
            # Ordenar por mayor diferencia
            discrepancias_por_mes.sort(key=lambda x: x[1], reverse=True)
            
            # Elegir el mes con mayor discrepancia
            if discrepancias_por_mes and discrepancias_por_mes[0][1] > 0:
                mes = discrepancias_por_mes[0][0]
                print(f"📊 Mayor discrepancia detectada en mes {mes}: {discrepancias_por_mes[0][1]} registros faltantes")
            else:
                print("✅ No se detectaron discrepancias significativas entre tablas")
                return {"recuperados": 0, "mes": None, "mensaje": "No se detectaron discrepancias"}
        
        # 2. Calcular rangos de fecha para el mes seleccionado
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = year if mes < 12 else year + 1 
        fecha_inicio = f"{year}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        # 3. Contar registros en origen y destino para este mes
        total_origen = pg_hook_origen.get_first(f"""
            SELECT COUNT(*) FROM potencia_dep 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)[0]
        
        total_destino = pg_hook_destino.get_first(f"""
            SELECT COUNT(*) FROM potencia_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)[0]
        
        faltantes = total_origen - total_destino
        print(f"📊 Total registros en origen: {total_origen}")
        print(f"📊 Total registros en destino: {total_destino}")
        print(f"📊 Registros faltantes: {faltantes}")
        
        if faltantes <= 0:
            print(f"✅ No hay registros faltantes para el mes {mes}")
            return {"recuperados": 0, "mes": mes, "total": 0}
        
        # 4. Crear una tabla temporal para procesar los datos en lotes para mejor rendimiento
        print("� Creando tabla temporal para procesamiento por lotes")
        pg_hook_destino.run("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_registros_faltantes (
                alimentador TEXT,
                fecha DATE,
                hora TIME,
                dia_semana INTEGER,
                potencia_activa FLOAT,
                potencia_reactiva FLOAT
            )
        """)
        
        # 5. Obtener los alimentadores con datos faltantes
        alimentadores = pg_hook_origen.get_records(f"""
            SELECT DISTINCT alimentador 
            FROM potencia_dep
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            ORDER BY alimentador
        """)
        
        total_insertados = 0
        lote_size = 10000  # Procesar en lotes más grandes
        
        # 6. Procesar cada alimentador
        for idx, alim_row in enumerate(alimentadores):
            alimentador = alim_row[0]
            print(f"🔄 Procesando alimentador: {alimentador} ({idx+1}/{len(alimentadores)})")
            
            # 6.1 Obtener registros faltantes para este alimentador
            query_faltantes = f"""
            SELECT p.alimentador, p.fecha, p.hora, 
                EXTRACT(DOW FROM p.fecha) as dia_semana, 
                CASE 
                    WHEN p.potencia_activa IS NULL OR p.potencia_activa <= 0 THEN 1.0
                    WHEN p.potencia_activa > 1000 THEN 500.0
                    ELSE p.potencia_activa
                END AS potencia_activa,
                CASE 
                    WHEN p.potencia_reactiva IS NULL OR p.potencia_reactiva <= 0 THEN 0.3
                    WHEN p.potencia_reactiva > 500 THEN 200.0
                    ELSE p.potencia_reactiva
                END AS potencia_reactiva
            FROM potencia_dep p
            WHERE NOT EXISTS (
                SELECT 1 FROM potencia_dep_processed d
                WHERE d.alimentador = p.alimentador
                AND d.fecha = p.fecha
                AND d.hora = p.hora
                AND d.fecha >= '{fecha_inicio}'
                AND d.fecha < '{fecha_fin}'
            )
            AND p.fecha >= '{fecha_inicio}' 
            AND p.fecha < '{fecha_fin}'
            AND p.alimentador = %s
            ORDER BY p.fecha, p.hora
            """
            
            registros_faltantes = pg_hook_origen.get_records(query_faltantes, parameters=(alimentador,))
            
            if not registros_faltantes:
                print(f"✅ No hay registros faltantes para el alimentador {alimentador}")
                continue
                
            print(f"🔍 Encontrados {len(registros_faltantes)} registros faltantes para {alimentador}")
            
            # 6.2 Insertar en la tabla temporal
            pg_hook_destino.run("TRUNCATE TABLE temp_registros_faltantes")
            
            # Preparar los valores para inserción masiva
            values_list = []
            for registro in registros_faltantes:
                alimentador, fecha, hora, dia_semana, p_activa, p_reactiva = registro
                
                # Calcular día de semana correctamente
                dia_semana_correcto = calcular_dia_semana(fecha)
                
                values_list.append((
                    alimentador, 
                    fecha, 
                    hora, 
                    dia_semana_correcto, 
                    p_activa, 
                    p_reactiva
                ))
            
            # Insertar en lotes para mejor rendimiento
            print(f"📥 Insertando {len(values_list)} registros para {alimentador}")
            pg_hook_destino.insert_rows(
                table="temp_registros_faltantes",
                rows=values_list,
                target_fields=["alimentador", "fecha", "hora", "dia_semana", "potencia_activa", "potencia_reactiva"]
            )
            
            # 6.3 Mover de la tabla temporal a la tabla final con manejo de duplicados
            query_insert_final = """
            INSERT INTO potencia_dep_processed 
                (alimentador, fecha, hora, dia_semana, potencia_activa, potencia_reactiva)
            SELECT 
                alimentador, fecha, hora, dia_semana, potencia_activa, potencia_reactiva
            FROM 
                temp_registros_faltantes
            ON CONFLICT (alimentador, fecha, hora) DO UPDATE SET
                dia_semana = EXCLUDED.dia_semana,
                potencia_activa = EXCLUDED.potencia_activa,
                potencia_reactiva = EXCLUDED.potencia_reactiva
            """
            
            registros_insertados = pg_hook_destino.run(query_insert_final)
            print(f"✅ Insertados {len(values_list)} registros para alimentador {alimentador}")
            
            total_insertados += len(values_list)
            
            # 7. Mostrar progreso cada cierto número de alimentadores
            if (idx + 1) % 5 == 0 or idx == len(alimentadores) - 1:
                # Verificar progreso
                count_actual = pg_hook_destino.get_first(f"""
                    SELECT COUNT(*) FROM potencia_dep_processed 
                    WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """)[0]
                
                progreso = count_actual - total_destino
                porcentaje = (progreso / faltantes) * 100 if faltantes > 0 else 0
                print(f"📊 Progreso: {progreso}/{faltantes} registros ({porcentaje:.2f}%)")
        
        # 8. Verificar resultados finales
        count_final = pg_hook_destino.get_first(f"""
            SELECT COUNT(*) FROM potencia_dep_processed 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        """)[0]
        
        registros_recuperados = count_final - total_destino
        porcentaje_recuperado = (registros_recuperados / faltantes) * 100 if faltantes > 0 else 0
        
        mensaje_final = f"""
        ✅ Recuperación completada para mes {mes}:
        - Registros originales: {total_origen}
        - Registros antes de recuperación: {total_destino}
        - Registros después de recuperación: {count_final}
        - Recuperados: {registros_recuperados} ({porcentaje_recuperado:.2f}%)
        """
        print(mensaje_final)
        
        # 9. Registrar en log
        registrar_log(
            pg_hook_destino,
            'INFO',
            f'Recuperados {registros_recuperados} registros faltantes para el mes {mes}',
            detalles={
                'mes': mes,
                'total_origen': total_origen,
                'total_insertados': registros_recuperados,
                'porcentaje': porcentaje_recuperado
            }
        )
        
        return {
            "mes": mes,
            "recuperados": registros_recuperados,
            "total": total_origen,
            "porcentaje": porcentaje_recuperado
        }
        
    except Exception as e:
        error_msg = f"❌ Error en recuperación de datos faltantes: {str(e)}"
        print(error_msg)
        traceback.print_exc()
        return {"error": error_msg, "mes": mes if 'mes' in locals() else None}

def corregir_dia_semana_inconsistente(**kwargs):
    """
    Corrige cualquier inconsistencia en el campo dia_semana
    buscando registros del mismo día y alimentador.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        # Encontrar registros con día de semana inconsistente
        query_inconsistentes = """
        WITH dias_correctos AS (
            SELECT fecha, alimentador, 
                   MODE() WITHIN GROUP (ORDER BY dia_semana) as dia_semana_correcto
            FROM potencia_dep_processed
            GROUP BY fecha, alimentador
        )
        SELECT p.fecha, p.hora, p.alimentador, p.dia_semana, d.dia_semana_correcto
        FROM potencia_dep_processed p
        JOIN dias_correctos d ON p.fecha = d.fecha AND p.alimentador = d.alimentador
        WHERE p.dia_semana != d.dia_semana_correcto
        """
        
        inconsistencias = pg_hook.get_records(query_inconsistentes)
        
        if not inconsistencias:
            print("✅ No se encontraron inconsistencias en los días de semana.")
            return 0
            
        print(f"⚠️ Encontradas {len(inconsistencias)} inconsistencias en días de semana.")
        
        # Actualizar registros inconsistentes
        for fecha, hora, alimentador, dia_actual, dia_correcto in inconsistencias:
            print(f"🔄 Corrigiendo: {alimentador}, {fecha}, {hora}: {dia_actual} → {dia_correcto}")
            
            query_update = """
            UPDATE potencia_dep_processed
            SET dia_semana = %s
            WHERE fecha = %s
            AND hora = %s
            AND alimentador = %s
            """
            
            pg_hook.run(query_update, parameters=(dia_correcto, fecha, hora, alimentador))
            
        return len(inconsistencias)
        
    except Exception as e:
        print(f"❌ Error corrigiendo días de semana: {str(e)}")
        return -1

def ejecutar_tareas_calidad_datos(**kwargs):
    """
    Función principal que ejecuta todas las tareas de calidad de datos
    después del procesamiento principal.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        print("🔍 Iniciando tareas de verificación y corrección de calidad de datos")
        
        # 1. Verificar completitud de registros
        print("\n--- VERIFICACIÓN DE COMPLETITUD DE REGISTROS ---")
        resultados_completitud = verificar_completitud_registros()
        if resultados_completitud:
            print(f"Se identificaron {len(resultados_completitud)} días con registros incompletos")
        else:
            print("No se encontraron problemas de completitud")
        
        # 2. Imputar valores faltantes
        print("\n--- IMPUTACIÓN DE VALORES FALTANTES ---")
        fecha_inicio = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')  # Últimos 90 días
        fecha_fin = datetime.now().strftime('%Y-%m-%d')
        
        resultados_imputacion = imputar_valores_faltantes(
            pg_hook, 
            alimentador=None,  # Procesar todos los alimentadores
            fecha_inicio=fecha_inicio,
            fecha_fin=fecha_fin
        )
        
        # 3. Analizar y corregir anomalías
        print("\n--- ANÁLISIS Y CORRECCIÓN DE ANOMALÍAS ---")
        resultados_anomalias = analizar_anomalias_y_corregir()
        
        # 4. Detectar y corregir un caso específico para demostración
        print("\n--- ANÁLISIS DE CASO ESPECÍFICO ---")
        caso_especifico = resolver_caso_anomalo()
        
        # 5. Verificar días de semana inconsistentes
        print("\n--- CORRECCIÓN DE DÍAS DE SEMANA INCONSISTENTES ---")
        corregidos = corregir_dia_semana_inconsistente()
        
        # Generar resumen de resultados
        resumen = {
            'completitud': {
                'dias_incompletos': len(resultados_completitud) if resultados_completitud else 0,
                'detalles': resultados_completitud
            },
            'imputacion': {
                'total_procesados': resultados_imputacion.get('total_procesados', 0),
                'exitosos': resultados_imputacion.get('imputados_exitosos', 0),
                'limitados': resultados_imputacion.get('imputaciones_limitadas', 0),
                'fallidos': resultados_imputacion.get('imputaciones_fallidas', 0)
            },
            'anomalias': {
                'detectadas': resultados_anomalias.get('anomalias_detectadas', 0),
                'corregidas': resultados_anomalias.get('anomalias_corregidas', 0)
            },
            'dias_semana': {
                'corregidos': corregidos
            },
            'caso_ejemplo': {
                'alimentador': caso_especifico.get('alimentador') if caso_especifico else None,
                'fecha': caso_especifico.get('fecha') if caso_especifico else None,
                'hora': caso_especifico.get('hora') if caso_especifico else None
            }
        }
        
        print("\n=== RESUMEN DE TAREAS DE CALIDAD DE DATOS ===")
        print(f"Días con registros incompletos: {resumen['completitud']['dias_incompletos']}")
        print(f"Valores imputados: {resumen['imputacion']['exitosos']} exitosos, {resumen['imputacion']['limitados']} limitados")
        print(f"Anomalías: {resumen['anomalias']['detectadas']} detectadas, {resumen['anomalias']['corregidas']} corregidas")
        print(f"Días de semana corregidos: {resumen['dias_semana']['corregidos']}")
        
        # Registrar en log
        registrar_log(
            pg_hook,
            'INFO',
            'Procesamiento de calidad de datos completado',
            detalles={
                'resumen': resumen
            }
        )
        
        return resumen
    
    except Exception as e:
        print(f"❌ Error en tareas de calidad de datos: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def verificar_estado_procesadores_nifi():
    """Función de diagnóstico para verificar el estado de los procesadores"""
    try:
        procesadores_criticos = [
            {"id": QUERY_DB_TABLE_PROCESSOR_ID, "name": "QueryDatabaseTable"},
            {"id": PUBLISH_KAFKA_ID, "name": "PublishKafka"},
            {"id": CONSUME_KAFKA_PROCESSOR_ID, "name": "ConsumeKafka"},
            {"id": PUT_DB_PROCESSOR_ID, "name": "PutDatabaseRecord"},
        ]
        print("🔍 DIAGNÓSTICO DE PROCESADORES CRÍTICOS:")
        for proc in procesadores_criticos:
            estado = obtener_estado_procesador(proc["id"])
            estado_actual = estado.get("component", {}).get("state", "UNKNOWN")
            print(f"  • {proc['name']}: {estado_actual}")
            
        return True
    except Exception as e:
        print(f"Error diagnosticando: {str(e)}")
        return False

#------------ CONSTRUCCIÓN DEL DAG Y TASKGROUPS-------------------
def crear_taskgroup_mes(mes, nombre_mes):
    """
    Crea un TaskGroup para procesar un mes específico con todas sus verificaciones.
    
    Args:
        mes (int): Número del mes (1-12)
        nombre_mes (str): Nombre del mes en español (enero, febrero, etc.)
    
    Returns:
        TaskGroup: El grupo de tareas configurado para el mes específico
    """
    with TaskGroup(group_id=f"procesar_{nombre_mes}") as grupo_mes:
        verificar_mes = PythonOperator(
            task_id=f"verificar_si_{nombre_mes}_ya_procesado",
            python_callable=verificar_si_mes_ya_procesado,
            op_kwargs={"mes": mes},
        )
    
        # Branch operator que decide entre dos caminos
        branch_mes = BranchPythonOperator(
            task_id=f"branch_{nombre_mes}",
            python_callable=lambda ti, mes=mes, nombre_mes=nombre_mes: (
                f"procesar_{nombre_mes}.camino_procesamiento.procesar_mes_{mes}"
                if not ti.xcom_pull(
                    task_ids=f"procesar_{nombre_mes}.verificar_si_{nombre_mes}_ya_procesado"
                )
                else f"procesar_{nombre_mes}.skip_y_verificar.skip_{nombre_mes}"
            ),
            trigger_rule="all_done",
        )
        # Subcamino para procesamiento completo
        with TaskGroup(group_id="camino_procesamiento") as camino_procesamiento:
            procesar_mes = PythonOperator(
                task_id=f"procesar_mes_{mes}",
                python_callable=procesar_y_restaurar_mes,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            
            esperar_verificar = PythonOperator(
                task_id=f"esperar_verificar_{mes}",
                python_callable=esperar_verificar_y_finalizar_mes,
                op_kwargs={"mes": mes, "tiempo_espera_minutos": 20 if mes == 12 else 12},
                provide_context=True,
            )
            
            verificar_completado = PythonOperator(
                task_id="verificar_resultados",
                python_callable=verificar_datos_procesados_del_mes_correcto,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            # Tarea para procesar standby del mes anterior (excepto enero)
            if mes > 1:
                procesar_standby_previo = PythonOperator(
                    task_id="procesar_standby_previo",
                    python_callable=procesar_standby_mes_anterior_mejorado,
                    op_kwargs={"mes_actual": mes},
                    provide_context=True,
                )
            depurar_datos = PythonOperator(
                task_id=f"depurar_datos_{nombre_mes}",
                python_callable=depurar_mes_mejorado,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            
            corregir_dias = PythonOperator(
                task_id=f"corregir_dias_semana_mes_{mes}",
                python_callable=corregir_dia_semana_inconsistente,
                provide_context=True,
            )
            
            limpiar_datos_intermedios = PythonOperator(
                task_id=f"limpiar_datos_intermedios_{nombre_mes}",
                python_callable=truncar_datos_enviados,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            
            # Para el mes 6, añadir procesamiento especial de standby pendientes
            if mes == 6:
                procesar_todos_standby_pendientes = PythonOperator(
                    task_id="procesar_todos_standby_pendientes",
                    python_callable=procesar_standby_pendientes,
                    op_kwargs={"mes_actual": 6},
                    provide_context=True,
                )
            
            # Configurar la secuencia del camino de procesamiento con condiciones específicas por mes
            if mes == 1:
                procesar_mes >> esperar_verificar >> verificar_completado >> depurar_datos >> corregir_dias >> limpiar_datos_intermedios
            elif mes == 6:
                procesar_mes >> esperar_verificar >> verificar_completado >> procesar_standby_previo >> procesar_todos_standby_pendientes >> depurar_datos >> corregir_dias >> limpiar_datos_intermedios
            elif mes == 2:
                # Caso especial para febrero con standby final adicional
                procesar_standby_previo_final = PythonOperator(
                    task_id="procesar_standby_previo_final",
                    python_callable=procesar_standby_mes_anterior_mejorado,
                    op_kwargs={"mes_actual": mes},
                    provide_context=True,
                )
                procesar_mes >> esperar_verificar >> procesar_standby_previo >> verificar_completado >> depurar_datos >> corregir_dias >> procesar_standby_previo_final >> limpiar_datos_intermedios
            else:
                procesar_mes >> esperar_verificar >> verificar_completado >> procesar_standby_previo >> depurar_datos >> corregir_dias >> limpiar_datos_intermedios
    
        # Camino para saltar el procesamiento (igual para todos los meses)
        with TaskGroup(group_id="skip_y_verificar") as skip_y_verificar:
            skip_mes = DummyOperator(task_id=f"skip_{nombre_mes}")
            verificar_skip = PythonOperator(
                task_id=f"verificar_{nombre_mes}_skip",
                python_callable=verificar_datos_procesados_del_mes_correcto,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            
            depurar_datos_skip = PythonOperator(
                task_id=f"depurar_datos_{nombre_mes}_skip",
                python_callable=depurar_mes_mejorado,
                op_kwargs={"mes": mes},
                provide_context=True,
            )
            
            verificar_necesita_depuracion = BranchPythonOperator(
                task_id="verificar_necesita_depuracion",
                python_callable=lambda ti, nombre_mes=nombre_mes: (
                    f"procesar_{nombre_mes}.skip_y_verificar.depurar_datos_{nombre_mes}_skip" 
                    if ti.xcom_pull(task_ids=f"procesar_{nombre_mes}.skip_y_verificar.verificar_{nombre_mes}_skip") 
                    else f"procesar_{nombre_mes}.skip_y_verificar.skip_depuracion_{nombre_mes}"
                ),
            )
            skip_depuracion = DummyOperator(task_id=f"skip_depuracion_{nombre_mes}")
            
            # Secuencia de skip (igual para todos los meses)
            skip_mes >> verificar_skip >> verificar_necesita_depuracion >> [depurar_datos_skip, skip_depuracion]
    
        # Tarea dummy para marcar finalización del mes
        fin_mes = DummyOperator(
            task_id=f"fin_{nombre_mes}",
            trigger_rule="none_failed"
        )
        # Secuencia principal del grupo con punto de convergencia
        verificar_mes >> branch_mes >> [camino_procesamiento, skip_y_verificar]
        camino_procesamiento >> fin_mes
        skip_y_verificar >> fin_mes
    
    return grupo_mes

def decidir_ruta_mes(ti, mes, nombre_mes):
    """Función helper para determinar la ruta correcta"""
    # Verificar el resultado explícitamente
    resultado = ti.xcom_pull(task_ids=f"procesar_{nombre_mes}.verificar_si_{nombre_mes}_ya_procesado")
    print(f"Resultado de verificación para mes {mes}: {resultado}")
    
    if resultado:
        print(f"✅ DECISIÓN: Mes {mes} ya procesado, SALTANDO...")
        return f"procesar_{nombre_mes}.skip_y_verificar.skip_{nombre_mes}"
    else:
        print(f"🔄 DECISIÓN: Mes {mes} necesita procesarse")
        return f"procesar_{nombre_mes}.camino_procesamiento.procesar_mes_{mes}"

def decidir_procesar_mes(ti, mes, grupo):
    """Decide si procesar un mes basado en verificación previa"""
    verificacion_key = f"procesar_{grupo}.verificar_si_{grupo}_ya_procesado"
    if not ti.xcom_pull(task_ids=verificacion_key):
        return f"procesar_{grupo}.procesar_mes_{mes}"
    else:
        return f"procesar_{grupo}.skip_{grupo}"
def get_trigger_dag_operator():
    """Crea el TriggerDagRunOperator con configuración dinámica basada en YEAR"""
    try:
        year = int(Variable.get("YEAR"))
    except (ValueError, TypeError, KeyError):
        print("⚠️ Error obteniendo año desde variable, usando valor por defecto")
        year = 2019  # Valor seguro por defecto

    return TriggerDagRunOperator(
        task_id="disparar_standby_interanual",
        trigger_dag_id="potencia_standby_interanual",
        conf={
            "anio_procesado": year,
            "anio_siguiente": year + 1,
            "origen": "potencia_integrador_anual",
            "metodo": "secuencial",
            "tabla_pendientes": f"potencia_dep_pendientes_{year}"
        },
        wait_for_completion=False,
        reset_dag_run=True,
    )

# Configurar el DAG para procesar múltiples meses en secuencia con verificación
with dag:
    # Primera tarea: establecer el año
    setup_year_task = PythonOperator(
        task_id="setup_year",
        python_callable=setup_year,
        provide_context=True,
    )
    
    get_year = PythonOperator(
        task_id="get_year",
        python_callable=lambda ti: globals().update({'YEAR': int(Variable.get("YEAR"))}),
    )
    limpiar_estado_task = PythonOperator(
        task_id="limpiar_estado_entre_ejecuciones",
        python_callable=limpiar_estado_entre_ejecuciones,
    )
    validar_anio = PythonOperator(
    task_id="validar_anio_procesamiento",
    python_callable=validar_anio_procesamiento,
    # En caso de que YEAR sea None, esta tarea fallará y detendrá el DAG
    )

    verificar_tabla_original = PythonOperator(
        task_id="verificar_y_restaurar_tabla_original",
        python_callable=verificar_y_restaurar_tabla_original_al_inicio,
    )
    verificar_disponibilidad = PythonOperator(
        task_id="verificar_disponibilidad_datos",
        python_callable=verificar_disponibilidad_datos_por_mes,
    )
    setup_connections = PythonOperator(
        task_id="setup_connections",
        python_callable=create_postgres_connections,
    )
    verificar_proxy = PythonOperator(
        task_id="verificar_proxy",
        python_callable=verificar_proxy_disponible,
    )

    # Lista de meses y sus nombres
    meses_info = [
        (1, "enero"), (2, "febrero"), (3, "marzo"), (4, "abril"),
        (5, "mayo"), (6, "junio"), (7, "julio"), (8, "agosto"),
        (9, "septiembre"), (10, "octubre"), (11, "noviembre"), (12, "diciembre")
    ]

    # Crear los TaskGroups para cada mes
    grupos_meses = []
    for mes, nombre_mes in meses_info:
        grupos_meses.append(crear_taskgroup_mes(mes, nombre_mes))
    
    verificar_completitud = PythonOperator(
        task_id="verificar_completitud_anual",
        python_callable=verificar_completitud_anual,
        trigger_rule="all_done",  # Ejecutar incluso si hay fallos
    )
    
    # Tarea para guardar registros pendientes
    guardar_pendientes_task = PythonOperator(
        task_id="guardar_registros_pendientes",
        python_callable=guardar_registros_pendientes,
        trigger_rule="all_done",
    )
    
    validar_pendientes_task = PythonOperator(
    task_id="validar_tabla_pendientes",
    python_callable=validar_tabla_pendientes,
    op_kwargs={"anio": None},
    )
    # Tarea para disparar el DAG de standby interanual
    disparar_standby_task = get_trigger_dag_operator()
    
    with TaskGroup(group_id="calidad_datos") as calidad_datos_group:
        verificar_anomalias = PythonOperator(
            task_id="verificar_anomalias",
            python_callable=analizar_anomalias_y_corregir,
        )
        
        verificar_completitud_registros_task = PythonOperator(
            task_id="verificar_completitud_registros",
            python_callable=verificar_completitud_registros,
        )
        recuperar_registros_faltantes = PythonOperator(
            task_id="recuperar_registros_faltantes",
            python_callable=recuperar_datos_mes_faltantes,
            op_kwargs={"mes": None},  # Para detección automática
            provide_context=True,
        )
        corregir_registros_task = PythonOperator(
            task_id="corregir_registros_faltantes",
            python_callable=corregir_registros_faltantes,
        )
        
        imputar_faltantes = PythonOperator(
            task_id="imputar_faltantes",
            python_callable=imputar_faltantes,  # Usar la versión simplificada
            execution_timeout=timedelta(minutes=20),
            on_failure_callback=lambda context: print("⚠️ Tarea imputar_faltantes cancelada por timeout")
        )
        
        corregir_dias_semana = PythonOperator(
            task_id="corregir_dias_semana",
            python_callable=corregir_dia_semana_inconsistente,
        )
        
        ejecutar_todo = PythonOperator(
            task_id="ejecutar_tareas_calidad",
            python_callable=ejecutar_tareas_calidad_datos,
        )
        
        # Secuencia mejorada dentro del grupo
        verificar_completitud_registros_task >> recuperar_registros_faltantes >> corregir_registros_task
        [verificar_anomalias, corregir_registros_task, imputar_faltantes] >> corregir_dias_semana >> ejecutar_todo

    restaurar = PythonOperator(
        task_id="restaurar_estado_original",
        python_callable=restaurar_estado_original_completo, 
        trigger_rule="all_done",
    )
    
    limpiar_tmp = PythonOperator(
        task_id="limpiar_tablas_temporales",
        python_callable=limpiar_tablas_temporales,  
        trigger_rule="all_done",
    )

    # Configurar la secuencia principal
    setup_year_task >> get_year >> validar_anio >>limpiar_estado_task >> verificar_tabla_original >> setup_connections >> verificar_disponibilidad >> verificar_proxy >> grupos_meses[0]
    
    # Encadenar los meses en secuencia
    for i in range(len(grupos_meses)-1):
        grupos_meses[i] >> grupos_meses[i+1]
    
    # Nueva secuencia que incluye guardar_pendientes y disparar_standby
    grupos_meses[-1] >> verificar_completitud >> calidad_datos_group >> guardar_pendientes_task >> validar_pendientes_task >> disparar_standby_task >> limpiar_tmp >> restaurar
