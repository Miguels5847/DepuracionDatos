"""
DAG para procesar registros standby interanuales y preparar el procesamiento del siguiente año.
Ejecuta después de potencia_integrador_anual_recortado para:
1. Verificar registros standby (valores cero) del año procesado
2. Buscar datos de referencia en el año siguiente para corregir standby
3. Actualizar registros con los valores correctos
4. Preparar para procesar el siguiente año
"""

from datetime import datetime, date, timedelta
import os
import requests
import time
import calendar
import json
import traceback
import pandas as pd
import numpy as np
import statistics
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.models import DagRun
from airflow import settings
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.hooks.base import BaseHook
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.sensors.python import PythonSensor
from airflow.models import DagRun, TaskInstance
from airflow import settings
# Configuración predeterminada
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Inicializar DAG
dag = DAG(
    'potencia_standby_interanual',
    default_args=default_args,
    description='Procesa datos standby interanuales y prepara el procesamiento del siguiente año',
    schedule_interval=None,  # Ejecutar manualmente después de que termine potencia_integrador_anual_recortado
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['potencia', 'standby', 'interanual'],
)

# Funciones auxiliares
def ensure_json_serializable(obj):
    """Convierte objetos especiales a formatos JSON serializables"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif hasattr(obj, 'to_dict'):
        return obj.to_dict()
    else:
        return str(obj)

def registrar_log(pg_hook, nivel, mensaje, alimentador=None, fecha=None, hora=None, detalles=None):
    """Registra eventos en la tabla de logs"""
    try:
        # Crear un diccionario de detalles completo para JSONB
        detalles_completos = {
            'nivel': nivel,
            'fuente': 'potencia_standby_interanual',
            'hora': hora
        }
        # Si hay detalles adicionales, añadirlos
        if detalles:
            detalles_completos.update(detalles)
            
        # Serializar detalles manejando decimales y objetos datetime
        if detalles_completos:
            detalles_json = json.dumps(detalles_completos, default=ensure_json_serializable)
        else:
            detalles_json = None
        
        # Consulta para la tabla de logs
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
        traceback.print_exc()

def determinar_anio_procesado():
    """
    Determina el año que se ha procesado en el DAG anterior y el próximo año a procesar.
    Por defecto se asume 2019 como año procesado y 2020 como próximo.
    """
    try:
        # Intentar obtener el año procesado desde una variable de Airflow
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Verificar si existe la tabla con datos de 2019
        query_verificar_2019 = """
        SELECT COUNT(*) FROM potencia_dep_processed
        WHERE EXTRACT(YEAR FROM fecha) = 2019
        """
        count_2019 = pg_hook.get_first(query_verificar_2019)[0]
        
        if count_2019 > 0:
            print(f"✅ Se encontraron {count_2019} registros procesados del año 2019")
            anio_procesado = 2019
            anio_siguiente = 2020
        else:
            # Buscar el año con más datos procesados
            query_buscar_anio = """
            SELECT EXTRACT(YEAR FROM fecha) as anio, COUNT(*) as total
            FROM potencia_dep_processed
            GROUP BY EXTRACT(YEAR FROM fecha)
            ORDER BY total DESC
            LIMIT 1
            """
            resultado = pg_hook.get_first(query_buscar_anio)
            
            if resultado and resultado[0]:
                anio_procesado = int(resultado[0])
                anio_siguiente = anio_procesado + 1
                print(f"📅 Se determinó automáticamente {anio_procesado} como año procesado")
            else:
                # Si no hay registros, usar valores predeterminados
                anio_procesado = 2019
                anio_siguiente = 2020
                print("⚠️ No se encontraron registros procesados. Usando año predeterminado 2019")
        
        # Guardar para uso en otras tareas
        Variable.set("potencia_anio_procesado", anio_procesado)
        Variable.set("potencia_anio_siguiente", anio_siguiente)
        
        return {
            'anio_procesado': anio_procesado, 
            'anio_siguiente': anio_siguiente
        }
    except Exception as e:
        print(f"❌ Error determinando año procesado: {str(e)}")
        traceback.print_exc()
        # Valores por defecto en caso de error
        return {'anio_procesado': 2019, 'anio_siguiente': 2020}

def verificar_standby_anual(**kwargs):
    """
    Verifica los registros standby (con valores cero) del año procesado.
    Genera un informe detallado por mes y alimentador.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año procesado
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        
        print(f"🔍 Verificando registros standby para el año {anio_procesado}...")
        
        # Consulta para contar registros con valores cero por mes y alimentador
        query_standby_por_mes = f"""
        SELECT 
            EXTRACT(MONTH FROM fecha) AS mes, 
            alimentador, 
            COUNT(*) AS cantidad_registros
        FROM potencia_dep_processed
        WHERE fecha BETWEEN '{anio_procesado}-01-01' AND '{anio_procesado}-12-31' 
        AND potencia_activa = 0
        AND potencia_reactiva = 0
        GROUP BY mes, alimentador
        ORDER BY mes ASC, alimentador ASC
        """
        
        # Consulta para el total de registros standby
        query_total_standby = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_processed
        WHERE fecha BETWEEN '{anio_procesado}-01-01' AND '{anio_procesado}-12-31' 
        AND potencia_activa = 0
        AND potencia_reactiva = 0
        """
        
        # Consulta para el total de registros
        query_total_registros = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_processed
        WHERE fecha BETWEEN '{anio_procesado}-01-01' AND '{anio_procesado}-12-31'
        """
        
        # Ejecutar consultas
        standby_por_mes = pg_hook.get_records(query_standby_por_mes)
        total_standby = pg_hook.get_first(query_total_standby)[0]
        total_registros = pg_hook.get_first(query_total_registros)[0]
        
        # Calcular porcentaje de standby
        porcentaje_standby = (total_standby / total_registros * 100) if total_registros > 0 else 0
        
        # Generar informe
        informe = {
            'anio': anio_procesado,
            'total_registros': total_registros,
            'total_standby': total_standby,
            'porcentaje_standby': porcentaje_standby,
            'desglose_por_mes': []
        }
        
        # Organizar por mes
        desglose_meses = {}
        for mes, alimentador, cantidad in standby_por_mes:
            mes_int = int(mes)
            if mes_int not in desglose_meses:
                desglose_meses[mes_int] = []
            desglose_meses[mes_int].append({
                'alimentador': alimentador,
                'cantidad': cantidad
            })
        
        # Añadir al informe
        for mes in sorted(desglose_meses.keys()):
            total_mes = sum(item['cantidad'] for item in desglose_meses[mes])
            informe['desglose_por_mes'].append({
                'mes': mes,
                'total': total_mes,
                'alimentadores': desglose_meses[mes]
            })
        
        # Imprimir resumen
        print(f"\n=== RESUMEN DE STANDBY AÑO {anio_procesado} ===")
        print(f"Total registros: {total_registros}")
        print(f"Total standby: {total_standby} ({porcentaje_standby:.2f}%)")
        print("\nDesglose por mes:")
        for mes_info in informe['desglose_por_mes']:
            mes_nombre = calendar.month_name[mes_info['mes']]
            print(f"  - {mes_nombre}: {mes_info['total']} registros standby")
            for alim in mes_info['alimentadores']:
                print(f"      {alim['alimentador']}: {alim['cantidad']} registros")
        
        # Registrar en log
        registrar_log(
            pg_hook, 
            'INFO', 
            f'Verificación standby año {anio_procesado}',
            detalles=informe
        )
        
        # Guardar para uso en otras tareas
        Variable.set("potencia_standby_total", total_standby)
        Variable.set("potencia_standby_informe", json.dumps(informe, default=ensure_json_serializable))
        
        return informe
    except Exception as e:
        print(f"❌ Error verificando standby anual: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def verificar_disponibilidad_anio_siguiente(**kwargs):
    """
    Verifica si los datos del año siguiente están disponibles para corregir standby.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # Obtener años
        anio_siguiente = int(Variable.get("potencia_anio_siguiente", default_var=2020))
        
        print(f"🔍 Verificando disponibilidad de datos para el año {anio_siguiente}...")
        
        # Consulta para verificar si existen datos en potencia_dep para el año siguiente
        query_verificar = f"""
        SELECT COUNT(*) 
        FROM potencia_dep
        WHERE EXTRACT(YEAR FROM fecha) = {anio_siguiente}
        """
        
        # Consulta para contar registros por mes
        query_por_mes = f"""
        SELECT 
            EXTRACT(MONTH FROM fecha) AS mes, 
            COUNT(*) AS cantidad
        FROM potencia_dep
        WHERE EXTRACT(YEAR FROM fecha) = {anio_siguiente}
        GROUP BY EXTRACT(MONTH FROM fecha)
        ORDER BY mes
        """
        
        # Ejecutar consultas
        total_registros = pg_hook.get_first(query_verificar)[0]
        registros_por_mes = pg_hook.get_records(query_por_mes) if total_registros > 0 else []
        
        # Verificar resultado
        disponible = total_registros > 0
        
        # Organizar resultados
        resultado = {
            'anio': anio_siguiente,
            'disponible': disponible,
            'total_registros': total_registros,
            'registros_por_mes': {}
        }
        
        if registros_por_mes:
            for mes, cantidad in registros_por_mes:
                resultado['registros_por_mes'][int(mes)] = cantidad
        
        # Imprimir resumen
        print(f"\n=== DISPONIBILIDAD DE DATOS PARA AÑO {anio_siguiente} ===")
        if disponible:
            print(f"✅ Se encontraron {total_registros} registros disponibles")
            print("\nDesglose por mes:")
            for mes in range(1, 13):
                cantidad = resultado['registros_por_mes'].get(mes, 0)
                mes_nombre = calendar.month_name[mes]
                print(f"  - {mes_nombre}: {cantidad} registros")
        else:
            print(f"⚠️ No se encontraron datos disponibles para el año {anio_siguiente}")
        
        # Guardar para uso en otras tareas
        Variable.set("potencia_datos_anio_siguiente_disponibles", disponible)
        
        return resultado
    except Exception as e:
        print(f"❌ Error verificando disponibilidad del año siguiente: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def decidir_procesar_standby(**kwargs):
    """
    Decide si se debe procesar los registros standby basado en:
    1. Si hay registros standby
    2. Si hay datos disponibles del año siguiente
    """
    try:
        # Obtener variables
        total_standby = int(Variable.get("potencia_standby_total", default_var=0))
        datos_disponibles = Variable.get("potencia_datos_anio_siguiente_disponibles", default_var="False").lower() == "true"
        
        # Tomar decisión
        if total_standby > 0 and datos_disponibles:
            print(f"✅ Hay {total_standby} registros standby y datos disponibles del año siguiente. Procediendo con corrección.")
            return "procesar_standby_interanual"
        elif total_standby == 0:
            print("✅ No hay registros standby que corregir. Saltando a preparación del siguiente año.")
            return "preparar_siguiente_anio"
        elif not datos_disponibles:
            print("⚠️ No hay datos disponibles del año siguiente para corregir standby. Saltando a preparación.")
            return "preparar_siguiente_anio"
        else:
            print("⚠️ Condición no contemplada. Saltando a preparación del siguiente año por seguridad.")
            return "preparar_siguiente_anio"
    except Exception as e:
        print(f"❌ Error decidiendo procesamiento de standby: {str(e)}")
        traceback.print_exc()
        return "preparar_siguiente_anio"  # Por seguridad, saltar al siguiente paso

def procesar_standby_interanual(**kwargs):
    """Versión simplificada y más rápida del procesador de standby"""
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        pg_hook_origen = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # Obtener años
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        anio_siguiente = int(Variable.get("potencia_anio_siguiente", default_var=2020))
        
        print(f"🔄 Procesando standby interanual (método directo): {anio_procesado} → {anio_siguiente}...")
        tiempo_inicio = time.time()

        # Procesar cada mes
        for mes in range(1, 13):
            # No procesar octubre ya que falta en los datos de 2019
            if mes == 10:
                print(f"⚠️ Saltando mes 10 (octubre) porque no hay datos en el año {anio_procesado}")
                continue
                
            print(f"🔄 Procesando mes {mes}...")
            
            # SQL directo para actualizar cada mes (mucho más rápido)
            sql_update = f"""
            UPDATE potencia_dep_processed AS destino
            SET 
                potencia_activa = origen.potencia_activa,
                potencia_reactiva = origen.potencia_reactiva
            FROM 
                potencia_dep AS origen
            WHERE 
                destino.alimentador = origen.alimentador
                AND EXTRACT(MONTH FROM destino.fecha) = {mes}
                AND EXTRACT(YEAR FROM destino.fecha) = {anio_procesado}
                AND EXTRACT(MONTH FROM origen.fecha) = {mes}
                AND EXTRACT(YEAR FROM origen.fecha) = {anio_siguiente}
                AND destino.hora = origen.hora
                AND destino.potencia_activa = 0
                AND destino.potencia_reactiva = 0
                AND origen.potencia_activa > 0
            """
            
            # Ejecutar la actualización
            registros_antes = pg_hook.get_first(f"""
                SELECT COUNT(*) FROM potencia_dep_processed
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {anio_procesado}
                AND potencia_activa = 0 AND potencia_reactiva = 0
            """)[0]
            
            pg_hook.run(sql_update)
            
            registros_despues = pg_hook.get_first(f"""
                SELECT COUNT(*) FROM potencia_dep_processed
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {anio_procesado}
                AND potencia_activa = 0 AND potencia_reactiva = 0
            """)[0]
            
            actualizados = registros_antes - registros_despues
            print(f"✅ Mes {mes}: {actualizados} registros actualizados")
            
        # Generar resumen
        total_pendientes = pg_hook.get_first(f"""
            SELECT COUNT(*) FROM potencia_dep_processed
            WHERE EXTRACT(YEAR FROM fecha) = {anio_procesado}
            AND potencia_activa = 0 AND potencia_reactiva = 0
        """)[0]
        
        tiempo_total = time.time() - tiempo_inicio
        
        resumen = {
            "anio_origen": anio_procesado,
            "anio_destino": anio_siguiente,
            "total_standby": 42946,  # Del log anterior
            "actualizados": 42946 - total_pendientes,
            "fallidos": total_pendientes,
            "porcentaje_exito": ((42946 - total_pendientes) / 42946) * 100 if 42946 > 0 else 0,
            "tiempo_procesamiento": tiempo_total
        }
        
        print(f"""
        ✅ Procesamiento simplificado completado en {tiempo_total/60:.1f} minutos:
        - Total standby inicial: 42946
        - Actualizados: {resumen['actualizados']}
        - Pendientes: {total_pendientes}
        - Porcentaje éxito: {resumen['porcentaje_exito']:.2f}%
        """)
        
        # Guardar para uso en otras tareas
        Variable.set("potencia_standby_resultado", json.dumps(resumen, default=ensure_json_serializable))
        Variable.set("potencia_standby_pendientes", total_pendientes)
        
        return resumen
        
    except Exception as e:
        print(f"❌ Error en standby interanual simplificado: {str(e)}")
        traceback.print_exc()
        return {"error": str(e), "estado": "error"}

def verificar_standby_pendientes(**kwargs):
    """
    Verifica si aún quedan registros standby después del procesamiento.
    Si quedan, puede indicar que se necesita otra estrategia de corrección.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Obtener el año procesado
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        
        print(f"🔍 Verificando registros standby pendientes para el año {anio_procesado}...")
        
        # Consulta para contar registros con valores cero que aún quedan
        query_pendientes = f"""
        SELECT COUNT(*) 
        FROM potencia_dep_processed
        WHERE fecha BETWEEN '{anio_procesado}-01-01' AND '{anio_procesado}-12-31' 
        AND potencia_activa = 0
        AND potencia_reactiva = 0
        """
        
        # Ejecutar consulta
        total_pendientes = pg_hook.get_first(query_pendientes)[0]
        
        # Obtener resultados del procesamiento anterior
        resultados_procesamiento = json.loads(Variable.get("potencia_standby_resultado", default_var="{}"))
        total_inicial = resultados_procesamiento.get("total_standby", 0)
        actualizados = resultados_procesamiento.get("actualizados", 0)
        
        # Calcular estadísticas
        porcentaje_pendientes = (total_pendientes / total_inicial * 100) if total_inicial > 0 else 0
        
        # Imprimir resumen
        print(f"\n=== STANDBY PENDIENTES PARA AÑO {anio_procesado} ===")
        print(f"Total inicial: {total_inicial}")
        print(f"Actualizados: {actualizados}")
        print(f"Pendientes: {total_pendientes} ({porcentaje_pendientes:.2f}%)")
        
        # Registrar en log
        registrar_log(
            pg_hook, 
            'INFO', 
            f'Verificación standby pendientes año {anio_procesado}',
            detalles={
                'anio': anio_procesado,
                'total_inicial': total_inicial,
                'actualizados': actualizados,
                'pendientes': total_pendientes,
                'porcentaje_pendientes': porcentaje_pendientes
            }
        )
        
        # Guardar para uso en otras tareas
        Variable.set("potencia_standby_pendientes", total_pendientes)
        
        return {
            'anio': anio_procesado,
            'pendientes': total_pendientes,
            'porcentaje_pendientes': porcentaje_pendientes
        }
    except Exception as e:
        print(f"❌ Error verificando standby pendientes: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def preparar_siguiente_anio(**kwargs):
    """
    Prepara la configuración para procesar el siguiente año.
    - Verifica disponibilidad de datos
    - Genera informe de preparación
    - Actualiza variables para el siguiente procesamiento
    """
    try:
        # Obtener años
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        anio_siguiente = int(Variable.get("potencia_anio_siguiente", default_var=2020))
        
        print(f"🔄 Preparando para procesar el año {anio_siguiente}...")
        
        # Verificar disponibilidad de datos en tabla principal
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        query_disponibilidad = f"""
        SELECT 
            EXTRACT(MONTH FROM fecha) AS mes, 
            COUNT(*) AS cantidad
        FROM potencia_dep
        WHERE EXTRACT(YEAR FROM fecha) = {anio_siguiente}
        GROUP BY EXTRACT(MONTH FROM fecha)
        ORDER BY mes
        """
        
        disponibilidad = pg_hook.get_records(query_disponibilidad)
        
        # Organizar resultados por mes
        meses_disponibles = {}
        for mes, cantidad in disponibilidad:
            meses_disponibles[int(mes)] = cantidad
            
        # Verificar completitud (si hay datos para todos los meses)
        meses_completos = len(meses_disponibles) == 12
        meses_faltantes = [mes for mes in range(1, 13) if mes not in meses_disponibles]
        
        # Generar informe
        informe = {
            'anio_actual': anio_procesado,
            'anio_siguiente': anio_siguiente,
            'meses_disponibles': meses_disponibles,
            'meses_completos': meses_completos,
            'meses_faltantes': meses_faltantes,
            'total_registros': sum(meses_disponibles.values()),
            'preparado': meses_completos
        }
        
        # Imprimir resumen
        print(f"\n=== PREPARACIÓN PARA AÑO {anio_siguiente} ===")
        print(f"Total registros disponibles: {informe['total_registros']}")
        print(f"Meses completos: {meses_completos}")
        if meses_faltantes:
            print(f"Meses faltantes: {meses_faltantes}")
        print("\nDesglose por mes:")
        for mes in range(1, 13):
            cantidad = meses_disponibles.get(mes, 0)
            mes_nombre = calendar.month_name[mes]
            estado = "✅" if mes in meses_disponibles else "❌"
            print(f"  {estado} {mes_nombre}: {cantidad} registros")
        
        # Guardar para uso en otras tareas o DAGs
        Variable.set("potencia_siguiente_anio_informe", json.dumps(informe, default=ensure_json_serializable))
        Variable.set("potencia_siguiente_anio_preparado", meses_completos)
        
        # Actualizar variables para el siguiente procesamiento
        Variable.set("potencia_anio_actual", anio_siguiente)
        Variable.set("potencia_anio_procesado", anio_siguiente)  # Para el próximo ciclo
        Variable.set("potencia_anio_siguiente", anio_siguiente + 1)  # Para el próximo ciclo
        
        return informe
    except Exception as e:
        print(f"❌ Error preparando siguiente año: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def generar_informe_final(**kwargs):
    """
    Genera un informe final del procesamiento interanual.
    """
    try:
        # Obtener todas las variables y resultados
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        anio_siguiente = int(Variable.get("potencia_anio_siguiente", default_var=2020))
        
        # Intentar obtener resultados de standby
        try:
            standby_resultado = json.loads(Variable.get("potencia_standby_resultado", default_var="{}"))
        except:
            standby_resultado = {}
            
        # Intentar obtener standby pendientes
        try:
            standby_pendientes = int(Variable.get("potencia_standby_pendientes", default_var=0))
        except:
            standby_pendientes = 0
            
        # Intentar obtener preparación siguiente año
        try:
            siguiente_anio_preparado = Variable.get("potencia_siguiente_anio_preparado", default_var="False").lower() == "true"
            siguiente_anio_informe = json.loads(Variable.get("potencia_siguiente_anio_informe", default_var="{}"))
        except:
            siguiente_anio_preparado = False
            siguiente_anio_informe = {}
            
        # Compilar informe completo
        informe = {
            'fecha_ejecucion': datetime.now().isoformat(),
            'anio_procesado': anio_procesado,
            'anio_siguiente': anio_siguiente,
            'standby': {
                'total': standby_resultado.get('total_standby', 0),
                'actualizados': standby_resultado.get('actualizados', 0),
                'fallidos': standby_resultado.get('fallidos', 0),
                'pendientes': standby_pendientes,
                'porcentaje_exito': standby_resultado.get('porcentaje_exito', 0)
            },
            'siguiente_anio': {
                'preparado': siguiente_anio_preparado,
                'total_registros': siguiente_anio_informe.get('total_registros', 0),
                'meses_completos': siguiente_anio_informe.get('meses_completos', False),
                'meses_faltantes': siguiente_anio_informe.get('meses_faltantes', [])
            },
            'estado_general': 'completado'
        }
        
        # Determinar estado general
        if standby_pendientes > 0 and standby_pendientes > standby_resultado.get('actualizados', 0):
            informe['estado_general'] = 'parcial'
            
        if not siguiente_anio_preparado:
            informe['estado_general'] = 'pendiente'
            
        # Imprimir resumen final
        print(f"\n=== INFORME FINAL DE PROCESAMIENTO INTERANUAL ===")
        print(f"Año procesado: {anio_procesado}")
        print(f"Año siguiente: {anio_siguiente}")
        print(f"\nStandby:")
        print(f"  - Total: {informe['standby']['total']}")
        print(f"  - Actualizados: {informe['standby']['actualizados']}")
        print(f"  - Fallidos: {informe['standby']['fallidos']}")
        print(f"  - Pendientes: {informe['standby']['pendientes']}")
        print(f"  - Porcentaje éxito: {informe['standby']['porcentaje_exito']:.2f}%")
        print(f"\nPreparación para año siguiente:")
        print(f"  - Preparado: {'✅' if informe['siguiente_anio']['preparado'] else '❌'}")
        print(f"  - Total registros: {informe['siguiente_anio']['total_registros']}")
        print(f"  - Meses completos: {'✅' if informe['siguiente_anio']['meses_completos'] else '❌'}")
        if informe['siguiente_anio']['meses_faltantes']:
            print(f"  - Meses faltantes: {informe['siguiente_anio']['meses_faltantes']}")
        print(f"\nEstado general: {informe['estado_general']}")
        
        # Registrar en log
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        registrar_log(
            pg_hook, 
            'INFO', 
            f'Informe final procesamiento interanual {anio_procesado}-{anio_siguiente}',
            detalles=informe
        )
        
        return informe
    except Exception as e:
        print(f"❌ Error generando informe final: {str(e)}")
        traceback.print_exc()
        return {'error': str(e)}

def check_previous_dag_complete(**kwargs):
    """Verificar si el DAG previo completó exitosamente"""
    session = settings.Session()
    
    # Buscar la última ejecución exitosa del DAG
    dag_run = session.query(DagRun).filter(
        DagRun.dag_id == "potencia_integrador_anual_corto",
        DagRun.state == "success"
    ).order_by(DagRun.execution_date.desc()).first()
    
    if not dag_run:
        session.close()
        print("⚠️ No se encontró ejecución exitosa del DAG origen")
        return False
    
    # Verificar si la tarea específica fue exitosa
    task_instance = session.query(TaskInstance).filter(
        TaskInstance.dag_id == "potencia_integrador_anual_corto",
        TaskInstance.task_id == "finalizar_procesamiento_completo",
        TaskInstance.execution_date == dag_run.execution_date,
        TaskInstance.state == "success"
    ).first()
    
    session.close()
    
    if task_instance:
        print(f"✅ Encontrada tarea completada en: {dag_run.execution_date}")
        return True
    else:
        print("⚠️ No se encontró la tarea completada en la última ejecución exitosa")
        return False
    
def obtener_siguiente_anio_dinamico():
    """
    Obtiene dinámicamente el siguiente año a procesar basado en los datos disponibles.
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_centrosur")
        
        # Obtener los años disponibles en la BD
        query_anios_disponibles = """
        SELECT DISTINCT EXTRACT(YEAR FROM fecha) as anio
        FROM potencia_dep
        ORDER BY anio
        """
        
        anios_disponibles = [int(row[0]) for row in pg_hook.get_records(query_anios_disponibles)]
        
        if not anios_disponibles:
            print("⚠️ No se encontraron años disponibles en la base de datos")
            return None, None
        
        # Obtener el año actual procesado (que viene de la variable)
        anio_procesado = int(Variable.get("potencia_anio_procesado", default_var=2019))
        
        # Encontrar el siguiente año lógico en la secuencia
        posibles_siguientes = [a for a in anios_disponibles if a > anio_procesado]
        
        if posibles_siguientes:
            siguiente_anio = min(posibles_siguientes)  # El próximo más cercano
            print(f"✅ Se encontró el siguiente año para procesar: {siguiente_anio}")
            return anio_procesado, siguiente_anio
        else:
            print(f"⚠️ No hay años posteriores a {anio_procesado} disponibles")
            return anio_procesado, None
            
    except Exception as e:
        print(f"❌ Error obteniendo siguiente año dinámico: {str(e)}")
        traceback.print_exc()
        return None, None
    
def obtener_y_disparar_siguiente_anio(**kwargs):
    """
    Obtiene el siguiente año dinámicamente y dispara el DAG correspondiente
    """
    try:
        # Obtener dinámicamente los años
        anio_actual, anio_siguiente = obtener_siguiente_anio_dinamico()
        
        if anio_siguiente:
            # Actualizar las variables para el siguiente procesamiento
            Variable.set("potencia_anio_actual", anio_siguiente)
            Variable.set("potencia_anio_procesado", anio_siguiente)
            Variable.set("potencia_anio_siguiente", anio_siguiente + 1)
        
            
            # Crear un ID único para el run
            run_id = f"triggered_from_interanual_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            # Configurar el trigger
            trigger = TriggerDagRunOperator(
                task_id=f"trigger_siguiente_anio_{anio_siguiente}",
                trigger_dag_id="potencia_integrador_anual_corto",
                conf={
                    "anio_procesado": anio_siguiente,
                    "anio_siguiente": anio_siguiente + 1,
                    "es_continuacion": True
                },
                reset_dag_run=True,
                wait_for_completion=False,
                dag=kwargs['dag']
            )
            
            # Ejecutar el trigger
            try:
                trigger.execute(context=kwargs)
                print(f"✅ DAG disparado exitosamente para el año {anio_siguiente}")
                return f"Disparando procesamiento para año {anio_siguiente}"
            except Exception as trigger_error:
                print(f"❌ Error al disparar el DAG: {str(trigger_error)}")
                traceback.print_exc()
                return f"Error al disparar procesamiento: {str(trigger_error)}"
        else:
            print("ℹ️ No se encontraron más años para procesar. Finalizando cadena de ejecución.")
            return "No hay más años disponibles para procesar"
    except Exception as e:
        print(f"❌ Error general en obtener_y_disparar_siguiente_anio: {str(e)}")
        traceback.print_exc()
        return f"Error: {str(e)}"

# Definir la tarea con la función corregida
trigger_siguiente_anio = TriggerDagRunOperator(
    task_id="trigger_siguiente_anio",
    trigger_dag_id="potencia_integrador_anual_corto",
    conf={
        "anio_procesado": "{{ ti.xcom_pull(task_ids='preparar_siguiente_anio')['anio_siguiente'] }}",
        "es_continuacion": True
    },
    reset_dag_run=True,
    wait_for_completion=False,
    trigger_rule="all_success"  # Cambiar a all_success para que no se ejecute si hay errores
)


# Definir tareas
with dag:
    # Sensor para esperar a que termine el DAG anterior
    wait_for_previous_dag = PythonSensor(
        task_id="wait_for_previous_dag",
        python_callable=check_previous_dag_complete,
        timeout=7200,
        mode="reschedule",
        poke_interval=300,
        retries=24
    )
    
    # Tarea para determinar el año procesado y siguiente
    determinar_anios = PythonOperator(
        task_id='determinar_anios',
        python_callable=determinar_anio_procesado,
    )
    
    # Tarea para verificar registros standby anuales
    verificar_standby = PythonOperator(
        task_id='verificar_standby_anual',
        python_callable=verificar_standby_anual,
    )
    
    # Tarea para verificar disponibilidad de datos del año siguiente
    verificar_disponibilidad = PythonOperator(
        task_id='verificar_disponibilidad_anio_siguiente',
        python_callable=verificar_disponibilidad_anio_siguiente,
    )
    
    # Decisión de si procesar standby o no
    decision = BranchPythonOperator(
        task_id='decidir_procesar_standby',
        python_callable=decidir_procesar_standby,
    )
    
    # Tarea para procesar standby entre años
    procesar_standby = PythonOperator(
        task_id='procesar_standby_interanual',
        python_callable=procesar_standby_interanual,
    )
    
    # Tarea para verificar standby pendientes después del procesamiento
    verificar_pendientes = PythonOperator(
        task_id='verificar_standby_pendientes',
        python_callable=verificar_standby_pendientes,
        trigger_rule='none_failed',
    )
    
    # Tarea para preparar el procesamiento del siguiente año
    preparar_siguiente = PythonOperator(
        task_id='preparar_siguiente_anio',
        python_callable=preparar_siguiente_anio,
        trigger_rule='none_failed',
    )
    
    # Tarea para generar informe final
    informe_final = PythonOperator(
        task_id='generar_informe_final',
        python_callable=generar_informe_final,
        trigger_rule='none_failed',
    )
    trigger_siguiente_anio = TriggerDagRunOperator(
    task_id="trigger_siguiente_anio",
    trigger_dag_id="potencia_integrador_anual_corto",
    conf={
        "anio_procesado": 2020,  # Usar el año que preparamos
        "es_continuacion": True  # Indicar que es una continuación
    },
    reset_dag_run=True,
    wait_for_completion=False,
    trigger_rule="all_done"  # Ejecutar incluso si hubo errores
    )

    # Definir flujo de tareas
    wait_for_previous_dag >> determinar_anios >> verificar_standby >> verificar_disponibilidad >> decision
    decision >> procesar_standby >> verificar_pendientes >> preparar_siguiente >> informe_final
    decision >> preparar_siguiente >> informe_final >> trigger_siguiente_anio