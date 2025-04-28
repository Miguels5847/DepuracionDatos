"""
DAG para la identificación y depuración de valores cero o nulos en la tabla potencia_dep.
Este DAG:
1. Identifica registros donde potencia_activa y potencia_reactiva son cero
2. Registra estos registros en una tabla de logs para auditoría
3. Intenta imputar valores basados en datos del mes anterior
4. Actualiza la tabla original con valores imputados cuando sea posible
"""
from datetime import datetime, timedelta
import calendar
import pandas as pd
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

# Configuración de parámetros
START_YEAR = 2019
END_YEAR = 2019  # Inicialmente solo 2019, luego escalar a otros años
POSTGRES_CONN_ID = "postgres_base_prueba"

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2025, 3, 1),
}

# Definición del DAG
dag = DAG(
    'depuracion_potencia_dep',
    default_args=default_args,
    description='Detecta y corrige valores cero o nulos en tabla potencia_dep',
    schedule_interval=None,
    catchup=False,
    tags=['potencia', 'limpieza', 'imputación'],
)

# Función para crear la tabla de logs si no existe
def crear_tabla_logs():
    """Crea la tabla para registrar los valores cero/nulos identificados"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # SQL para crear la tabla de logs
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS potencia_dep_logs (
        id SERIAL PRIMARY KEY,
        fecha DATE NOT NULL,
        hora VARCHAR(5) NOT NULL,
        alimentador VARCHAR(100) NOT NULL,
        potencia_activa NUMERIC,
        potencia_reactiva NUMERIC,
        fecha_deteccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        accion VARCHAR(50),
        valor_anterior_activa NUMERIC,
        valor_anterior_reactiva NUMERIC,
        valor_nuevo_activa NUMERIC,
        valor_nuevo_reactiva NUMERIC,
        fuente_imputacion VARCHAR(200),
        mes_original INT,
        ano_original INT
    );
    """
    
    pg_hook.run(create_table_sql)
    return "Tabla de logs creada o verificada correctamente"

# Función para identificar valores cero/nulos por mes
def identificar_ceros_mes(year, month, **kwargs):
    """Identifica registros con potencia_activa=0 y potencia_reactiva=0 para un mes específico"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Determinar el primer y último día del mes
    ultimo_dia = calendar.monthrange(year, month)[1]
    fecha_inicio = f"{year}-{month:02d}-01"
    fecha_fin = f"{year}-{month:02d}-{ultimo_dia}"
    
    # Consulta para identificar valores cero
    query = f"""
    SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
    FROM public.potencia_dep 
    WHERE (potencia_activa = 0 OR potencia_activa IS NULL)
    AND (potencia_reactiva = 0 OR potencia_reactiva IS NULL)
    AND fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
    ORDER BY fecha ASC, hora ASC, alimentador ASC
    """
    
    # Ejecutar consulta y obtener resultados
    registros_cero = pg_hook.get_records(query)
    
    # Registrar resultados en XCom para uso posterior
    kwargs['ti'].xcom_push(key=f'ceros_mes_{month}_{year}', value=registros_cero)
    
    # Información de resumen
    total_registros = len(registros_cero)
    print(f"Se identificaron {total_registros} registros con valores cero en {calendar.month_name[month]} {year}")
    
    return total_registros

# Función para registrar en tabla de logs
def registrar_en_logs(year, month, **kwargs):
    """Registra los valores cero encontrados en la tabla de logs"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ti = kwargs['ti']
    
    # Recuperar registros identificados en la tarea anterior
    registros_cero = ti.xcom_pull(key=f'ceros_mes_{month}_{year}', task_ids=f'identificar_ceros_mes_{month}')
    
    if not registros_cero:
        print(f"No hay registros con valores cero para registrar en {calendar.month_name[month]} {year}")
        return 0
    
    # Preparar la inserción masiva a la tabla de logs
    insert_values = []
    for registro in registros_cero:
        fecha, hora, alimentador, potencia_activa, potencia_reactiva = registro
        
        # Formato para la inserción en la tabla de logs
        insert_values.append(
            f"('{fecha}', '{hora}', '{alimentador}', "
            f"{potencia_activa if potencia_activa is not None else 'NULL'}, "
            f"{potencia_reactiva if potencia_reactiva is not None else 'NULL'}, "
            f"CURRENT_TIMESTAMP, 'IDENTIFICADO', "
            f"{potencia_activa if potencia_activa is not None else 'NULL'}, "
            f"{potencia_reactiva if potencia_reactiva is not None else 'NULL'}, "
            f"NULL, NULL, NULL, {month}, {year})"
        )
    
    # Ejecutar la inserción masiva
    insert_sql = f"""
    INSERT INTO potencia_dep_logs 
    (fecha, hora, alimentador, potencia_activa, potencia_reactiva, 
     fecha_deteccion, accion, valor_anterior_activa, valor_anterior_reactiva,
     valor_nuevo_activa, valor_nuevo_reactiva, fuente_imputacion, mes_original, ano_original)
    VALUES {', '.join(insert_values)}
    """
    
    pg_hook.run(insert_sql)
    
    print(f"Registrados {len(insert_values)} entradas en la tabla de logs para {calendar.month_name[month]} {year}")
    return len(insert_values)

# Función para imputar valores basados en el mes anterior
def imputar_valores(year, month, **kwargs):
    """Intenta imputar valores cero usando datos del mes anterior"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    ti = kwargs['ti']
    
    # Recuperar registros identificados
    registros_cero = ti.xcom_pull(key=f'ceros_mes_{month}_{year}', task_ids=f'identificar_ceros_mes_{month}')
    
    if not registros_cero:
        print(f"No hay registros a imputar para {calendar.month_name[month]} {year}")
        return 0
    
    # Calcular mes y año anterior
    prev_month = month - 1 if month > 1 else 12
    prev_year = year if month > 1 else year - 1
    
    # Contador de registros imputados
    imputados = 0
    
    for registro in registros_cero:
        fecha, hora, alimentador, _, _ = registro
        
        # Consultar valores del mismo alimentador/hora en el mes anterior
        query_anterior = f"""
        SELECT potencia_activa, potencia_reactiva
        FROM public.potencia_dep 
        WHERE alimentador = '{alimentador}'
        AND hora = '{hora}'
        AND EXTRACT(MONTH FROM fecha) = {prev_month}
        AND EXTRACT(YEAR FROM fecha) = {prev_year}
        AND potencia_activa > 0
        AND potencia_reactiva > 0
        ORDER BY fecha DESC
        LIMIT 1
        """
        
        valores_anteriores = pg_hook.get_first(query_anterior)
        
        if valores_anteriores and len(valores_anteriores) == 2:
            potencia_activa_nueva, potencia_reactiva_nueva = valores_anteriores
            
            # Actualizar la tabla original
            update_sql = f"""
            UPDATE public.potencia_dep
            SET potencia_activa = {potencia_activa_nueva},
                potencia_reactiva = {potencia_reactiva_nueva}
            WHERE fecha = '{fecha}'
            AND hora = '{hora}'
            AND alimentador = '{alimentador}'
            """
            
            pg_hook.run(update_sql)
            
            # Actualizar el registro en la tabla de logs
            update_log_sql = f"""
            UPDATE potencia_dep_logs
            SET accion = 'IMPUTADO',
                valor_nuevo_activa = {potencia_activa_nueva},
                valor_nuevo_reactiva = {potencia_reactiva_nueva},
                fuente_imputacion = 'Mes anterior ({calendar.month_name[prev_month]} {prev_year})'
            WHERE fecha = '{fecha}'
            AND hora = '{hora}'
            AND alimentador = '{alimentador}'
            AND mes_original = {month}
            AND ano_original = {year}
            """
            
            pg_hook.run(update_log_sql)
            imputados += 1
    
    print(f"Se imputaron {imputados} de {len(registros_cero)} registros para {calendar.month_name[month]} {year}")
    return imputados

# Función para generar resumen de depuración
def generar_resumen_mes(year, month, **kwargs):
    """Genera estadísticas resumidas del proceso de depuración"""
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Consultar estadísticas de la tabla de logs para el mes específico
    query = f"""
    SELECT 
        COUNT(*) as total_registros,
        SUM(CASE WHEN accion = 'IMPUTADO' THEN 1 ELSE 0 END) as registros_imputados,
        SUM(CASE WHEN accion = 'IDENTIFICADO' THEN 1 ELSE 0 END) as registros_sin_imputar,
        AVG(CASE WHEN valor_nuevo_activa IS NOT NULL THEN valor_nuevo_activa ELSE 0 END) as promedio_activa_imputada,
        AVG(CASE WHEN valor_nuevo_reactiva IS NOT NULL THEN valor_nuevo_reactiva ELSE 0 END) as promedio_reactiva_imputada
    FROM potencia_dep_logs
    WHERE mes_original = {month}
    AND ano_original = {year}
    """
    
    resultado = pg_hook.get_first(query)
    
    if resultado:
        total, imputados, sin_imputar, prom_activa, prom_reactiva = resultado
        
        resumen = f"""
        === RESUMEN DE DEPURACIÓN: {calendar.month_name[month]} {year} ===
        Total de registros con valores cero: {total}
        Registros imputados exitosamente: {imputados} ({imputados/total*100:.2f}% si total > 0)
        Registros sin imputar: {sin_imputar}
        Valor promedio imputado (potencia activa): {prom_activa:.2f}
        Valor promedio imputado (potencia reactiva): {prom_reactiva:.2f}
        """
        
        print(resumen)
    else:
        print(f"No hay datos de depuración para {calendar.month_name[month]} {year}")
    
    return True

# Construir el DAG
with dag:
    # Tarea inicial para crear la tabla de logs
    crear_tabla = PythonOperator(
        task_id='crear_tabla_logs',
        python_callable=crear_tabla_logs,
    )
    
    # Variable para la secuencia de tareas
    tareas_previas = crear_tabla
    
    # Generar tareas para cada mes del año
    for year in range(START_YEAR, END_YEAR + 1):
        for month in range(1, 13):
            # Task Group para cada mes
            with TaskGroup(group_id=f'procesar_mes_{month}_{year}') as grupo_mes:
                # Identificar registros con valores cero
                identificar = PythonOperator(
                    task_id=f'identificar_ceros_mes_{month}',
                    python_callable=identificar_ceros_mes,
                    op_kwargs={'year': year, 'month': month},
                )
                
                # Registrar en tabla de logs
                registrar = PythonOperator(
                    task_id=f'registrar_logs_mes_{month}',
                    python_callable=registrar_en_logs,
                    op_kwargs={'year': year, 'month': month},
                )
                
                # Imputar valores
                imputar = PythonOperator(
                    task_id=f'imputar_valores_mes_{month}',
                    python_callable=imputar_valores,
                    op_kwargs={'year': year, 'month': month},
                )
                
                # Generar resumen
                resumen = PythonOperator(
                    task_id=f'resumen_mes_{month}',
                    python_callable=generar_resumen_mes,
                    op_kwargs={'year': year, 'month': month},
                )
                
                # Secuencia dentro del grupo
                identificar >> registrar >> imputar >> resumen
            
            # Conectar con la tarea previa
            tareas_previas >> grupo_mes
            tareas_previas = grupo_mes
    
    # Tarea final para generar resumen anual
    resumen_anual = PostgresOperator(
        task_id='resumen_anual',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql="""
        SELECT 
            ano_original as año,
            mes_original as mes,
            COUNT(*) as registros_totales,
            SUM(CASE WHEN accion = 'IMPUTADO' THEN 1 ELSE 0 END) as registros_imputados,
            ROUND((SUM(CASE WHEN accion = 'IMPUTADO' THEN 1 ELSE 0 END)::float / COUNT(*)) * 100, 2) as porcentaje_imputado
        FROM potencia_dep_logs
        GROUP BY ano_original, mes_original
        ORDER BY ano_original, mes_original
        """,
    )
    
    # Conectar resumen anual
    tareas_previas >> resumen_anual