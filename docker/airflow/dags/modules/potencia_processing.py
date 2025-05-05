"""
Funciones de procesamiento específicas para el DAG de potencia
que integran la funcionalidad de NiFi.
"""
import logging
import time
from airflow.providers.postgres.hooks.postgres import PostgresHook
from modules.potencia_nifi_utils import crear_gestor_potencia_nifi
from configs.potencia_config import SOURCE_TABLE, PROCESSED_TABLE, BACKUP_TABLE
from modules.potencia_nifi_utils import PotenciaNiFiManager

# Configuración de logging
logger = logging.getLogger(__name__)

# Año de procesamiento
YEAR = 2019

def procesar_mes_nifi_completo(mes, year=2019, **kwargs):
    """Procesa un mes completo usando configuración con filtro explícito"""
    try:
        logger.info(f"Iniciando procesamiento NiFi para mes {mes}")
        
        # Crear gestor NiFi
        gestor = crear_gestor_potencia_nifi()
        if not gestor:
            logger.error("No se pudo crear gestor NiFi")
            return False
        
        # Usar el método específico para configurar consulta con filtro
        if hasattr(gestor, 'configurar_consulta_con_filtro_mes'):
            resultado = gestor.configurar_consulta_con_filtro_mes(mes, year)
        elif hasattr(gestor, 'restaurar_configuracion_original'):
            resultado = gestor.restaurar_configuracion_original(mes, year)
        else:
            logger.error("No se encontró un método adecuado para configurar procesadores")
            return False
            
        if not resultado:
            logger.error(f"Error iniciando procesamiento NiFi para mes {mes}")
            return False
        
        logger.info(f"Esperando 12 minutos para completar procesamiento...")
        import time
        time.sleep(12 * 60)
        
        logger.info(f"Procesamiento NiFi completado para mes {mes}")
        return True
    except Exception as e:
        logger.error(f"Error en procesamiento NiFi: {str(e)}")
        return False

def verificar_si_mes_ya_procesado(mes: int, **kwargs):
    """
    Verifica si un mes ya ha sido procesado en la tabla de destino.
    
    Args:
        mes: Número de mes (1-12)
        **kwargs: Argumentos de contexto de Airflow
        
    Returns:
        bool: True si el mes ya está procesado, False en caso contrario
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Consulta para verificar si hay datos del mes específico
        query = f"""
        SELECT COUNT(*) AS total_registros
        FROM {PROCESSED_TABLE}
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {YEAR}
        """
        
        result = pg_hook.get_first(query)
        total_registros = result[0] if result else 0
        
        # Un mes se considera procesado si tiene suficientes registros
        minimo_esperado = 10000
        mes_completo = total_registros >= minimo_esperado
        
        logger.info(f"Mes {mes}: {total_registros} registros encontrados")
        logger.info(f"Mes {mes} ya procesado: {mes_completo}")
        
        # Este valor puede ser usado directamente por BranchPythonOperator
        return mes_completo
        
    except Exception as e:
        logger.error(f"Error verificando mes {mes}: {str(e)}")
        return False

def depurar_mes_mejorado(mes: int, **kwargs):
    """
    Depura los datos de un mes específico corrigiendo valores anómalos.
    
    Args:
        mes: Número de mes (1-12)
        **kwargs: Argumentos de contexto de Airflow
        
    Returns:
        int: Número de registros depurados
    """
    try:
        logger.info(f"Iniciando depuración de datos para el mes {mes}")
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # 1. Identificar y corregir outliers en potencia_activa
        query_outliers_activa = f"""
        WITH stats AS (
            SELECT 
                alimentador,
                AVG(potencia_activa) AS promedio,
                STDDEV(potencia_activa) AS desviacion
            FROM {PROCESSED_TABLE}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {YEAR}
            GROUP BY alimentador
        )
        UPDATE {PROCESSED_TABLE} t
        SET potencia_activa = (
            SELECT AVG(potencia_activa)
            FROM {PROCESSED_TABLE} r
            WHERE r.alimentador = t.alimentador
            AND r.hora = t.hora
            AND ABS(EXTRACT(DAY FROM r.fecha) - EXTRACT(DAY FROM t.fecha)) < 7  -- Mismo período del mes
            AND r.potencia_activa BETWEEN s.promedio - 2*s.desviacion AND s.promedio + 2*s.desviacion
        )
        FROM stats s
        WHERE t.alimentador = s.alimentador
        AND EXTRACT(MONTH FROM t.fecha) = {mes}
        AND EXTRACT(YEAR FROM t.fecha) = {YEAR}
        AND (t.potencia_activa > s.promedio + 3*s.desviacion
             OR t.potencia_activa < s.promedio - 3*s.desviacion
             OR t.potencia_activa IS NULL
             OR t.potencia_activa = 0)
        """
        
        result_activa = pg_hook.run(query_outliers_activa)
        
        # 2. Identificar y corregir outliers en potencia_reactiva
        query_outliers_reactiva = f"""
        WITH stats AS (
            SELECT 
                alimentador,
                AVG(potencia_reactiva) AS promedio,
                STDDEV(potencia_reactiva) AS desviacion
            FROM {PROCESSED_TABLE}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {YEAR}
            GROUP BY alimentador
        )
        UPDATE {PROCESSED_TABLE} t
        SET potencia_reactiva = (
            SELECT AVG(potencia_reactiva)
            FROM {PROCESSED_TABLE} r
            WHERE r.alimentador = t.alimentador
            AND r.hora = t.hora
            AND ABS(EXTRACT(DAY FROM r.fecha) - EXTRACT(DAY FROM t.fecha)) < 7  -- Mismo período del mes
            AND r.potencia_reactiva BETWEEN s.promedio - 2*s.desviacion AND s.promedio + 2*s.desviacion
        )
        FROM stats s
        WHERE t.alimentador = s.alimentador
        AND EXTRACT(MONTH FROM t.fecha) = {mes}
        AND EXTRACT(YEAR FROM t.fecha) = {YEAR}
        AND (t.potencia_reactiva > s.promedio + 3*s.desviacion
             OR t.potencia_reactiva < s.promedio - 3*s.desviacion
             OR t.potencia_reactiva IS NULL)
        """
        
        result_reactiva = pg_hook.run(query_outliers_reactiva)
        
        # 3. Verificar estado final de los datos
        query_verificacion = f"""
        SELECT 
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE potencia_activa IS NULL OR potencia_activa = 0) AS nulos_activa,
            COUNT(*) FILTER (WHERE potencia_reactiva IS NULL) AS nulos_reactiva
        FROM {PROCESSED_TABLE}
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {YEAR}
        """
        
        total, nulos_activa, nulos_reactiva = pg_hook.get_first(query_verificacion)
        
        logger.info(f"Depuración del mes {mes} completada:")
        logger.info(f"Total registros: {total}")
        logger.info(f"Registros con potencia activa nula o cero: {nulos_activa}")
        logger.info(f"Registros con potencia reactiva nula: {nulos_reactiva}")
        
        return total
        
    except Exception as e:
        logger.error(f"Error depurando mes {mes}: {str(e)}")
        return -1

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
            logger.info("✅ No se encontraron inconsistencias en los días de semana.")
            return 0
            
        logger.info(f"⚠️ Encontradas {len(inconsistencias)} inconsistencias en días de semana.")
        
        # Actualizar registros inconsistentes
        for fecha, hora, alimentador, dia_actual, dia_correcto in inconsistencias:
            logger.info(f"🔄 Corrigiendo: {alimentador}, {fecha}, {hora}: {dia_actual} → {dia_correcto}")
            
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
        logger.error(f"❌ Error corrigiendo días de semana: {str(e)}")
        return -1

def truncar_datos_enviados(mes: int, **kwargs):
    """
    Limpia datos intermedios para liberar espacio.
    
    Args:
        mes: Número de mes (1-12)
        **kwargs: Argumentos de contexto de Airflow
        
    Returns:
        bool: True si se completó correctamente
    """
    try:
        logger.info(f"Limpiando datos intermedios del mes {mes}")
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
        
        # Eliminar datos temporales si existen
        query_limpiar = """
        DO $$
        BEGIN
            IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'datos_enviados') THEN
                TRUNCATE TABLE datos_enviados;
            END IF;
        END
        $$;
        """
        
        pg_hook.run(query_limpiar)
        logger.info("✅ Limpieza de datos intermedios completada")
        return True
        
    except Exception as e:
        logger.error(f"❌ Error limpiando datos intermedios: {str(e)}")
        return False