import time
import calendar
import logging
import os
from typing import Dict, List, Optional, Tuple, Union, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class ProcessUtils:
    """
    Clase para gestionar el procesamiento de datos, monitoreo y verificación
    de resultados en el flujo de integración de datos.
    """
    
    def __init__(self, 
                nifi_utils=None, 
                db_utils=None, 
                tabla_base: str = "potencia_dep",
                tabla_procesada: str = "potencia_dep_processed",
                conexion_origen: str = "postgres_centrosur",
                conexion_destino: str = "postgres_base_prueba",
                ano: int = 2019):
        """
        Inicializa la clase ProcessUtils.
        
        Args:
            nifi_utils: Instancia de NifiUtils para interactuar con NiFi
            db_utils: Instancia opcional de DbUtils para operaciones de bases de datos
            tabla_base: Nombre de la tabla base en origen
            tabla_procesada: Nombre de la tabla procesada en destino
            conexion_origen: Nombre de la conexión a la base de datos origen
            conexion_destino: Nombre de la conexión a la base de datos destino
            ano: Año a procesar
        """
        self.nifi_utils = nifi_utils
        self.db_utils = db_utils
        self.tabla_base = tabla_base
        self.tabla_procesada = tabla_procesada
        self.tabla_original = f"{tabla_base}_original"
        self.conexion_origen = conexion_origen
        self.conexion_destino = conexion_destino
        self.ano = ano
        
    def configurar_procesamiento(self, 
                               producer_group_id: str,
                               consumer_group_id: str,
                               query_processor_id: str,
                               publish_kafka_id: str,
                               consume_kafka_id: str) -> None:
        """
        Configura los IDs de grupos y procesadores para el procesamiento.
        
        Args:
            producer_group_id: ID del grupo productor
            consumer_group_id: ID del grupo consumidor
            query_processor_id: ID del procesador QueryDatabaseTable
            publish_kafka_id: ID del procesador PublishKafka
            consume_kafka_id: ID del procesador ConsumeKafka
        """
        self.producer_group_id = producer_group_id
        self.consumer_group_id = consumer_group_id
        self.query_processor_id = query_processor_id
        self.publish_kafka_id = publish_kafka_id
        self.consume_kafka_id = consume_kafka_id
        
        # Registrar configuración
        logger.info(f"Configuración establecida para procesamiento de {self.tabla_base}")
        logger.info(f"Grupo productor: {producer_group_id}")
        logger.info(f"Grupo consumidor: {consumer_group_id}")
        logger.info(f"Procesador QueryDatabaseTable: {query_processor_id}")
        
def obtener_mes_referencia(self, alimentador: str, ano: int = 2019) -> int:
    """
    Obtiene el mejor mes de referencia para un alimentador.
    
    Args:
        alimentador: Código del alimentador
        ano: Año a analizar
        
    Returns:
        Número del mes (1-12) o 0 si no se encuentra ninguno adecuado
    """
    try:
        # Analizar calidad por mes
        analisis = self.analizar_calidad_datos_por_mes(alimentador, ano)
        
        # Buscar meses de referencia (>= 85% datos válidos) - Umbral ajustado
        meses_referencia = []
        for mes, datos in analisis.items():
            if datos.get("porcentaje_validos", 0) >= 85:  # Cambio de 95% a 85%
                meses_referencia.append((mes, datos.get("porcentaje_validos", 0)))
        
        # Ordenar por porcentaje de válidos (descendente)
        meses_referencia.sort(key=lambda x: x[1], reverse=True)
        
        # Preferir mes 3 (marzo) si está entre los de referencia
        if any(mes[0] == 3 for mes in meses_referencia):
            return 3
        
        # Si no hay mes 3, tomar el mejor
        if meses_referencia:
            return meses_referencia[0][0]
        
        # Si no hay ninguno, retornar 0
        return 0
        
    except Exception as e:
        logger.error(f"❌ Error obteniendo mes de referencia: {str(e)}")
        return 0

def obtener_nombres_meses(self) -> Dict[int, str]:
        """
        Obtiene un diccionario con los nombres de los meses.
        
        Returns:
            Dict[int, str]: Diccionario con {número_mes: nombre_mes}
        """
        return {i: calendar.month_name[i] for i in range(1, 13)}
    
def calcular_rango_fechas(self, mes: int) -> Tuple[str, str]:
        """
        Calcula el rango de fechas para un mes específico.
        
        Args:
            mes: Número de mes (1-12)
            
        Returns:
            Tuple[str, str]: (fecha_inicio, fecha_fin) en formato ISO
        """
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = self.ano if mes < 12 else self.ano + 1
        fecha_inicio = f"{self.ano}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        return fecha_inicio, fecha_fin
    
def inicializar_procesamiento(self) -> bool:
        """
        Realiza las verificaciones y preparaciones iniciales antes de procesar datos.
        
        Returns:
            bool: True si la inicialización fue exitosa
        """
        try:
            logger.info("Inicializando procesamiento de datos...")
            
            # 1. Verificar existencia de tabla original
            if not self.verificar_existencia_tabla_original():
                logger.error(f"No existe la tabla original {self.tabla_original}")
                return False
            
            # 2. Verificar disponibilidad de NiFi
            if self.nifi_utils and not self.nifi_utils.verify_proxy_available():
                logger.error("NiFi no está disponible. Abortando procesamiento.")
                return False
            
            # 3. Verificar disponibilidad de datos
            disponibilidad = self.verificar_disponibilidad_datos_por_mes()
            if not disponibilidad:
                logger.warning("No se pudo verificar disponibilidad de datos.")
                
            # 4. Generar informe inicial
            meses_con_datos = sum(1 for count in disponibilidad.values() if count > 0)
            logger.info(f"✅ Inicialización completada. {meses_con_datos}/12 meses tienen datos disponibles.")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error en inicialización: {str(e)}")
            return False
    
def verificar_existencia_tabla_original(self) -> bool:
        """
        Verifica si existe la tabla original en la base de datos.
        
        Returns:
            bool: True si la tabla existe
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.tabla_original}')"
            )[0]
            
            if existe:
                logger.info(f"✅ Tabla original {self.tabla_original} existe")
                return True
            else:
                logger.warning(f"⚠️ Tabla original {self.tabla_original} no existe")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error verificando tabla original: {str(e)}")
            return False
        
def verificar_disponibilidad_datos_por_mes(self) -> Dict[int, int]:
        """
        Genera un informe de disponibilidad de datos por mes en la tabla origen.
        
        Returns:
            Dict[int, int]: Diccionario con {mes: cantidad_registros}
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            logger.info("📊 VERIFICACIÓN DE DISPONIBILIDAD DE DATOS POR MES EN TABLA ORIGEN")
            logger.info("=" * 60)
            
            disponibilidad = {}
            
            for mes in range(1, 13):
                # Calcular fechas para cada mes
                fecha_inicio, fecha_fin = self.calcular_rango_fechas(mes)
                
                # Consulta para verificar datos
                query = f"""
                SELECT COUNT(*) 
                FROM {self.tabla_original}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """
                
                count = pg_hook.get_first(query)[0]
                disponibilidad[mes] = count
                
                if count > 0:
                    logger.info(f"✅ MES {mes:2d} ({calendar.month_name[mes]:>9}): {count:,} registros disponibles")
                else:
                    logger.info(f"❌ MES {mes:2d} ({calendar.month_name[mes]:>9}): SIN DATOS")
            
            logger.info("=" * 60)
            logger.info(f"Total de meses con datos: {sum(1 for count in disponibilidad.values() if count > 0)}/12")
            
            return disponibilidad
            
        except Exception as e:
            logger.error(f"❌ Error verificando disponibilidad: {str(e)}")
            return {}
    
def verificar_existencia_datos_fuente(self, mes: int, **kwargs) -> bool:
        """
        Verifica si existen datos para un mes específico en cualquier tabla disponible.
        
        Args:
            mes: Número de mes a verificar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si existen datos, False en caso contrario
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)

            # Calcular el rango de fechas para el mes
            fecha_inicio, fecha_fin = self.calcular_rango_fechas(mes)

            logger.info(f"🔍 BÚSQUEDA EXHAUSTIVA: Buscando datos REALES del mes {mes} en todas las tablas disponibles")
            
            # 1. Verificar en tabla original
            query_original = f"""
            SELECT COUNT(*) FROM {self.tabla_original}  
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """
            count_original = pg_hook.get_first(query_original)[0]
            
            if count_original > 0:
                logger.info(f"✅ Encontrados {count_original} registros reales para mes {mes} en {self.tabla_original}")
                return True
                
            # 2. Verificar en tabla base (no original)
            query_main = f"""
            SELECT COUNT(*) FROM {self.tabla_base} 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """
            try:
                count_main = pg_hook.get_first(query_main)[0]
                
                if count_main > 0:
                    logger.info(f"✅ Encontrados {count_main} registros reales para mes {mes} en {self.tabla_base} principal")
                    return True
            except:
                logger.warning(f"Tabla {self.tabla_base} no existe o no es accesible")
                
            # 3. Buscar en TODAS las tablas que podrían tener datos reales
            logger.info(f"🔎 Buscando en todas las tablas relacionadas...")
            
            # Obtener todas las tablas potenciales
            query_tablas = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE '{self.tabla_base}_%' 
            AND table_schema = 'public'
            """
            
            tablas = pg_hook.get_records(query_tablas)
            
            for tabla in tablas:
                tabla_nombre = tabla[0]
                # Evitar tablas ya verificadas o de procesamiento
                if tabla_nombre not in [self.tabla_base, self.tabla_original, self.tabla_procesada]:
                    try:
                        # Verificar si tiene datos para el mes
                        check_query = f"""
                        SELECT COUNT(*) FROM {tabla_nombre}
                        WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                        """
                        count = pg_hook.get_first(check_query)[0]
                        
                        if count > 0:
                            logger.info(f"✅ ENCONTRADOS {count} registros REALES del mes {mes} en tabla {tabla_nombre}")
                            return True
                    except:
                        # Algunas tablas pueden no tener la estructura esperada
                        pass
            
            # Si llegamos aquí, realmente no hay datos
            logger.warning(f"❌ CONFIRMADO: No existen datos reales para el mes {mes} en NINGUNA tabla")
            logger.warning(f"❌ No se generarán datos artificiales siguiendo el requerimiento del usuario")
            
            # Marcar como sin datos reales
            if 'ti' in kwargs and kwargs['ti']:
                kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
                kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
                
            return False
                
        except Exception as e:
            logger.error(f"❌ Error verificando datos fuente: {str(e)}")
            return False
    
def verificar_si_mes_ya_procesado(self, mes: int, **kwargs) -> bool:
        """
        Verifica si un mes específico ya tiene datos procesados en la tabla destino.
        
        Args:
            mes: Número de mes a verificar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si el mes ya tiene datos procesados
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_destino)

            # Construir consulta para verificar existencia de datos
            query = f"""
            SELECT COUNT(*) FROM {self.tabla_procesada} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {self.ano}
            """
            count = pg_hook.get_first(query)[0]
            
            if count > 0:
                logger.info(f"✅ El mes {mes} del año {self.ano} ya tiene {count} registros procesados")

                # Mostrar una muestra para verificar la calidad de los datos
                sample_query = f"""
                SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {self.tabla_procesada} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {self.ano}
                ORDER BY RANDOM()
                LIMIT 2
                """
                samples = pg_hook.get_records(sample_query)
                logger.info("📊 Muestra de registros existentes:")
                for sample in samples:
                    logger.info(f"  {sample}")

                return True
            else:
                logger.info(f"⚠️ No se encontraron datos del mes {mes} año {self.ano}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en verificación: {str(e)}")
            return False
    
def verificar_datos_procesados_del_mes_correcto(self, mes: int, **kwargs) -> bool:
        """
        Verifica que los datos procesados correspondan al mes correcto.
        
        Args:
            mes: Número de mes a verificar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si los datos corresponden al mes correcto
        """
        try:
            # VERIFICACIÓN DE MESES SIN DATOS
            ti = kwargs.get('ti')
            if ti and ti.xcom_pull(key=f'mes_{mes}_sin_datos_reales'):
                logger.info(f"✅ VERIFICACIÓN EXITOSA: El mes {mes} fue marcado como sin datos. No se requieren registros.")
                return True  # Éxito para meses sin datos
                
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_destino)

            # VERIFICACIÓN ESPECIAL PARA DICIEMBRE
            if mes == 12:
                fecha_inicio = f"{self.ano}-12-01"
                fecha_fin = f"{self.ano+1}-01-01"
                
                # Verificar por fecha explícitamente
                query_fecha = f"""
                SELECT COUNT(*) FROM {self.tabla_procesada} 
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                """
                count_fecha = pg_hook.get_first(query_fecha)[0]
                
                if count_fecha > 0:
                    logger.info(f"✅ VERIFICACIÓN DICIEMBRE CORRECTA: {count_fecha} registros en el rango de fechas correcto")
                    return True
            
            # VERIFICACIÓN ESTÁNDAR PARA OTROS MESES
            query = f"""
            SELECT COUNT(*) FROM {self.tabla_procesada} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {self.ano}
            """
            count = pg_hook.get_first(query)[0]
            
            if count > 0:
                logger.info(f"✅ Verificación exitosa: {count} registros del mes {mes} año {self.ano}")
                
                # Mostrar algunos ejemplos para validar
                sample_query = f"""
                SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {self.tabla_procesada} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {self.ano}
                LIMIT 5
                """
                samples = pg_hook.get_records(sample_query)
                logger.info("📊 Muestra de registros procesados:")
                for sample in samples:
                    logger.info(f"  {sample}")
                    
                return True
            else:
                logger.warning(f"⚠️ No se encontraron datos del mes {mes} año {self.ano}")
                return False

        except Exception as e:
            logger.error(f"❌ Error en verificación: {str(e)}")
            return False
def procesar_y_restaurar_mes(self, mes: int, **kwargs) -> bool:
        """
        Procesa el mes utilizando la función de sustitución de tabla y gestiona los casos sin datos.
        
        Args:
            mes: Número de mes a procesar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # VERIFICACIÓN CRÍTICA - Confirmar que la tabla base tiene todos los datos
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # 1. Verificar que la tabla contiene datos de diferentes meses
            otros_meses = pg_hook.get_records(f"""
                SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) 
                FROM {self.tabla_base} 
                GROUP BY EXTRACT(MONTH FROM fecha)
                ORDER BY mes
            """)
            
            logger.info(f"📊 VERIFICACIÓN PREVIA para mes {mes} - Distribución en {self.tabla_base}:")
            for m in otros_meses:
                mes_num = int(m[0])
                logger.info(f"  - Mes {mes_num}: {m[1]} registros")
            
            # Si solo hay un mes y no es 1 (enero), probable error de restauración
            if len(otros_meses) <= 1 and mes > 1:
                logger.warning(f"⚠️ ALERTA CRÍTICA: {self.tabla_base} solo contiene datos de un mes, pero estamos procesando mes {mes}")
                logger.info(f"🔄 Intentando restauración de emergencia antes de procesar...")
                
                # Restaurar desde tabla original
                if pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.tabla_original}')"
                )[0]:
                    # Hacer backup del estado actual por si acaso
                    backup_actual = f"{self.tabla_base}_backup_emergencia_{int(time.time())}"
                    pg_hook.run(f"CREATE TABLE {backup_actual} AS SELECT * FROM {self.tabla_base}")
                    
                    # Restaurar desde original
                    pg_hook.run(f"DROP TABLE IF EXISTS {self.tabla_base}")
                    pg_hook.run(f"CREATE TABLE {self.tabla_base} AS SELECT * FROM {self.tabla_original}")
                    logger.info(f"🆘 RESTAURACIÓN DE EMERGENCIA completada antes de procesar mes {mes}")
                    
                    # Verificar nuevamente
                    count_restaurado = pg_hook.get_first(f"SELECT COUNT(*) FROM {self.tabla_base}")[0]
                    logger.info(f"📊 Tabla {self.tabla_base} restaurada con {count_restaurado} registros totales")
            
            # VERIFICACIÓN ADICIONAL: Si ya hay datos, no procesar
            pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
            query = f"""
            SELECT COUNT(*) FROM {self.tabla_procesada} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {self.ano}
            """
            count = pg_hook_destino.get_first(query)[0]
            if count > 0:
                logger.info(f"🔒 VERIFICACIÓN REDUNDANTE: El mes {mes} ya tiene {count} registros. NO SE PROCESARÁ.")
                return True
            
            # VERIFICACIÓN MÁS COMPLETA: Buscar en todas las fuentes posibles
            # Primero verificar en tabla original
            tiene_datos = self.verificar_existencia_datos_fuente(mes, **kwargs)
            
            if not tiene_datos:
                # Registramos esta situación para información
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
                logger.warning(f"⚠️ Advertencia: No se detectaron datos en origen principal para el mes {mes}")
                logger.info(f"🔍 Intentando buscar en fuentes alternativas...")
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
                logger.warning(f"⚠️ NO HAY DATOS para el mes {mes}. Se ha verificado en todas las fuentes disponibles.")
                logger.warning(f"⚠️ Este no es un error del DAG, simplemente no hay datos para procesar.")
                return True
            
            # Si hay datos, ejecutar el procesamiento
            logger.info(f"🚀 Iniciando procesamiento del mes {mes}...")
            resultado_procesamiento = self.procesar_mes_sustituyendo_tabla(mes, **kwargs)
            
            return resultado_procesamiento
            
        except Exception as e:
            logger.error(f"❌ Error procesando mes {mes}: {str(e)}")
            
            # Restauración de emergencia si hay error
            try:
                pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
                pg_hook.run(f"DROP TABLE IF EXISTS {self.tabla_base}")
                pg_hook.run(f"CREATE TABLE {self.tabla_base} AS SELECT * FROM {self.tabla_original}")
                logger.info(f"🆘 RESTAURACIÓN DE EMERGENCIA tras error en procesar_y_restaurar_mes: {mes}")
            except Exception as e2:
                logger.error(f"❌ Error secundario en restauración: {str(e2)}")
                
            return False
    
def procesar_mes_sustituyendo_tabla(self, mes: int, **kwargs) -> bool:
        """
        Crea una tabla temporal para el procesamiento sin alterar la tabla original.
        Implementa el "método nuclear" para aislar los datos del mes.
        
        Args:
            mes: Número de mes a procesar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # Inicialización de variables
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Calcular rango de fechas
            fecha_inicio, fecha_fin = self.calcular_rango_fechas(mes)
            logger.info(f"🚨 AISLAMIENTO SEGURO para mes {mes} ({fecha_inicio} a {fecha_fin})")
            
            # Detener todos los procesadores para asegurar un entorno limpio
            logger.info("⏸️ Deteniendo todos los procesadores...")
            if self.nifi_utils:
                self.nifi_utils.stop_all_processors([self.producer_group_id, self.consumer_group_id])
                time.sleep(20)  # Tiempo extra para asegurar detención completa
            
            # 1. CREAR NOMBRE ÚNICO PARA LA TABLA TEMPORAL
            tabla_trabajo = f"{self.tabla_base}_work_{int(time.time())}"
            logger.info(f"📋 Creando tabla de trabajo {tabla_trabajo}...")
            
            # 2. Eliminar tabla si existe (por seguridad)
            pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
            
            # 3. Verificar registros existentes para este mes
            query_verificar = f"""
                SELECT COUNT(*) FROM {self.tabla_base} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {self.ano}
            """
            count = pg_hook.get_first(query_verificar)[0]
            
            if count == 0:
                logger.warning(f"⚠️ No hay datos para el mes {mes} en {self.tabla_base}. Abortando procesamiento.")
                return False
            
            logger.info(f"✅ Encontrados {count} registros en {self.tabla_base} para el mes {mes}")
            
            # 4. CREAR LA TABLA DE TRABAJO con filtros de fecha
            # Para Diciembre, aplicar filtros más estrictos
            if mes == 12:
                create_query = f"""
                    CREATE TABLE {tabla_trabajo} AS
                    SELECT * FROM {self.tabla_base} 
                    WHERE EXTRACT(MONTH FROM fecha) = 12
                    AND EXTRACT(YEAR FROM fecha) = {self.ano}
                    AND fecha >= '{self.ano}-12-01'
                    AND fecha < '{self.ano+1}-01-01'
                """
            else:
                create_query = f"""
                    CREATE TABLE {tabla_trabajo} AS
                    SELECT * FROM {self.tabla_base} 
                    WHERE EXTRACT(MONTH FROM fecha) = {mes}
                    AND EXTRACT(YEAR FROM fecha) = {self.ano}
                    AND fecha >= '{fecha_inicio}'
                    AND fecha < '{fecha_fin}'
                """
                
            pg_hook.run(create_query)
                
            # 5. Verificar creación correcta
            count_trabajo = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_trabajo}")[0]
            logger.info(f"✅ Tabla de trabajo {tabla_trabajo} creada con {count_trabajo} registros exclusivos del mes {mes}")
            
            # 6. Registrar la tabla trabajo para limpieza posterior
            if 'ti' in kwargs:
                kwargs['ti'].xcom_push(key=f'tabla_trabajo_mes_{mes}', value=tabla_trabajo)
            
            # 7. Aplicar validación adicional para Diciembre
            if mes == 12:
                # Verificar que la tabla solo contiene datos de Diciembre
                verify_query = f"""
                SELECT DISTINCT EXTRACT(MONTH FROM fecha) AS mes
                FROM {tabla_trabajo}
                """
                meses_en_tabla = pg_hook.get_records(verify_query)
                logger.info(f"🔍 Verificación de meses en tabla {tabla_trabajo}: {meses_en_tabla}")
                
                # Si encontramos datos de otros meses, filtrar de nuevo
                if len(meses_en_tabla) > 1:
                    logger.warning(f"⚠️ ¡ALERTA! La tabla incluye datos de varios meses. Filtrando estrictamente...")
                    temp_tabla = f"{tabla_trabajo}_filtered"
                    pg_hook.run(f"DROP TABLE IF EXISTS {temp_tabla}")
                    pg_hook.run(f"""
                        CREATE TABLE {temp_tabla} AS
                        SELECT * FROM {tabla_trabajo} 
                        WHERE EXTRACT(MONTH FROM fecha) = 12
                        AND EXTRACT(YEAR FROM fecha) = {self.ano}
                        AND fecha >= '{self.ano}-12-01' AND fecha < '{self.ano+1}-01-01'
                    """)
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                    pg_hook.run(f"ALTER TABLE {temp_tabla} RENAME TO {tabla_trabajo}")
                    logger.info(f"✅ Tabla {tabla_trabajo} purificada para contener SOLO datos de Diciembre")
                    
                # MÉTODO NUCLEAR PARA DICIEMBRE
                logger.info("☢️ APLICANDO MÉTODO NUCLEAR PARA DICIEMBRE...")
                
                # 1. Crear backup de tabla original
                backup_tabla = f"{self.tabla_base}_backup_dic_{int(time.time())}"
                pg_hook.run(f"CREATE TABLE {backup_tabla} AS SELECT * FROM {self.tabla_base}")
                logger.info(f"✅ Backup creado: {backup_tabla}")
                
                # 2. Truncar y solo dejar datos de Diciembre
                pg_hook.run(f"TRUNCATE TABLE {self.tabla_base}")
                pg_hook.run(f"""
                    INSERT INTO {self.tabla_base}
                    SELECT * FROM {tabla_trabajo}
                    WHERE EXTRACT(MONTH FROM fecha) = 12
                    AND EXTRACT(YEAR FROM fecha) = {self.ano}
                """)
                temp_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {self.tabla_base}")[0]
                logger.info(f"✅ Tabla {self.tabla_base} modificada para contener SOLO {temp_count} registros de Diciembre")
                
                # 3. Registrar para restauración posterior
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key='diciembre_backup_table', value=backup_tabla)
                
                # 4. Crear una consulta simple (sin preocuparse por la tabla temporal)
                nueva_consulta = f"""/* MÉTODO_NUCLEAR_DICIEMBRE_{int(time.time())} */
    SELECT 
        fecha, 
        hora, 
        alimentador, 
        dia_semana, 
        potencia_activa AS potencia_activa_real, 
        potencia_reactiva AS potencia_reactiva_real
    FROM {self.tabla_base}
    ORDER BY fecha, hora, alimentador
    """
                # Actualizar el procesador con la consulta simple
                if self.nifi_utils and self.query_processor_id:
                    self.nifi_utils.update_query_processor(
                        self.query_processor_id, 
                        nueva_consulta, 
                        clear_table_name=True
                    )
                
            else:
                # Consulta regular para otros meses
                nueva_consulta = f"""/* CONSULTA_MES_{mes}_{int(time.time())} */
    SELECT 
        fecha, 
        hora, 
        alimentador, 
        dia_semana, 
        potencia_activa AS potencia_activa_real, 
        potencia_reactiva AS potencia_reactiva_real
    FROM {tabla_trabajo}
    WHERE EXTRACT(MONTH FROM fecha) = {mes}
    AND EXTRACT(YEAR FROM fecha) = {self.ano}
    ORDER BY fecha, hora, alimentador
    """
                
                # Actualizar consulta en el procesador
                if self.nifi_utils and self.query_processor_id:
                    resultado = self.nifi_utils.update_query_processor(
                        self.query_processor_id, 
                        nueva_consulta, 
                        clear_table_name=True
                    )
                    
                    if not resultado:
                        logger.error(f"❌ Error actualizando consulta: no se pudo configurar el procesador")
                        pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                        return False
                        
                    logger.info(f"✅ Consulta SQL configurada para usar tabla de trabajo {tabla_trabajo}")
            
            # 8. Limpiar datos existentes en destino
            logger.info("🗑️ Limpiando datos existentes en destino...")
            pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
            pg_hook_destino.run(f"""
                DELETE FROM {self.tabla_procesada} 
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """)
            
            # 9. Configurar topic Kafka único
            if self.nifi_utils and self.publish_kafka_id and self.consume_kafka_id:
                topic_name = f"{self.tabla_base.replace('_', '-')}-mes{mes}-{int(time.time())}"
                logger.info(f"🔄 Configurando topic Kafka único: {topic_name}")
                self.nifi_utils.configure_kafka_topic(self.publish_kafka_id, "Topic Name", topic_name)
                self.nifi_utils.configure_kafka_topic(self.consume_kafka_id, "topic", topic_name)
            
            # 10. Limpiar estado y colas
            logger.info("🧹 Limpiando estado del procesador y colas...")
            if self.nifi_utils:
                if self.query_processor_id:
                    self.nifi_utils.clear_processor_state_and_queues([self.query_processor_id])
                
        except Exception as e:
            logger.error(f"❌ Error en el procesamiento: {str(e)}")
            return False
            
def procesar_y_restaurar_mes(self, mes: int, **kwargs) -> bool:
        """
        Procesa el mes utilizando la función de sustitución de tabla y gestiona los casos sin datos.
        
        Args:
            mes: Número de mes a procesar (1-12)
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si el procesamiento fue exitoso
        """
        try:
            # VERIFICACIÓN CRÍTICA - Confirmar que la tabla base tiene todos los datos
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # 1. Verificar que la tabla contiene datos de diferentes meses
            otros_meses = pg_hook.get_records(f"""
                SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) 
                FROM {self.tabla_base} 
                GROUP BY EXTRACT(MONTH FROM fecha)
                ORDER BY mes
            """)
            
            logger.info(f"📊 VERIFICACIÓN PREVIA para mes {mes} - Distribución en {self.tabla_base}:")
            for m in otros_meses:
                mes_num = int(m[0])
                logger.info(f"  - Mes {mes_num}: {m[1]} registros")
            
            # Si solo hay un mes y no es 1 (enero), probable error de restauración
            if len(otros_meses) <= 1 and mes > 1:
                logger.warning(f"⚠️ ALERTA CRÍTICA: {self.tabla_base} solo contiene datos de un mes, pero estamos procesando mes {mes}")
                logger.info(f"🔄 Intentando restauración de emergencia antes de procesar...")
                
                # Restaurar desde tabla original
                if pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{self.tabla_original}')"
                )[0]:
                    # Hacer backup del estado actual por si acaso
                    backup_actual = f"{self.tabla_base}_backup_emergencia_{int(time.time())}"
                    pg_hook.run(f"CREATE TABLE {backup_actual} AS SELECT * FROM {self.tabla_base}")
                    
                    # Restaurar desde original
                    pg_hook.run(f"DROP TABLE IF EXISTS {self.tabla_base}")
                    pg_hook.run(f"CREATE TABLE {self.tabla_base} AS SELECT * FROM {self.tabla_original}")
                    logger.info(f"🆘 RESTAURACIÓN DE EMERGENCIA completada antes de procesar mes {mes}")
                    
                    # Verificar nuevamente
                    count_restaurado = pg_hook.get_first(f"SELECT COUNT(*) FROM {self.tabla_base}")[0]
                    logger.info(f"📊 Tabla {self.tabla_base} restaurada con {count_restaurado} registros totales")
            
            # VERIFICACIÓN ADICIONAL: Si ya hay datos, no procesar
            pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
            query = f"""
            SELECT COUNT(*) FROM {self.tabla_procesada} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {self.ano}
            """
            count = pg_hook_destino.get_first(query)[0]
            if count > 0:
                logger.info(f"🔒 VERIFICACIÓN REDUNDANTE: El mes {mes} ya tiene {count} registros. NO SE PROCESARÁ.")
                return True
            
            # VERIFICACIÓN MÁS COMPLETA: Buscar en todas las fuentes posibles
            # Primero verificar en tabla original
            tiene_datos = self.verificar_existencia_datos_fuente(mes, **kwargs)
            
            if not tiene_datos:
                # Registramos esta situación para información
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key=f'mes_{mes}_datos_no_detectados', value=True)
                logger.warning(f"⚠️ Advertencia: No se detectaron datos en origen principal para el mes {mes}")
                logger.info(f"🔍 Intentando buscar en fuentes alternativas...")
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key=f'mes_{mes}_sin_datos_reales', value=True)
                logger.warning(f"⚠️ NO HAY DATOS para el mes {mes}. Se ha verificado en todas las fuentes disponibles.")
                logger.warning(f"⚠️ Este no es un error del DAG, simplemente no hay datos para procesar.")
                return True
            
            # Si hay datos, ejecutar el procesamiento
            logger.info(f"🚀 Iniciando procesamiento del mes {mes}...")
            resultado_procesamiento = self.procesar_mes_sustituyendo_tabla(mes, **kwargs)
            
            return resultado_procesamiento
            
        except Exception as e:
            logger.error(f"❌ Error procesando mes {mes}: {str(e)}")
            
            # Restauración de emergencia si hay error
            try:
                pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
                pg_hook.run(f"DROP TABLE IF EXISTS {self.tabla_base}")
                pg_hook.run(f"CREATE TABLE {self.tabla_base} AS SELECT * FROM {self.tabla_original}")
                logger.info(f"🆘 RESTAURACIÓN DE EMERGENCIA tras error en procesar_y_restaurar_mes: {mes}")
            except Exception as e2:
                logger.error(f"❌ Error secundario en restauración: {str(e2)}")
                
            return False
    
def procesar_mes_sustituyendo_tabla(self, mes: int, ano: int = 2019, 
                                       tabla_origen: str = "potencia_dep",
                                       tabla_original: str = "potencia_dep_original",
                                       conexion_origen: str = "postgres_centrosur",
                                       **kwargs) -> bool:
        """
        Crea una tabla temporal para el procesamiento aislado del mes.
        
        Args:
            mes: Número de mes a procesar (1-12)
            ano: Año a procesar
            tabla_origen: Nombre de la tabla origen
            tabla_original: Nombre de la tabla respaldo original
            conexion_origen: Nombre de la conexión a la base de datos origen
            **kwargs: Argumentos adicionales
            
        Returns:
            bool: True si el procesamiento fue correcto
        """
        try:
            # Inicialización de variables
            pg_hook = PostgresHook(postgres_conn_id=conexion_origen)
            
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            logger.info(f"🚨 AISLAMIENTO SEGURO para mes {mes} ({fecha_inicio} a {fecha_fin})")
            
            # Detener todos los procesadores para asegurar un entorno limpio
            logger.info("⏸️ Deteniendo todos los procesadores...")
            self.nifi_utils.stop_all_processors(self.nifi_utils.get_all_processor_groups())
            time.sleep(20)  # Tiempo extra para asegurar detención completa
            
            # 1. CREAR NOMBRE ÚNICO PARA LA TABLA TEMPORAL
            tabla_trabajo = f"{tabla_origen}_work_{int(time.time())}"
            logger.info(f"📋 Creando tabla de trabajo {tabla_trabajo}...")
            
            # 2. Eliminar tabla si existe (por seguridad)
            pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
            
            # 3. Verificar registros existentes para este mes
            query_verificar = f"""
                SELECT COUNT(*) FROM {tabla_origen} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            count = pg_hook.get_first(query_verificar)[0]
            
            if count == 0:
                logger.warning(f"⚠️ No hay datos para el mes {mes} en {tabla_origen}. Abortando procesamiento.")
                return False
            
            logger.info(f"✅ Encontrados {count} registros en {tabla_origen} para el mes {mes}")
            
            # 4. CREAR LA TABLA DE TRABAJO con filtros de fecha
            # Para Diciembre, aplicar filtros más estrictos
            if mes == 12:
                create_query = f"""
                    CREATE TABLE {tabla_trabajo} AS
                    SELECT * FROM {tabla_origen} 
                    WHERE EXTRACT(MONTH FROM fecha) = 12
                    AND EXTRACT(YEAR FROM fecha) = {ano}
                    AND fecha >= '{ano}-12-01'
                    AND fecha < '{ano+1}-01-01'
                """
            else:
                create_query = f"""
                    CREATE TABLE {tabla_trabajo} AS
                    SELECT * FROM {tabla_origen} 
                    WHERE EXTRACT(MONTH FROM fecha) = {mes}
                    AND EXTRACT(YEAR FROM fecha) = {ano}
                    AND fecha >= '{fecha_inicio}'
                    AND fecha < '{fecha_fin}'
                """
                
            pg_hook.run(create_query)
                
            # 5. Verificar creación correcta
            count_trabajo = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_trabajo}")[0]
            logger.info(f"✅ Tabla de trabajo {tabla_trabajo} creada con {count_trabajo} registros exclusivos del mes {mes}")
            
            # 6. Registrar la tabla trabajo para limpieza posterior
            if 'ti' in kwargs:
                kwargs['ti'].xcom_push(key=f'tabla_trabajo_mes_{mes}', value=tabla_trabajo)
            
            # 7. Aplicar validación adicional para Diciembre
            if mes == 12:
                # Verificar que la tabla solo contiene datos de Diciembre
                verify_query = f"""
                SELECT DISTINCT EXTRACT(MONTH FROM fecha) AS mes
                FROM {tabla_trabajo}
                """
                meses_en_tabla = pg_hook.get_records(verify_query)
                logger.info(f"🔍 Verificación de meses en tabla {tabla_trabajo}: {meses_en_tabla}")
                
                # Si encontramos datos de otros meses, filtrar de nuevo
                if len(meses_en_tabla) > 1:
                    logger.warning(f"⚠️ ¡ALERTA! La tabla incluye datos de varios meses. Filtrando estrictamente...")
                    temp_tabla = f"{tabla_trabajo}_filtered"
                    pg_hook.run(f"DROP TABLE IF EXISTS {temp_tabla}")
                    pg_hook.run(f"""
                        CREATE TABLE {temp_tabla} AS
                        SELECT * FROM {tabla_trabajo} 
                        WHERE EXTRACT(MONTH FROM fecha) = 12
                        AND EXTRACT(YEAR FROM fecha) = {ano}
                        AND fecha >= '{ano}-12-01' AND fecha < '{ano+1}-01-01'
                    """)
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                    pg_hook.run(f"ALTER TABLE {temp_tabla} RENAME TO {tabla_trabajo}")
                    logger.info(f"✅ Tabla {tabla_trabajo} purificada para contener SOLO datos de Diciembre")
                    
                # MÉTODO NUCLEAR PARA DICIEMBRE
                logger.info("☢️ APLICANDO MÉTODO NUCLEAR PARA DICIEMBRE...")
                
                # 1. Crear backup de tabla original
                backup_tabla = f"{tabla_origen}_backup_dic_{int(time.time())}"
                pg_hook.run(f"CREATE TABLE {backup_tabla} AS SELECT * FROM {tabla_origen}")
                logger.info(f"✅ Backup creado: {backup_tabla}")
                
                # 2. Truncar y solo dejar datos de Diciembre
                pg_hook.run(f"TRUNCATE TABLE {tabla_origen}")
                pg_hook.run(f"""
                    INSERT INTO {tabla_origen}
                    SELECT * FROM {tabla_trabajo}
                    WHERE EXTRACT(MONTH FROM fecha) = 12
                    AND EXTRACT(YEAR FROM fecha) = {ano}
                """)
                temp_count = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_origen}")[0]
                logger.info(f"✅ Tabla {tabla_origen} modificada para contener SOLO {temp_count} registros de Diciembre")
                
                # 3. Registrar para restauración posterior
                if 'ti' in kwargs:
                    kwargs['ti'].xcom_push(key='diciembre_backup_table', value=backup_tabla)
                
                # 4. Crear una consulta simple (sin preocuparse por la tabla temporal)
                nueva_consulta = f"""/* MÉTODO_NUCLEAR_DICIEMBRE_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    alimentador, 
    dia_semana, 
    potencia_activa AS potencia_activa_real, 
    potencia_reactiva AS potencia_reactiva_real
FROM {tabla_origen}
ORDER BY fecha, hora, alimentador
"""
                # Actualizar el procesador con la consulta simple
                if hasattr(self.nifi_utils, 'query_processor_id') and self.nifi_utils.query_processor_id:
                    self.nifi_utils.update_query_processor(
                        self.nifi_utils.query_processor_id, 
                        nueva_consulta,
                        clear_table_name=True
                    )
            else:
                # Consulta regular para otros meses
                nueva_consulta = f"""/* CONSULTA_MES_{mes}_{int(time.time())} */
SELECT 
    fecha, 
    hora, 
    alimentador, 
    dia_semana, 
    potencia_activa AS potencia_activa_real, 
    potencia_reactiva AS potencia_reactiva_real
FROM {tabla_trabajo}
WHERE EXTRACT(MONTH FROM fecha) = {mes}
AND EXTRACT(YEAR FROM fecha) = {ano}
ORDER BY fecha, hora, alimentador
"""
                # Actualizar el procesador con la consulta
                if hasattr(self.nifi_utils, 'query_processor_id') and self.nifi_utils.query_processor_id:
                    actualizado = self.nifi_utils.update_query_processor(
                        self.nifi_utils.query_processor_id, 
                        nueva_consulta,
                        clear_table_name=True
                    )
                    
                    if not actualizado:
                        logger.error(f"❌ Error actualizando consulta para mes {mes}")
                        pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                        return False
                        
                logger.info(f"✅ Consulta SQL configurada para usar tabla de trabajo {tabla_trabajo}")
            
            # 8. Limpiar datos existentes en destino
            logger.info("🗑️ Limpiando datos existentes en destino...")
            self._limpiar_datos_existentes_en_destino(mes, ano)
            
            # 9. Configurar topic Kafka único
            topic_name = f"potencia-mes{mes}-seguro-{int(time.time())}"
            logger.info(f"🔄 Configurando topic Kafka único: {topic_name}")
            
            # Configurar tópico en los procesadores adecuados
            if hasattr(self.nifi_utils, 'publish_kafka_processor_id') and self.nifi_utils.publish_kafka_processor_id:
                self.nifi_utils.configure_kafka_topic(
                    self.nifi_utils.publish_kafka_processor_id, 
                    "Topic Name", 
                    topic_name
                )
                
            if hasattr(self.nifi_utils, 'consume_kafka_processor_id') and self.nifi_utils.consume_kafka_processor_id:
                self.nifi_utils.configure_kafka_topic(
                    self.nifi_utils.consume_kafka_processor_id, 
                    "topic", 
                    topic_name
                )
            
            # 10. Limpiar estado y colas
            logger.info("🧹 Limpiando estado del procesador y colas...")
            self.nifi_utils.clear_processor_state_and_queues(
                self.nifi_utils.producer_group_id,
                self.nifi_utils.consumer_group_id,
                processor_ids=[self.nifi_utils.query_processor_id] if hasattr(self.nifi_utils, 'query_processor_id') else []
            )
            
            # 11. Iniciar procesadores
            logger.info("▶️ Iniciando grupos de procesadores completos...")
            if hasattr(self.nifi_utils, 'iniciar_todos_procesadores_por_grupo'):
                self.nifi_utils.iniciar_todos_procesadores_por_grupo()
            
            return True
                
        except Exception as e:
            logger.error(f"❌ Error procesando mes {mes}: {str(e)}")
            
            # Intentar limpiar la tabla de trabajo en caso de error
            try:
                pg_hook = PostgresHook(postgres_conn_id=conexion_origen)
                if 'tabla_trabajo' in locals():  # Verificar si la variable fue definida
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_trabajo}")
                    logger.info(f"🧹 Eliminada tabla de trabajo {tabla_trabajo} después de error")
                
                # Si estamos procesando diciembre, restaurar la tabla original en caso de error
                if mes == 12 and 'backup_tabla' in locals():
                    logger.info(f"🔄 Restaurando tabla {tabla_origen} desde backup {backup_tabla}...")
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_origen}")
                    pg_hook.run(f"ALTER TABLE {backup_tabla} RENAME TO {tabla_origen}")
                    logger.info(f"✅ Tabla {tabla_origen} restaurada desde backup")
                
            except Exception as cleanup_error:
                logger.error(f"⚠️ Error adicional durante limpieza: {str(cleanup_error)}")
                
            return False
    
def _limpiar_datos_existentes_en_destino(self, mes: int, ano: int = 2019, 
                                           tabla_destino: str = "potencia_dep_processed",
                                           conexion_destino: str = "postgres_base_prueba") -> bool:
        """
        Elimina los datos existentes del mes específico en la tabla destino.
        
        Args:
            mes: Número de mes a limpiar (1-12)
            ano: Año a limpiar
            tabla_destino: Nombre de la tabla destino
            conexion_destino: Nombre de la conexión a la base de datos destino
            
        Returns:
            bool: True si se limpiaron correctamente
        """
        try:
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"

            logger.info(f"🗑️ Limpiando datos existentes en destino para {fecha_inicio} a {fecha_fin}")
            pg_hook_destino = PostgresHook(postgres_conn_id=conexion_destino)

            # Delete existing records for this month
            delete_query = f"""
            DELETE FROM {tabla_destino} 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """

            pg_hook_destino.run(delete_query)
            logger.info(f"✅ Eliminados datos del mes {mes} en tabla destino")
            return True

        except Exception as e:
            logger.error(f"❌ Error al eliminar datos del mes {mes}: {str(e)}")
            return False
    
def restaurar_tabla_original(self, tabla_origen: str = "potencia_dep", 
                               tabla_original: str = "potencia_dep_original",
                               conexion_origen: str = "postgres_centrosur", 
                               **kwargs) -> str:
        """
        Restaura la tabla original de manera segura y completa.
        
        Args:
            tabla_origen: Nombre de la tabla a restaurar
            tabla_original: Nombre de la tabla respaldo original
            conexion_origen: Nombre de la conexión a la base de datos
            **kwargs: Argumentos adicionales
            
        Returns:
            str: Mensaje con el resultado de la restauración
        """
        try:
            logger.info(f"🔄 Restaurando tabla original {tabla_origen}...")
            pg_hook = PostgresHook(postgres_conn_id=conexion_origen)
            
            # NUEVA PROTECCIÓN: Asegurarse que la tabla original existe
            backup_exists = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_original}')"
            )[0]
            
            if not backup_exists:
                logger.warning(f"⚠️ ADVERTENCIA CRÍTICA: No existe tabla {tabla_original}! Abortando restauración.")
                return "SEGURIDAD: Restauración abortada porque no existe tabla original"
                
            # 1. Buscar tablas temporales previas
            query_tablas_temp = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE '{tabla_origen}_prev_%' 
            ORDER BY table_name DESC
            LIMIT 1
            """
            
            # Obtener la tabla temporal más reciente para restaurar
            tablas_temp = pg_hook.get_records(query_tablas_temp)
            
            if tablas_temp and len(tablas_temp) > 0:
                temp_original = tablas_temp[0][0]
                logger.info(f"🔍 Encontrada tabla temporal más reciente: {temp_original}")
                
                # Verificar si la tabla original existe y eliminarla si es necesario
                tabla_existe = pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_origen}')"
                )[0]
                
                if tabla_existe:
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_origen}")
                    logger.info(f"🗑️ Tabla {tabla_origen} actual eliminada")
                    
                # Restaurar desde la tabla temporal
                pg_hook.run(f"ALTER TABLE {temp_original} RENAME TO {tabla_origen}")
                logger.info(f"✅ Tabla original restaurada desde {temp_original}")
                
                # Eliminar otras tablas temporales
                pg_hook.run(f"""
                    DO $$ 
                    DECLARE 
                        tbl text;
                    BEGIN
                        FOR tbl IN 
                            SELECT table_name 
                            FROM information_schema.tables 
                            WHERE table_name LIKE '{tabla_origen}_temp_%' 
                               OR (table_name LIKE '{tabla_origen}_prev_%' AND table_name != '{temp_original}')
                        LOOP
                            EXECUTE 'DROP TABLE IF EXISTS ' || tbl;
                        END LOOP;
                    END $$;
                """)
                logger.info("🧹 Otras tablas temporales eliminadas")
                
                return f"Tabla restaurada desde {temp_original}"
            else:
                # Si no hay tablas temporales, verificar respaldo original
                backup_exists = pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_original}')"
                )[0]
                
                if backup_exists:
                    # Recrear desde respaldo original
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla_origen}")
                    pg_hook.run(f"CREATE TABLE {tabla_origen} AS SELECT * FROM {tabla_original}")
                    logger.info("✅ Tabla recreada desde respaldo original")
                    return "Tabla recreada desde respaldo"
                else:
                    logger.warning("⚠️ No se encontraron tablas temporales ni respaldo original")
                    return "No hay tablas para restaurar"
        except Exception as e:
            logger.error(f"❌ Error al restaurar tabla: {str(e)}")
            return f"Error: {str(e)}"
