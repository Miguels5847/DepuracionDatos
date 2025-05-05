import time
import logging
import psycopg2
import datetime
import calendar
from typing import Dict, List, Optional, Tuple, Union, Any
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow import settings

logger = logging.getLogger(__name__)

class DbUtils:
    """Clase para gestionar operaciones de bases de datos."""
    
    def __init__(self, origen_conn_id: str = "postgres_centrosur", 
                destino_conn_id: str = "postgres_base_prueba"):
        """
        Inicializa la clase DbUtils.
        
        Args:
            origen_conn_id: ID de la conexión a la base de datos origen
            destino_conn_id: ID de la conexión a la base de datos destino
        """
        self.origen_conn_id = origen_conn_id
        self.destino_conn_id = destino_conn_id
    
    def create_postgres_connections(self) -> bool:
        """
        Crea o actualiza las conexiones PostgreSQL requeridas.
        
        Returns:
            bool: True si se crearon/actualizaron correctamente
        """
        try:
            logger.info("Creando/actualizando conexiones PostgreSQL...")
            
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
                    "host": "postgres_centrosur",  # Mismo host
                    "login": "postgres",
                    "password": "1150040812",
                    "schema": "base_prueba",  # Base de datos diferente
                    "port": 5432,
                },
            ]
            
            session = settings.Session()
            for conn_info in connections:
                conn = Connection(**conn_info)
                existing_conn = (
                    session.query(Connection)
                    .filter(Connection.conn_id == conn.conn_id)
                    .first()
                )
                if existing_conn:
                    session.delete(existing_conn)
                    session.commit()
                session.add(conn)
            session.commit()
            session.close()
            
            logger.info("✅ Conexiones PostgreSQL creadas/actualizadas correctamente")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error creando conexiones: {str(e)}")
            return False
    
    def verificar_y_restaurar_tabla_original(self, 
                                           tabla_origen: str = "potencia_dep",
                                           tabla_original: str = "potencia_dep_original") -> str:
        """
        Verifica el estado de la tabla original y la restaura desde el respaldo si es necesario.
        
        Args:
            tabla_origen: Nombre de la tabla origen
            tabla_original: Nombre de la tabla respaldo original
            
        Returns:
            str: Mensaje con el resultado de la verificación
        """
        try:
            logger.info(f"🔍 Verificando integridad de la tabla original {tabla_origen}...")
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)

            # 1. Verificar si existe el respaldo original
            respaldo_existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_original}')"
            )[0]

            if not respaldo_existe:
                logger.warning(
                    f"⚠️ No se encontró respaldo {tabla_original}. Se creará si existe la tabla original."
                )

                # Verificar si existe la tabla original
                original_existe = pg_hook.get_first(
                    f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_origen}')"
                )[0]

                if original_existe:
                    logger.info(f"📋 Creando respaldo inicial {tabla_original}...")
                    pg_hook.run(
                        f"CREATE TABLE {tabla_original} AS SELECT * FROM {tabla_origen}"
                    )
                    logger.info("✅ Respaldo inicial creado exitosamente")
                    return "Respaldo inicial creado"
                else:
                    logger.error(
                        "❌ ERROR CRÍTICO: No existe ni la tabla original ni el respaldo!"
                    )
                    return "Error: No existen tablas necesarias"

            # 2. Verificar si existe la tabla original
            original_existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_origen}')"
            )[0]

            if not original_existe:
                logger.warning(f"⚠️ Tabla {tabla_origen} no existe. Restaurando desde respaldo...")
                pg_hook.run(
                    f"CREATE TABLE {tabla_origen} AS SELECT * FROM {tabla_original}"
                )
                logger.info(f"✅ Tabla {tabla_origen} restaurada desde respaldo")
                return "Tabla restaurada desde respaldo"

            # 3. Verificar integridad comparando conteos
            count_original = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_origen}")[0]
            count_backup = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_original}")[0]

            logger.info(f"📊 Conteo en {tabla_origen}: {count_original}")
            logger.info(f"📊 Conteo en {tabla_original}: {count_backup}")

            # Si hay una gran diferencia, restaurar desde respaldo
            if count_original < count_backup * 0.9:  # Si tiene menos del 90% de registros
                logger.warning(
                    f"⚠️ Tabla {tabla_origen} parece inconsistente ({count_original} vs {count_backup})"
                )
                logger.info("🔄 Restaurando tabla completa desde respaldo...")

                # Backup de la tabla actual por si acaso
                backup_nombre = f"{tabla_origen}_backup_{int(time.time())}"
                pg_hook.run(f"CREATE TABLE {backup_nombre} AS SELECT * FROM {tabla_origen}")
                logger.info(f"📋 Se creó respaldo adicional: {backup_nombre}")

                # Restaurar desde respaldo original
                pg_hook.run(f"DROP TABLE {tabla_origen}")
                pg_hook.run(
                    f"CREATE TABLE {tabla_origen} AS SELECT * FROM {tabla_original}"
                )
                logger.info(f"✅ Tabla {tabla_origen} restaurada completamente desde respaldo")
                return "Tabla restaurada por inconsistencia"

            # 4. Limpieza de tablas temporales antiguas
            query_tablas_temp = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE '{tabla_origen}_prev_%' OR table_name LIKE '{tabla_origen}_temp_%'
            """
            tablas_temp = pg_hook.get_records(query_tablas_temp)

            if tablas_temp:
                logger.info(f"🧹 Limpiando {len(tablas_temp)} tablas temporales antiguas...")
                for tabla in tablas_temp:
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla[0]}")
                logger.info("✅ Limpieza de tablas temporales completada")

            return "Verificación completada: Tabla original íntegra"

        except Exception as e:
            logger.error(f"❌ Error verificando tabla original: {str(e)}")
            return f"Error: {str(e)}"
    
    def restaurar_estado_original(self,
                                 tabla_origen: str = "potencia_dep",
                                 tabla_original: str = "potencia_dep_original",
                                 tabla_destino: str = "potencia_dep_processed") -> str:
        """
        Asegura que la base de datos vuelva a su estado original, eliminando tablas temporales.
        
        Args:
            tabla_origen: Nombre de la tabla origen
            tabla_original: Nombre de la tabla respaldo original
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            str: Mensaje con el resultado de la restauración
        """
        try:
            logger.info("🔄 Verificando y restaurando estado original de la base de datos...")
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            # 1. Verificar existencia de tablas
            origen_existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_origen}')"
            )[0]
            
            original_existe = pg_hook.get_first(
                f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_original}')"
            )[0]
            
            # 2. RESTAURACIÓN FINAL: Eliminar respaldo original y mantener solo tabla principal
            if origen_existe and original_existe:
                # Contar registros para comparación
                count_origen = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_origen}")[0]
                count_original = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_original}")[0]
                
                logger.info(f"📊 Registros en {tabla_origen}: {count_origen}")
                logger.info(f"📊 Registros en {tabla_original}: {count_original}")
                
                # PASO FINAL: Eliminar tabla original como solicitado
                pg_hook.run(f"DROP TABLE IF EXISTS {tabla_original}")
                logger.info(f"✅ TABLA {tabla_original} ELIMINADA - Restauración completada")
            
            # 3. Buscar y eliminar todas las tablas temporales
            query_tablas_temp = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE (table_name LIKE '{tabla_origen}_%') 
            AND table_name != '{tabla_origen}' AND table_name != '{tabla_destino}'
            """
            
            tablas_temp = pg_hook.get_records(query_tablas_temp)
            
            if tablas_temp:
                logger.info(f"🧹 Eliminando {len(tablas_temp)} tablas temporales...")
                for tabla in tablas_temp:
                    nombre_tabla = tabla[0]
                    pg_hook.run(f"DROP TABLE IF EXISTS {nombre_tabla}")
                    logger.info(f"  🗑️ Eliminada tabla: {nombre_tabla}")
            
            # 4. Verificar estado final
            if origen_existe:
                count_final = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_origen}")[0]
                logger.info(f"📊 ESTADO FINAL: Tabla {tabla_origen} contiene {count_final} registros")
            
            return f"Base de datos restaurada a estado original. Eliminadas {len(tablas_temp) if tablas_temp else 0} tablas temporales."
            
        except Exception as e:
            logger.error(f"❌ Error al restaurar estado original: {str(e)}")
            return f"Error: {str(e)}"
    
    def limpiar_tablas_temporales(self, 
                                 tabla_origen: str = "potencia_dep",
                                 tabla_destino: str = "potencia_dep_processed",
                                 **kwargs) -> str:
        """
        Limpia todas las tablas temporales creadas durante el procesamiento.
        
        Args:
            tabla_origen: Nombre de la tabla origen
            tabla_destino: Nombre de la tabla destino
            **kwargs: Argumentos adicionales
            
        Returns:
            str: Mensaje con el resultado de la limpieza
        """
        try:
            logger.info("🧹 Limpiando tablas temporales...")
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            # 1. Recuperar nombres de tablas de trabajo registradas en XComs
            ti = kwargs.get('ti')
            tablas_trabajo = []
            
            if ti:
                for mes in range(1, 13):
                    tabla = ti.xcom_pull(key=f'tabla_trabajo_mes_{mes}')
                    if tabla:
                        tablas_trabajo.append(tabla)
            
            # 2. Buscar todas las tablas temporales en la base de datos
            query_tablas_temp = f"""
            SELECT table_name FROM information_schema.tables 
            WHERE table_name LIKE '{tabla_origen}_work_%' 
               OR table_name LIKE '{tabla_origen}_temp_%'
               OR table_name LIKE '{tabla_origen}_prev_%'
               OR table_name LIKE '{tabla_origen}_backup_%'
            """
            
            tablas_db = pg_hook.get_records(query_tablas_temp)
            tablas_a_eliminar = set(tablas_trabajo)  # Uso set para evitar duplicados
            
            for tabla in tablas_db:
                nombre_tabla = tabla[0]
                # Proteger las tablas críticas 
                if nombre_tabla not in [tabla_origen, f'{tabla_origen}_original', tabla_destino]:
                    tablas_a_eliminar.add(nombre_tabla)
            
            # 3. Eliminar todas las tablas identificadas
            count = 0
            for tabla in tablas_a_eliminar:
                try:
                    pg_hook.run(f"DROP TABLE IF EXISTS {tabla}")
                    logger.info(f"🗑️ Eliminada tabla temporal: {tabla}")
                    count += 1
                except Exception as e:
                    logger.warning(f"⚠️ Error al eliminar tabla {tabla}: {str(e)}")
            
            return f"Limpiadas {count} tablas temporales"
            
        except Exception as e:
            logger.error(f"⚠️ Error en limpieza de tablas: {str(e)}")
            return f"Error en limpieza: {str(e)}"
    
    def verificar_si_mes_ya_procesado(self, mes: int, ano: int = 2019, 
                                     tabla_destino: str = "potencia_dep_processed") -> bool:
        """
        Verifica si un mes específico ya tiene datos procesados en la tabla destino.
        
        Args:
            mes: Número de mes a verificar (1-12)
            ano: Año a verificar
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            bool: True si ya hay datos procesados
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # Construir consulta para verificar existencia de datos
            query = f"""
            SELECT COUNT(*) FROM {tabla_destino} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            count = pg_hook.get_first(query)[0]
            
            if count > 0:
                logger.info(f"✅ El mes {mes} del año {ano} ya tiene {count} registros procesados")

                # Mostrar una muestra para verificar la calidad de los datos
                sample_query = f"""
                SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {tabla_destino} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                ORDER BY RANDOM()
                LIMIT 2
                """
                samples = pg_hook.get_records(sample_query)
                logger.info("📊 Muestra de registros existentes:")
                for sample in samples:
                    logger.info(f"  {sample}")

                return True
            else:
                logger.info(f"⚠️ No se encontraron datos del mes {mes} año {ano}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en verificación: {str(e)}")
            return False
    
    def truncate_existing_month_data(self, mes: int, ano: int = 2019,
                                   tabla_destino: str = "potencia_dep_processed") -> bool:
        """
        Elimina los datos existentes del mes en la tabla destino.
        
        Args:
            mes: Número de mes a limpiar (1-12)
            ano: Año a limpiar
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            bool: True si se eliminaron correctamente
        """
        try:
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"

            logger.info(f"🗑️ Limpiando datos existentes en destino para {fecha_inicio} a {fecha_fin}")
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # Eliminar registros existentes para este mes
            delete_query = f"""
            DELETE FROM {tabla_destino} 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            """

            pg_hook.run(delete_query)
            logger.info(f"✅ Eliminados datos del mes {mes} en tabla destino")
            return True

        except Exception as e:
            logger.error(f"❌ Error al eliminar datos del mes {mes}: {str(e)}")
            return False
    
    def crear_tabla_temporal_para_mes(self, mes: int, ano: int = 2019,
                                    tabla_origen: str = "potencia_dep") -> Tuple[bool, Optional[str]]:
        """
        Crea una tabla temporal con datos filtrados para un mes específico.
        
        Args:
            mes: Número de mes (1-12)
            ano: Año 
            tabla_origen: Nombre de la tabla origen
            
        Returns:
            Tuple[bool, Optional[str]]: (éxito, nombre_tabla_temporal)
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            # Cálculo de fechas
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            # Nombre único para la tabla temporal
            tabla_temporal = f"{tabla_origen}_work_{int(time.time())}"
            logger.info(f"📋 Creando tabla temporal {tabla_temporal} para mes {mes}...")
            
            # Eliminar tabla si existe
            pg_hook.run(f"DROP TABLE IF EXISTS {tabla_temporal}")
            
            # Verificar registros existentes para este mes
            query_verificar = f"""
                SELECT COUNT(*) FROM {tabla_origen} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            count = pg_hook.get_first(query_verificar)[0]
            
            if count == 0:
                logger.warning(f"⚠️ No hay datos para el mes {mes} en {tabla_origen}")
                return False, None
            
            logger.info(f"✅ Encontrados {count} registros en {tabla_origen} para el mes {mes}")
            
            # Crear tabla temporal filtrada
            create_query = f"""
                CREATE TABLE {tabla_temporal} AS
                SELECT * FROM {tabla_origen} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                AND fecha >= '{fecha_inicio}'
                AND fecha < '{fecha_fin}'
            """
            
            pg_hook.run(create_query)
            
            # Verificar creación correcta
            count_temporal = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_temporal}")[0]
            logger.info(f"✅ Tabla temporal {tabla_temporal} creada con {count_temporal} registros")
            
            return True, tabla_temporal
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla temporal para mes {mes}: {str(e)}")
            return False, None
    
    def verificar_datos_procesados_por_mes(self, mes: int, ano: int = 2019,
                                         tabla_destino: str = "potencia_dep_processed") -> bool:
        """
        Verifica que se hayan procesado correctamente los datos de un mes específico.
        
        Args:
            mes: Número de mes a verificar (1-12)
            ano: Año a verificar
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            bool: True si hay datos procesados correctamente
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # Consulta para verificar datos
            query = f"""
            SELECT COUNT(*) FROM {tabla_destino} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            count = pg_hook.get_first(query)[0]
            
            if count > 0:
                logger.info(f"✅ Verificación exitosa: {count} registros del mes {mes} año {ano}")
                
                # Mostrar algunos ejemplos para validar
                sample_query = f"""
                SELECT fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {tabla_destino} 
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                LIMIT 5
                """
                samples = pg_hook.get_records(sample_query)
                logger.info("📊 Muestra de registros procesados:")
                for sample in samples:
                    logger.info(f"  {sample}")
                    
                return True
            else:
                logger.warning(f"⚠️ No se encontraron datos del mes {mes} año {ano}")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error en verificación de datos procesados: {str(e)}")
            return False

    def generar_estadisticas_origen(self, 
                              tabla_origen: str = "potencia_dep",
                              ano: int = 2019) -> Dict[str, Any]:
        """
        Genera estadísticas detalladas de la tabla origen para el análisis de completitud.

        Args:
            tabla_origen: Nombre de la tabla origen
            ano: Año a analizar
        
        Returns:
            Dict[str, Any]: Estadísticas detalladas por mes y general
        """
        try:
            logger.info(f"📊 Generando estadísticas de tabla origen {tabla_origen} para año {ano}...")
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)

            # Estadísticas generales para todo el año
            query_general = f"""
            SELECT 
                COUNT(*) as total_registros,
                COUNT(DISTINCT fecha) as total_dias,
                COUNT(DISTINCT alimentador) as total_alimentadores,
                MIN(fecha) as fecha_min,
                MAX(fecha) as fecha_max
            FROM {tabla_origen}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
        
            stats_general = pg_hook.get_first(query_general)
        
            if not stats_general or stats_general[0] == 0:
                logger.warning(f"⚠️ No hay datos para el año {ano} en {tabla_origen}")
                return {
                    "error": True,
                    "mensaje": f"No hay datos para el año {ano}"
                }
        
            total_registros, total_dias, total_alimentadores, fecha_min, fecha_max = stats_general

            # Estadísticas por mes
            stats_por_mes = []
            for mes in range(1, 13):
                query_mes = f"""
                SELECT 
                    COUNT(*) as registros,
                    COUNT(DISTINCT fecha) as dias,
                    COUNT(DISTINCT alimentador) as alimentadores,
                    MIN(fecha) as fecha_min,
                    MAX(fecha) as fecha_max
                FROM {tabla_origen}
                WHERE EXTRACT(YEAR FROM fecha) = {ano}
                AND EXTRACT(MONTH FROM fecha) = {mes}
                """
            
                stats_mes = pg_hook.get_first(query_mes)

                if stats_mes and stats_mes[0] > 0:
                    registros, dias, alimentadores, mes_fecha_min, mes_fecha_max = stats_mes

                    # Calcular días esperados en el mes
                    primer_dia = datetime.date(ano, mes, 1)
                    if mes == 12:
                        ultimo_dia = datetime.date(ano + 1, 1, 1) - datetime.timedelta(days=1)
                    else:
                        ultimo_dia = datetime.date(ano, mes + 1, 1) - datetime.timedelta(days=1)
                    dias_esperados = (ultimo_dia - primer_dia).days + 1
                
                    completitud_dias = (dias / dias_esperados * 100) if dias_esperados > 0 else 0

                    stats_por_mes.append({
                        "mes": mes,
                        "nombre_mes": calendar.month_name[mes],
                        "registros": registros,
                        "dias": dias,
                        "dias_esperados": dias_esperados,
                        "completitud_dias": completitud_dias,
                        "alimentadores": alimentadores,
                        "fecha_min": mes_fecha_min,
                        "fecha_max": mes_fecha_max,
                        "estado": "Completo" if completitud_dias >= 95 else "Incompleto"
                    })
                else:
                    stats_por_mes.append({
                        "mes": mes,
                        "nombre_mes": calendar.month_name[mes],
                        "registros": 0,
                        "dias": 0,
                        "dias_esperados": 0,
                        "completitud_dias": 0,
                        "alimentadores": 0,
                        "fecha_min": None,
                        "fecha_max": None,
                        "estado": "Sin datos"
                    })
        
            # Calcular días esperados en todo el año
            dias_en_ano = 366 if calendar.isleap(ano) else 365
            completitud_dias_anual = (total_dias / dias_en_ano * 100) if dias_en_ano > 0 else 0

            # Construir resultado final
            resultado = {
                "tabla_origen": tabla_origen,
                "ano": ano,
                "total_registros": total_registros,
                "total_dias": total_dias,
                "dias_esperados": dias_en_ano,
                "completitud_dias": completitud_dias_anual,
                "total_alimentadores": total_alimentadores,
                "fecha_inicio": fecha_min,
                "fecha_fin": fecha_max,
                "estado_general": "Completo" if completitud_dias_anual >= 95 else "Incompleto",
                "estadisticas_por_mes": stats_por_mes,
                "timestamp": datetime.datetime.now().isoformat()
            }
        
            logger.info(f"✅ Estadísticas generadas: {total_registros} registros, {total_dias}/{dias_en_ano} días, {total_alimentadores} alimentadores")
            return resultado
        
        except Exception as e:
            logger.error(f"❌ Error generando estadísticas: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error: {str(e)}"
            }
