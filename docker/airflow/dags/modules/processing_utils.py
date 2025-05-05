from typing import Dict, List, Optional, Tuple, Union, Any, Set
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class ProcessingUtils:
    """
    Clase para el procesamiento y depuración de datos de potencia.
    
    Implementa métodos para:
    - Detección de outliers (valores cero)
    - Corrección de datos basada en patrones
    - Análisis de calidad de datos por alimentador/mes
    """
    
    def __init__(self, 
                 origen_conn_id: str = "postgres_centrosur", 
                 destino_conn_id: str = "postgres_base_prueba",
                 tabla_origen: str = "potencia_dep",
                 tabla_destino: str = "potencia_dep_processed"):
        """
        Inicializa la clase ProcessingUtils.
        
        Args:
            origen_conn_id: ID de la conexión a la base de datos origen
            destino_conn_id: ID de la conexión a la base de datos destino
            tabla_origen: Nombre de la tabla origen
            tabla_destino: Nombre de la tabla destino
        """
        self.origen_conn_id = origen_conn_id
        self.destino_conn_id = destino_conn_id
        self.tabla_origen = tabla_origen
        self.tabla_destino = tabla_destino
    
    def corregir_datos_alimentador(self, alimentador: str, 
                                  ano: int = 2019) -> Tuple[bool, int, int]:
        """
        Corrige los datos de un alimentador específico para todo el año.

        Args:
            alimentador: Código del alimentador
            ano: Año a corregir

        Returns:
            Tuple (éxito, total_corregidos, total_procesados)
        """
        try:
            # 1. Obtener el mes de referencia
            mes_referencia = self.obtener_mes_referencia(alimentador, ano)

            if mes_referencia == 0:
                logger.warning(f"⚠️ No se puede corregir alimentador {alimentador}: sin mes de referencia")
                return False, 0, 0
        
            # 2. Obtener patrones de referencia por hora
            patrones = self.obtener_patrones_referencia(alimentador, mes_referencia, ano)
        
            if not patrones:
                logger.warning(f"⚠️ No se obtuvieron patrones para alimentador {alimentador}")
                return False, 0, 0
        
            # 3. Obtener datos originales
            pg_hook_origen = PostgresHook(postgres_conn_id=self.origen_conn_id)
            pg_hook_destino = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # Asegurar que la tabla de log existe
            self._crear_tabla_log_cambios()
        
            # 4. Procesar mes a mes
            total_corregidos = 0
            total_procesados = 0
        
            for mes in range(1, 13):
                # Si es el mes de referencia, copiar directamente
                if mes == mes_referencia:
                    self._copiar_mes_referencia(alimentador, mes, ano)
                    continue
            
                # Fecha inicio y fin del mes
                fecha_inicio = f"{ano}-{mes:02d}-01"
                if mes == 12:
                    fecha_fin = f"{ano+1}-01-01"
                else:
                    fecha_fin = f"{ano}-{mes+1:02d}-01"
            
                # Obtener datos del mes
                query = f"""
                SELECT id, fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {self.tabla_origen}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                AND alimentador = '{alimentador}'
                ORDER BY fecha, hora
                """
            
                registros = pg_hook_origen.get_records(query)

                # Procesar cada registro
                for id, fecha, hora, alim, p_activa, p_reactiva in registros:
                    total_procesados += 1
                
                    # Determinar si necesita corrección
                    necesita_correccion = False
                    nueva_p_activa = p_activa
                    nueva_p_reactiva = p_reactiva
                
                    # Corregir potencia activa si es cero
                    if p_activa == 0 and hora in patrones:
                        nueva_p_activa = patrones[hora]["potencia_activa"]
                        necesita_correccion = True

                        # Registrar cambio en tabla de log
                        self._registrar_cambio(
                            registro_id=id,
                            alimentador=alim,
                            fecha=fecha,
                            hora=hora,
                            campo_modificado='potencia_activa',
                            valor_anterior=p_activa,
                            valor_nuevo=nueva_p_activa,
                            mes_referencia=mes_referencia,
                            motivo_cambio='Valor cero corregido con patrón de referencia'
                        )
                
                    # Corregir potencia reactiva si es cero
                    if p_reactiva == 0 and hora in patrones:
                        nueva_p_reactiva = patrones[hora]["potencia_reactiva"]
                        necesita_correccion = True

                        # Registrar cambio en tabla de log
                        self._registrar_cambio(
                            registro_id=id,
                            alimentador=alim,
                            fecha=fecha,
                            hora=hora,
                            campo_modificado='potencia_reactiva',
                            valor_anterior=p_reactiva,
                            valor_nuevo=nueva_p_reactiva,
                            mes_referencia=mes_referencia,
                            motivo_cambio='Valor cero corregido con patrón de referencia'
                        )
                
                    # Si necesita corrección, insertar en tabla destino
                    if necesita_correccion:
                        # Insertar registro corregido
                        query_insert = f"""
                        INSERT INTO {self.tabla_destino} 
                        (id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, procesado)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, TRUE)
                        """
                    
                        pg_hook_destino.run(
                            query_insert, 
                            parameters=(id, fecha, hora, alim, nueva_p_activa, nueva_p_reactiva)
                        )
                    
                        total_corregidos += 1
                    else:
                        # Si no necesita corrección, copiar tal cual
                        query_insert = f"""
                        INSERT INTO {self.tabla_destino} 
                        (id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, procesado)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, FALSE)
                        """
                    
                        pg_hook_destino.run(
                            query_insert, 
                            parameters=(id, fecha, hora, alim, p_activa, p_reactiva)
                        )
        
            logger.info(f"✅ Procesado alimentador {alimentador}: {total_corregidos}/{total_procesados} registros corregidos")
            return True, total_corregidos, total_procesados
        
        except Exception as e:
            logger.error(f"❌ Error corrigiendo datos del alimentador {alimentador}: {str(e)}")
            return False, 0, 0

    def obtener_resumen_cambios(self, ano: int = 2019) -> Dict[str, Any]:
        """
        Genera resumen de cambios realizados en el procesamiento.

        Args:
            ano: Año a analizar
        
        Returns:
            Dict con estadísticas de cambios
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # Verificar si existe la tabla de logs
            query_check = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'potencia_log_cambios'
            );
            """
            tabla_existe = pg_hook.get_first(query_check)[0]
        
            if not tabla_existe:
                logger.warning("⚠️ La tabla potencia_log_cambios no existe")
                return {
                    "ano": ano,
                    "error": "Tabla de logs no encontrada",
                    "total_registros_modificados": 0
                }
        
            # Obtener total de registros modificados
            query_total = f"""
            SELECT COUNT(DISTINCT registro_id) 
            FROM potencia_log_cambios 
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            total_modificados = pg_hook.get_first(query_total)[0]
        
            # Obtener detalle por alimentador y campo
            query_detalle = f"""
            SELECT alimentador, campo_modificado, COUNT(*) as total_cambios,
                AVG(valor_nuevo) as promedio_nuevo
            FROM potencia_log_cambios
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            GROUP BY alimentador, campo_modificado
            ORDER BY total_cambios DESC
            LIMIT 20
            """
        
            resultados = pg_hook.get_records(query_detalle)
        
            # Construir el resumen
            detalle = []
            for row in resultados:
                detalle.append({
                    "alimentador": row[0],
                    "campo": row[1],
                    "cambios": row[2],
                    "valor_promedio": float(row[3]) if row[3] else 0
                })
        
            resumen = {
                "ano": ano,
                "total_registros_modificados": total_modificados,
                "detalle_por_alimentador": detalle
            }
        
            logger.info(f"✅ Generado resumen de cambios para el año {ano}")
            return resumen
        
        except Exception as e:
            logger.error(f"❌ Error generando resumen de cambios: {str(e)}")
            return {
                "ano": ano,
                "error": str(e),
                "total_registros_modificados": 0
            }

    def _crear_tabla_log_cambios(self):
        """Crea la tabla de log de cambios si no existe."""
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # SQL para crear la tabla si no existe
            query = """
            CREATE TABLE IF NOT EXISTS potencia_log_cambios (
                id SERIAL PRIMARY KEY,
                fecha_proceso TIMESTAMP DEFAULT NOW(),
                registro_id INTEGER,
                alimentador VARCHAR(50),
                fecha DATE,
                hora INTEGER,
                campo_modificado VARCHAR(50),
                valor_anterior FLOAT,
                valor_nuevo FLOAT,
                mes_referencia INTEGER,
                motivo_cambio VARCHAR(200),
                dag_id VARCHAR(100),
                task_id VARCHAR(100)
            );
            """
        
            pg_hook.run(query)
            logger.info("✅ Tabla de log de cambios verificada/creada")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error creando tabla de log: {str(e)}")
            return False

    def _registrar_cambio(self, registro_id, alimentador, fecha, hora, 
                        campo_modificado, valor_anterior, valor_nuevo,
                        mes_referencia, motivo_cambio):
        """Registra un cambio en la tabla de log."""
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)

            # Obtener información del contexto de ejecución
            dag_id = 'depuracion_simplificada'  # Valor predeterminado
            task_id = 'procesar_datos_completo'  # Valor predeterminado
        
            try:
                # Intentar obtener contexto de Airflow si está disponible
                from airflow.hooks.base_hook import BaseHook
                from airflow.models import TaskInstance
                import os
            
                # Si estamos en contexto de Airflow, estos valores estarán disponibles
                if 'AIRFLOW_CTX_DAG_ID' in os.environ:
                    dag_id = os.environ.get('AIRFLOW_CTX_DAG_ID')
            
                if 'AIRFLOW_CTX_TASK_ID' in os.environ:
                    task_id = os.environ.get('AIRFLOW_CTX_TASK_ID')
            except:
                # Si falla, usar los valores predeterminados
                pass
        
            # Insertar en tabla de log
            query = """
            INSERT INTO potencia_log_cambios 
            (registro_id, alimentador, fecha, hora, campo_modificado, 
            valor_anterior, valor_nuevo, mes_referencia, motivo_cambio, dag_id, task_id)
            VALUES
            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
        
            pg_hook.run(
                query, 
                parameters=(
                    registro_id, alimentador, fecha, hora, campo_modificado,
                    valor_anterior, valor_nuevo, mes_referencia, motivo_cambio,
                    dag_id, task_id
                )
            )
        
            return True
        
        except Exception as e:
            logger.error(f"❌ Error registrando cambio en log: {str(e)}")
            return False

    def analizar_calidad_datos_por_mes(self, 
                                      alimentador: str, 
                                      ano: int = 2019) -> Dict[int, Dict[str, Any]]:
        """
        Analiza la calidad de datos por mes para un alimentador específico.
        
        Args:
            alimentador: Código del alimentador a analizar
            ano: Año a analizar (por defecto 2019)
            
        Returns:
            Dict con resultados de calidad por mes
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            # Resultados por mes
            resultados = {}
            
            for mes in range(1, 13):
                # Fecha inicio y fin del mes
                fecha_inicio = f"{ano}-{mes:02d}-01"
                # Último día del mes
                if mes == 12:
                    fecha_fin = f"{ano+1}-01-01"
                else:
                    fecha_fin = f"{ano}-{mes+1:02d}-01"
                
                # 1. Total de registros para el mes y alimentador
                query_total = f"""
                SELECT COUNT(*)
                FROM {self.tabla_origen}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                AND alimentador = '{alimentador}'
                """
                total_registros = pg_hook.get_first(query_total)[0]
                
                # 2. Registros con ambos valores en cero
                query_ceros = f"""
                SELECT COUNT(*)
                FROM {self.tabla_origen}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                AND alimentador = '{alimentador}'
                AND potencia_activa = 0 AND potencia_reactiva = 0
                """
                registros_ceros = pg_hook.get_first(query_ceros)[0]
                
                # 3. Registros con valores reales (no ambos en cero)
                registros_validos = total_registros - registros_ceros
                
                # 4. Calcular porcentajes
                if total_registros > 0:
                    porcentaje_validos = (registros_validos / total_registros) * 100
                    porcentaje_ceros = (registros_ceros / total_registros) * 100
                else:
                    porcentaje_validos = 0
                    porcentaje_ceros = 0
                
                # 5. Determinar si el mes es apto como referencia (>= 95% datos válidos)
                es_referencia = porcentaje_validos >= 95
                
                # Guardar resultados del mes
                resultados[mes] = {
                    "total_registros": total_registros,
                    "registros_validos": registros_validos,
                    "registros_ceros": registros_ceros,
                    "porcentaje_validos": porcentaje_validos,
                    "porcentaje_ceros": porcentaje_ceros,
                    "es_mes_referencia": es_referencia
                }
            
            logger.info(f"✅ Análisis de calidad para alimentador {alimentador} completado")
            return resultados
                
        except Exception as e:
            logger.error(f"❌ Error analizando calidad de datos: {str(e)}")
            return {}
    
    def identificar_alimentadores_ano(self, ano: int = 2019) -> List[str]:
        """
        Identifica todos los alimentadores presentes en un año específico.
        
        Args:
            ano: Año a analizar
            
        Returns:
            Lista de códigos de alimentadores
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            query = f"""
            SELECT DISTINCT alimentador
            FROM {self.tabla_origen}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            ORDER BY alimentador
            """
            
            resultados = pg_hook.get_records(query)
            alimentadores = [row[0] for row in resultados]
            
            logger.info(f"✅ Identificados {len(alimentadores)} alimentadores para el año {ano}")
            return alimentadores
            
        except Exception as e:
            logger.error(f"❌ Error identificando alimentadores: {str(e)}")
            return []
    
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
            
            # Buscar meses de referencia (>= 95% datos válidos)
            meses_referencia = []
            for mes, datos in analisis.items():
                if datos.get("es_mes_referencia", False):
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
    
    def obtener_patrones_referencia(self, alimentador: str, 
                                   mes_referencia: int, 
                                   ano: int = 2019) -> Dict[int, Dict[str, float]]:
        """
        Obtiene patrones de referencia por hora desde un mes específico.
        
        Args:
            alimentador: Código del alimentador
            mes_referencia: Mes a usar como referencia (1-12)
            ano: Año a analizar
            
        Returns:
            Dict con patrones por hora {hora: {potencia_activa: valor, potencia_reactiva: valor}}
        """
        try:
            if mes_referencia == 0:
                logger.warning(f"⚠️ No hay mes de referencia para alimentador {alimentador}")
                return {}
            
            pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
            
            # Fecha inicio y fin del mes de referencia
            fecha_inicio = f"{ano}-{mes_referencia:02d}-01"
            if mes_referencia == 12:
                fecha_fin = f"{ano+1}-01-01"
            else:
                fecha_fin = f"{ano}-{mes_referencia+1:02d}-01"
            
            # Obtener promedios por hora (excluyendo valores cero)
            query = f"""
            SELECT hora, 
                   AVG(CASE WHEN potencia_activa != 0 THEN potencia_activa ELSE NULL END) as prom_activa,
                   AVG(CASE WHEN potencia_reactiva != 0 THEN potencia_reactiva ELSE NULL END) as prom_reactiva
            FROM {self.tabla_origen}
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            AND alimentador = '{alimentador}'
            GROUP BY hora
            ORDER BY hora
            """
            
            resultados = pg_hook.get_records(query)
            
            # Convertir a diccionario de patrones
            patrones = {}
            for hora, prom_activa, prom_reactiva in resultados:
                patrones[hora] = {
                    "potencia_activa": float(prom_activa) if prom_activa is not None else 0,
                    "potencia_reactiva": float(prom_reactiva) if prom_reactiva is not None else 0
                }
            
            logger.info(f"✅ Patrones de referencia obtenidos del mes {mes_referencia} para alimentador {alimentador}")
            return patrones
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo patrones de referencia: {str(e)}")
            return {}
    def generar_resumen_cambios(ano):
        """Genera resumen de cambios realizados"""
        pg_hook = PostgresHook(postgres_conn_id="postgres_base_prueba")
    
        query = f"""
        SELECT alimentador, campo_modificado, COUNT(*) as total_cambios,
            AVG(valor_nuevo) as promedio_nuevo
        FROM potencia_log_cambios
        WHERE EXTRACT(YEAR FROM fecha) = {ano}
        GROUP BY alimentador, campo_modificado
        ORDER BY total_cambios DESC
        LIMIT 20
        """
    
        resultados = pg_hook.get_records(query)
    
        resumen = {
            "ano": ano,
            "total_registros_modificados": len(pg_hook.get_records(
                f"SELECT COUNT(DISTINCT registro_id) FROM potencia_log_cambios WHERE EXTRACT(YEAR FROM fecha) = {ano}"
            )[0][0]),
            "detalle_por_alimentador": [{
                "alimentador": row[0],
                "campo": row[1], 
                "cambios": row[2],
                "valor_promedio": float(row[3])
            } for row in resultados]
        }
    
        return resumen
    def corregir_datos_alimentador(self, alimentador: str, 
                                  ano: int = 2019) -> Tuple[bool, int, int]:
        """
        Corrige los datos de un alimentador específico para todo el año.
        
        Args:
            alimentador: Código del alimentador
            ano: Año a corregir
            
        Returns:
            Tuple (éxito, total_corregidos, total_procesados)
        """
        try:
            # 1. Obtener el mes de referencia
            mes_referencia = self.obtener_mes_referencia(alimentador, ano)
            
            if mes_referencia == 0:
                logger.warning(f"⚠️ No se puede corregir alimentador {alimentador}: sin mes de referencia")
                return False, 0, 0
            
            # 2. Obtener patrones de referencia por hora
            patrones = self.obtener_patrones_referencia(alimentador, mes_referencia, ano)
            
            if not patrones:
                logger.warning(f"⚠️ No se obtuvieron patrones para alimentador {alimentador}")
                return False, 0, 0
            
            # 3. Obtener datos originales
            pg_hook_origen = PostgresHook(postgres_conn_id=self.origen_conn_id)
            pg_hook_destino = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # 4. Procesar mes a mes
            total_corregidos = 0
            total_procesados = 0
            
            for mes in range(1, 13):
                # Si es el mes de referencia, copiar directamente
                if mes == mes_referencia:
                    self._copiar_mes_referencia(alimentador, mes, ano)
                    continue
                
                # Fecha inicio y fin del mes
                fecha_inicio = f"{ano}-{mes:02d}-01"
                if mes == 12:
                    fecha_fin = f"{ano+1}-01-01"
                else:
                    fecha_fin = f"{ano}-{mes+1:02d}-01"
                
                # Obtener datos del mes
                query = f"""
                SELECT id, fecha, hora, alimentador, potencia_activa, potencia_reactiva
                FROM {self.tabla_origen}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                AND alimentador = '{alimentador}'
                ORDER BY fecha, hora
                """
                
                registros = pg_hook_origen.get_records(query)
                
                # Procesar cada registro
                for id, fecha, hora, alim, p_activa, p_reactiva in registros:
                    total_procesados += 1
                    
                    # Determinar si necesita corrección
                    necesita_correccion = False
                    nueva_p_activa = p_activa
                    nueva_p_reactiva = p_reactiva
                    
                    # Corregir potencia activa si es cero
                    if p_activa == 0 and hora in patrones:
                        nueva_p_activa = patrones[hora]["potencia_activa"]
                        necesita_correccion = True
                    
                    # Corregir potencia reactiva si es cero
                    if p_reactiva == 0 and hora in patrones:
                        nueva_p_reactiva = patrones[hora]["potencia_reactiva"]
                        necesita_correccion = True
                    
                    # Si necesita corrección, insertar en tabla destino
                    if necesita_correccion:
                        # Insertar registro corregido
                        query_insert = f"""
                        INSERT INTO {self.tabla_destino} 
                        (id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, procesado)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, TRUE)
                        """
                        
                        pg_hook_destino.run(
                            query_insert, 
                            parameters=(id, fecha, hora, alim, nueva_p_activa, nueva_p_reactiva)
                        )
                        
                        total_corregidos += 1
                    else:
                        # Si no necesita corrección, copiar tal cual
                        query_insert = f"""
                        INSERT INTO {self.tabla_destino} 
                        (id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, procesado)
                        VALUES
                        (%s, %s, %s, %s, %s, %s, FALSE)
                        """
                        
                        pg_hook_destino.run(
                            query_insert, 
                            parameters=(id, fecha, hora, alim, p_activa, p_reactiva)
                        )
            
            logger.info(f"✅ Procesado alimentador {alimentador}: {total_corregidos}/{total_procesados} registros corregidos")
            return True, total_corregidos, total_procesados
            
        except Exception as e:
            logger.error(f"❌ Error corrigiendo datos del alimentador {alimentador}: {str(e)}")
            return False, 0, 0
    
    def _copiar_mes_referencia(self, alimentador: str, mes: int, ano: int) -> int:
        """
        Copia directamente los datos del mes de referencia a la tabla destino.
        
        Args:
            alimentador: Código del alimentador
            mes: Mes de referencia
            ano: Año
            
        Returns:
            int: Cantidad de registros copiados
        """
        try:
            # Fecha inicio y fin del mes
            fecha_inicio = f"{ano}-{mes:02d}-01"
            if mes == 12:
                fecha_fin = f"{ano+1}-01-01"
            else:
                fecha_fin = f"{ano}-{mes+1:02d}-01"
            
            # Copiar datos directamente
            pg_hook_origen = PostgresHook(postgres_conn_id=self.origen_conn_id)
            pg_hook_destino = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            query = f"""
            INSERT INTO {self.tabla_destino}
            (id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, procesado)
            SELECT id, fecha, hora, alimentador, potencia_activa, potencia_reactiva, FALSE
            FROM {self.tabla_origen}
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            AND alimentador = '{alimentador}'
            """
            
            pg_hook_destino.run(query)
            
            # Contar registros copiados
            query_count = f"""
            SELECT COUNT(*)
            FROM {self.tabla_origen}
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            AND alimentador = '{alimentador}'
            """
            
            count = pg_hook_origen.get_first(query_count)[0]
            
            logger.info(f"✅ Copiados {count} registros del mes de referencia {mes} para alimentador {alimentador}")
            return count
            
        except Exception as e:
            logger.error(f"❌ Error copiando mes de referencia: {str(e)}")
            return 0
    
    def procesar_completo_ano(self, ano: int = 2019) -> Dict[str, Any]:
        """
        Procesa todos los alimentadores para un año completo.
        
        Args:
            ano: Año a procesar
            
        Returns:
            Dict con resumen del procesamiento
        """
        try:
            # 1. Limpiar tabla destino para el año
            self._limpiar_destino_para_ano(ano)
            
            # 2. Identificar todos los alimentadores del año
            alimentadores = self.identificar_alimentadores_ano(ano)
            
            if not alimentadores:
                return {
                    "exito": False,
                    "mensaje": f"No se encontraron alimentadores para el año {ano}"
                }
            
            # 3. Procesar cada alimentador
            resultados = {}
            total_corregidos = 0
            total_procesados = 0
            
            for alimentador in alimentadores:
                exito, corregidos, procesados = self.corregir_datos_alimentador(alimentador, ano)
                
                resultados[alimentador] = {
                    "exito": exito,
                    "corregidos": corregidos,
                    "procesados": procesados,
                    "porcentaje_corregidos": (corregidos / procesados * 100) if procesados > 0 else 0
                }
                
                total_corregidos += corregidos
                total_procesados += procesados
            
            # 4. Generar resumen
            resumen = {
                "exito": True,
                "ano": ano,
                "alimentadores_procesados": len(alimentadores),
                "total_registros_procesados": total_procesados,
                "total_registros_corregidos": total_corregidos,
                "porcentaje_global_corregidos": (total_corregidos / total_procesados * 100) if total_procesados > 0 else 0,
                "detalle_por_alimentador": resultados
            }
            
            logger.info(f"✅ Procesamiento completo del año {ano}: {total_corregidos}/{total_procesados} registros corregidos")
            return resumen
            
        except Exception as e:
            logger.error(f"❌ Error en procesamiento completo del año {ano}: {str(e)}")
            return {
                "exito": False,
                "mensaje": f"Error en procesamiento: {str(e)}"
            }
    
    def _limpiar_destino_para_ano(self, ano: int) -> bool:
        """
        Limpia los registros del año en la tabla destino.
        
        Args:
            ano: Año a limpiar
            
        Returns:
            bool: True si se realizó correctamente
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # Eliminar datos del año
            query = f"""
            DELETE FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            pg_hook.run(query)
            logger.info(f"✅ Limpieza de registros del año {ano} en tabla destino completada")
            return True
            
        except Exception as e:
            logger.error(f"❌ Error limpiando tabla destino: {str(e)}")
            return False
    
    def obtener_estadisticas_procesamiento(self, ano: int = 2019) -> Dict[str, Any]:
        """
        Obtiene estadísticas del procesamiento realizado.
        
        Args:
            ano: Año analizado
            
        Returns:
            Dict con estadísticas
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # 1. Total de registros procesados
            query_total = f"""
            SELECT COUNT(*)
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            total = pg_hook.get_first(query_total)[0]
            
            # 2. Total de registros corregidos
            query_corregidos = f"""
            SELECT COUNT(*)
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            AND procesado = TRUE
            """
            corregidos = pg_hook.get_first(query_corregidos)[0]
            
            # 3. Estadísticas por mes
            stats_por_mes = {}
            for mes in range(1, 13):
                query_mes = f"""
                SELECT COUNT(*),
                       SUM(CASE WHEN procesado = TRUE THEN 1 ELSE 0 END)
                FROM {self.tabla_destino}
                WHERE EXTRACT(YEAR FROM fecha) = {ano}
                AND EXTRACT(MONTH FROM fecha) = {mes}
                """
                
                result = pg_hook.get_first(query_mes)
                if result and result[0] > 0:
                    stats_por_mes[mes] = {
                        "total": result[0],
                        "corregidos": result[1],
                        "porcentaje_corregidos": (result[1] / result[0] * 100) if result[0] > 0 else 0
                    }
            
            # 4. Estadísticas por alimentador (top 10)
            query_alimentadores = f"""
            SELECT alimentador,
                   COUNT(*) as total,
                   SUM(CASE WHEN procesado = TRUE THEN 1 ELSE 0 END) as corregidos
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            GROUP BY alimentador
            ORDER BY corregidos DESC
            LIMIT 10
            """
            
            resultados_alim = pg_hook.get_records(query_alimentadores)
            
            stats_por_alimentador = {}
            for alim, total_alim, corregidos_alim in resultados_alim:
                stats_por_alimentador[alim] = {
                    "total": total_alim,
                    "corregidos": corregidos_alim,
                    "porcentaje_corregidos": (corregidos_alim / total_alim * 100) if total_alim > 0 else 0
                }
            
            # Compilar resultados
            return {
                "ano": ano,
                "total_registros": total,
                "total_corregidos": corregidos,
                "porcentaje_global_corregidos": (corregidos / total * 100) if total > 0 else 0,
                "estadisticas_por_mes": stats_por_mes,
                "top_alimentadores": stats_por_alimentador
            }
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas de procesamiento: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error en estadísticas: {str(e)}"
            }