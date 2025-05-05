import logging
import datetime
import pandas as pd
import numpy as np
import time
import calendar
from typing import Dict, List, Optional, Union, Tuple, Any, Set
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class ValidationUtils:
    """Clase para validar la calidad e integridad de datos."""
    
    def __init__(self, tabla_origen: str = "potencia_dep", 
                tabla_destino: str = "potencia_dep_processed",
                conexion_origen: str = "postgres_centrosur",
                conexion_destino: str = "postgres_base_prueba"):
        """
        Inicializa el validador de datos.
        
        Args:
            tabla_origen: Nombre de la tabla origen
            tabla_destino: Nombre de la tabla destino
            conexion_origen: Nombre de la conexión a la base de datos origen
            conexion_destino: Nombre de la conexión a la base de datos destino
        """
        self.tabla_origen = tabla_origen
        self.tabla_destino = tabla_destino
        self.conexion_origen = conexion_origen
        self.conexion_destino = conexion_destino

        self.origen_conn_id = conexion_origen
        self.destino_conn_id = conexion_destino
    
    def validar_rangos_fechas(self, mes: int, ano: int = 2019) -> Dict[str, Any]:
        """
        Valida los rangos de fechas para un mes específico.
        
        Args:
            mes: Número de mes a validar (1-12)
            ano: Año a validar
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Definir rangos esperados
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            # Verificar fechas mínima y máxima en los datos
            query = f"""
            SELECT 
                MIN(fecha) as fecha_min, 
                MAX(fecha) as fecha_max 
            FROM {self.tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            resultado = pg_hook.get_first(query)
            
            if not resultado or not resultado[0]:
                return {
                    "valido": False,
                    "mensaje": f"No hay datos para el mes {mes} año {ano}",
                    "fecha_min": None,
                    "fecha_max": None,
                    "rango_esperado": (fecha_inicio, fecha_fin)
                }
            
            fecha_min, fecha_max = resultado
            
            # Convertir a str para comparación si son datetime
            if isinstance(fecha_min, datetime.date):
                fecha_min = fecha_min.strftime("%Y-%m-%d")
            if isinstance(fecha_max, datetime.date):
                fecha_max = fecha_max.strftime("%Y-%m-%d")
                
            # Validar que fechas están dentro del rango esperado
            fecha_min_valida = fecha_min >= fecha_inicio
            fecha_max_valida = fecha_max < fecha_fin
            
            return {
                "valido": fecha_min_valida and fecha_max_valida,
                "mensaje": "Rango de fechas válido" if (fecha_min_valida and fecha_max_valida) else "Rango de fechas inválido",
                "fecha_min": fecha_min,
                "fecha_max": fecha_max,
                "rango_esperado": (fecha_inicio, fecha_fin)
            }
            
        except Exception as e:
            logger.error(f"Error validando rangos de fechas: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    
    def verificar_valores_nulos(self, mes: int, ano: int = 2019, 
                              columnas: List[str] = None) -> Dict[str, Any]:
        """
        Verifica valores nulos en columnas críticas.
        
        Args:
            mes: Número de mes a validar
            ano: Año a validar
            columnas: Lista de columnas a verificar (si None, se usan columnas predeterminadas)
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Columnas predeterminadas si no se especifican
            if not columnas:
                columnas = ["fecha", "hora", "alimentador", "potencia_activa", "potencia_reactiva"]
            
            # Construir condiciones de filtro
            filtro_mes = f"EXTRACT(MONTH FROM fecha) = {mes}" if "fecha" in columnas else "TRUE"
            filtro_ano = f"EXTRACT(YEAR FROM fecha) = {ano}" if "fecha" in columnas else "TRUE"
            
            # Construir query para contar nulos en cada columna
            conditions = []
            for col in columnas:
                conditions.append(f"{col} IS NULL")
            
            query = f"""
            SELECT 
                COUNT(*) as total_registros,
                {', '.join([f'SUM(CASE WHEN {condition} THEN 1 ELSE 0 END) as {col}_nulos' 
                           for condition, col in zip(conditions, columnas)])}
            FROM {self.tabla_origen}
            WHERE {filtro_mes} AND {filtro_ano}
            """
            
            resultado = pg_hook.get_first(query)
            
            if not resultado or resultado[0] == 0:
                return {
                    "valido": False,
                    "mensaje": f"No hay datos para el mes {mes} año {ano}",
                    "total_registros": 0,
                    "nulos_por_columna": {}
                }
            
            # Procesar resultados
            total_registros = resultado[0]
            nulos_por_columna = {}
            tiene_nulos = False
            
            for i, col in enumerate(columnas):
                nulos = resultado[i+1]
                nulos_por_columna[col] = {
                    "nulos": nulos,
                    "porcentaje": (nulos / total_registros * 100) if total_registros > 0 else 0
                }
                if nulos > 0:
                    tiene_nulos = True
            
            return {
                "valido": not tiene_nulos,
                "mensaje": "No hay valores nulos" if not tiene_nulos else "Se encontraron valores nulos",
                "total_registros": total_registros,
                "nulos_por_columna": nulos_por_columna
            }
            
        except Exception as e:
            logger.error(f"Error verificando valores nulos: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    def verificar_valores_duplicados(self, mes: int, ano: int = 2019) -> Dict[str, Any]:
        """
        Verifica registros duplicados en la tabla origen.
        
        Args:
            mes: Número de mes a verificar
            ano: Año a verificar
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Consulta para contar registros duplicados basados en clave primaria lógica
            query = f"""
            WITH duplicados AS (
                SELECT 
                    fecha, hora, alimentador, COUNT(*) as conteo
                FROM {self.tabla_origen}
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                GROUP BY fecha, hora, alimentador
                HAVING COUNT(*) > 1
            )
            SELECT 
                COUNT(*) as total_grupos_duplicados,
                SUM(conteo) as total_registros_duplicados
            FROM duplicados
            """
            
            resultado = pg_hook.get_first(query)
            
            if not resultado:
                return {
                    "valido": True,
                    "mensaje": "No hay datos o error en la consulta",
                    "duplicados": 0,
                    "grupos_duplicados": 0
                }
            
            total_grupos, total_registros = resultado
            
            # Consulta para contar total de registros
            total_query = f"""
            SELECT COUNT(*) 
            FROM {self.tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            total_mes = pg_hook.get_first(total_query)[0]
            
            # Calcular porcentaje de duplicación
            porcentaje_duplicados = (total_registros / total_mes * 100) if total_mes > 0 else 0
            
            # Si hay duplicados, obtener algunos ejemplos
            ejemplos = []
            if total_grupos > 0:
                ejemplos_query = f"""
                SELECT fecha, hora, alimentador, COUNT(*) as conteo
                FROM {self.tabla_origen}
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                GROUP BY fecha, hora, alimentador
                HAVING COUNT(*) > 1
                ORDER BY conteo DESC
                LIMIT 5
                """
                
                ejemplos = pg_hook.get_records(ejemplos_query)
            
            return {
                "valido": total_grupos == 0,
                "mensaje": "No hay duplicados" if total_grupos == 0 else f"Se encontraron {total_grupos} grupos con duplicados",
                "total_registros": total_mes,
                "grupos_duplicados": total_grupos,
                "registros_duplicados": total_registros,
                "porcentaje_duplicados": porcentaje_duplicados,
                "ejemplos": ejemplos
            }
            
        except Exception as e:
            logger.error(f"Error verificando duplicados: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    
    def verificar_valores_atipicos(self, mes: int, ano: int = 2019, 
                                columnas: List[str] = None, 
                                umbral_std: float = 3.0) -> Dict[str, Any]:
        """
        Identifica valores atípicos (outliers) usando el método de desviación estándar.
        
        Args:
            mes: Número de mes a verificar
            ano: Año a verificar
            columnas: Lista de columnas numéricas a verificar
            umbral_std: Número de desviaciones estándar para considerar valor atípico
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Columnas predeterminadas si no se especifican
            if not columnas:
                columnas = ["potencia_activa", "potencia_reactiva"]
            
            resultados = {}
            
            for columna in columnas:
                # Consulta para obtener estadísticas básicas
                query = f"""
                SELECT 
                    AVG({columna}) as media,
                    STDDEV({columna}) as desviacion,
                    COUNT(*) as total,
                    MIN({columna}) as minimo,
                    MAX({columna}) as maximo
                FROM {self.tabla_origen}
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                AND {columna} IS NOT NULL
                """
                
                stats = pg_hook.get_first(query)
                
                if not stats or stats[0] is None:
                    resultados[columna] = {
                        "valido": True,
                        "mensaje": "No hay datos suficientes para análisis",
                        "total_registros": 0,
                        "outliers": 0
                    }
                    continue
                
                media, desviacion, total, minimo, maximo = stats
                
                # Calcular límites para outliers
                limite_inf = media - (umbral_std * desviacion)
                limite_sup = media + (umbral_std * desviacion)
                
                # Contar outliers
                outliers_query = f"""
                SELECT COUNT(*)
                FROM {self.tabla_origen}
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
                AND {columna} IS NOT NULL
                AND ({columna} < {limite_inf} OR {columna} > {limite_sup})
                """
                
                outliers = pg_hook.get_first(outliers_query)[0]
                
                # Calcular porcentaje
                porcentaje_outliers = (outliers / total * 100) if total > 0 else 0
                
                # Obtener ejemplos de outliers
                ejemplos = []
                if outliers > 0:
                    ejemplos_query = f"""
                    SELECT fecha, hora, alimentador, {columna}
                    FROM {self.tabla_origen}
                    WHERE EXTRACT(MONTH FROM fecha) = {mes}
                    AND EXTRACT(YEAR FROM fecha) = {ano}
                    AND {columna} IS NOT NULL
                    AND ({columna} < {limite_inf} OR {columna} > {limite_sup})
                    ORDER BY 
                        CASE 
                            WHEN {columna} > {limite_sup} THEN {columna} - {limite_sup}
                            ELSE {limite_inf} - {columna}
                        END DESC
                    LIMIT 5
                    """
                    
                    ejemplos = pg_hook.get_records(ejemplos_query)
                
                resultados[columna] = {
                    "valido": outliers == 0,
                    "mensaje": f"Se encontraron {outliers} valores atípicos" if outliers > 0 else "No hay valores atípicos",
                    "total_registros": total,
                    "outliers": outliers,
                    "porcentaje_outliers": porcentaje_outliers,
                    "estadisticas": {
                        "media": media,
                        "desviacion": desviacion,
                        "minimo": minimo,
                        "maximo": maximo,
                        "limite_inferior": limite_inf,
                        "limite_superior": limite_sup
                    },
                    "ejemplos": ejemplos
                }
            
            # Resultado general
            hay_outliers = any(not result["valido"] for result in resultados.values())
            
            return {
                "valido": not hay_outliers,
                "mensaje": "No hay valores atípicos" if not hay_outliers else "Se encontraron valores atípicos",
                "resultados_por_columna": resultados
            }
            
        except Exception as e:
            logger.error(f"Error verificando valores atípicos: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    def verificar_integridad_referencial(self, mes: int, ano: int = 2019) -> Dict[str, Any]:
        """
        Verifica la integridad referencial entre alimentadores y subestaciones.
        
        Args:
            mes: Número de mes a verificar
            ano: Año a verificar
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # Verificar si existe la tabla de subestaciones
            check_table_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'subestacion_dep'
            )
            """
            
            tabla_subestacion_existe = pg_hook.get_first(check_table_query)[0]
            
            if not tabla_subestacion_existe:
                return {
                    "valido": True,  # No se puede validar si no existe la tabla
                    "mensaje": "No existe tabla subestacion_dep para validar integridad referencial",
                    "referencia_invalida": 0
                }
            
            # Consulta para encontrar alimentadores sin subestación asociada
            query = f"""
            WITH alimentadores_mes AS (
                SELECT DISTINCT alimentador
                FROM {self.tabla_origen}
                WHERE EXTRACT(MONTH FROM fecha) = {mes}
                AND EXTRACT(YEAR FROM fecha) = {ano}
            )
            SELECT
                COUNT(DISTINCT am.alimentador) as total_alimentadores,
                COUNT(DISTINCT CASE WHEN NOT EXISTS (
                    SELECT 1 FROM subestacion_dep s
                    WHERE s.alimentador = am.alimentador
                ) THEN am.alimentador ELSE NULL END) as alimentadores_sin_subestacion
            FROM alimentadores_mes am
            """
            
            resultados = pg_hook.get_first(query)
            
            if not resultados:
                return {
                    "valido": True,
                    "mensaje": "No hay datos o error en la consulta",
                    "total_alimentadores": 0,
                    "alimentadores_sin_referencia": 0
                }
            
            total_alimentadores, sin_subestacion = resultados
            
            # Obtener ejemplos de alimentadores sin subestación
            ejemplos = []
            if sin_subestacion > 0:
                ejemplos_query = f"""
                SELECT DISTINCT p.alimentador
                FROM {self.tabla_origen} p
                WHERE EXTRACT(MONTH FROM p.fecha) = {mes}
                AND EXTRACT(YEAR FROM p.fecha) = {ano}
                AND NOT EXISTS (
                    SELECT 1 FROM subestacion_dep s
                    WHERE s.alimentador = p.alimentador
                )
                LIMIT 5
                """
                
                ejemplos = pg_hook.get_records(ejemplos_query)
            
            return {
                "valido": sin_subestacion == 0,
                "mensaje": "Todas las referencias son válidas" if sin_subestacion == 0 
                          else f"Hay {sin_subestacion} alimentadores sin subestación asociada",
                "total_alimentadores": total_alimentadores,
                "alimentadores_sin_referencia": sin_subestacion,
                "porcentaje_invalido": (sin_subestacion / total_alimentadores * 100) if total_alimentadores > 0 else 0,
                "ejemplos": ejemplos
            }
            
        except Exception as e:
            logger.error(f"Error verificando integridad referencial: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    
    def generar_informe_calidad_datos(self, mes: int, ano: int = 2019) -> Dict[str, Any]:
        """
        Genera un informe completo de calidad de datos para un mes específico.
        
        Args:
            mes: Número de mes a validar
            ano: Año a validar
            
        Returns:
            Dict con informe completo de calidad
        """
        try:
            # Ejecutar todas las validaciones
            rangos_fechas = self.validar_rangos_fechas(mes, ano)
            valores_nulos = self.verificar_valores_nulos(mes, ano)
            duplicados = self.verificar_valores_duplicados(mes, ano)
            atipicos = self.verificar_valores_atipicos(mes, ano)
            integridad = self.verificar_integridad_referencial(mes, ano)
            
            # Compilar resultados
            informe = {
                "mes": mes,
                "ano": ano,
                "timestamp": datetime.datetime.now().isoformat(),
                "validaciones": {
                    "rangos_fechas": rangos_fechas,
                    "valores_nulos": valores_nulos,
                    "duplicados": duplicados,
                    "valores_atipicos": atipicos,
                    "integridad_referencial": integridad
                }
            }
            
            # Evaluar calidad general
            validaciones_ok = [
                rangos_fechas.get("valido", False),
                valores_nulos.get("valido", False),
                duplicados.get("valido", False),
                atipicos.get("valido", False),
                integridad.get("valido", False)
            ]
            
            total_validaciones = len(validaciones_ok)
            validaciones_correctas = sum(1 for v in validaciones_ok if v)
            
            # Calcular puntaje de calidad (0-100)
            puntaje_calidad = (validaciones_correctas / total_validaciones * 100) if total_validaciones > 0 else 0
            
            # Determinar nivel de calidad
            if puntaje_calidad >= 90:
                nivel_calidad = "Excelente"
            elif puntaje_calidad >= 75:
                nivel_calidad = "Bueno"
            elif puntaje_calidad >= 50:
                nivel_calidad = "Regular"
            else:
                nivel_calidad = "Deficiente"
            
            informe["evaluacion"] = {
                "puntaje_calidad": puntaje_calidad,
                "nivel_calidad": nivel_calidad,
                "validaciones_correctas": validaciones_correctas,
                "total_validaciones": total_validaciones
            }
            
            return informe
            
        except Exception as e:
            logger.error(f"Error generando informe de calidad: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error generando informe: {str(e)}"
            }
    def validar_datos_procesados(self, mes: int, ano: int = 2019) -> Dict[str, Any]:
        """
        Valida la calidad e integridad de los datos procesados en la tabla destino.
        
        Args:
            mes: Número de mes a validar
            ano: Año a validar
            
        Returns:
            Dict con resultados de validación
        """
        try:
            pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
            pg_hook_origen = PostgresHook(postgres_conn_id=self.conexion_origen)
            
            # 1. Verificar conteo de registros (comparación con origen)
            query_conteo_origen = f"""
            SELECT COUNT(*) 
            FROM {self.tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            query_conteo_destino = f"""
            SELECT COUNT(*) 
            FROM {self.tabla_destino}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            conteo_origen = pg_hook_origen.get_first(query_conteo_origen)[0]
            conteo_destino = pg_hook_destino.get_first(query_conteo_destino)[0]
            
            # Calcular ratio de completitud
            ratio_completitud = (conteo_destino / conteo_origen * 100) if conteo_origen > 0 else 0
            
            # 2. Verificar integridad de alimentadores
            query_alim_origen = f"""
            SELECT COUNT(DISTINCT alimentador) 
            FROM {self.tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            query_alim_destino = f"""
            SELECT COUNT(DISTINCT alimentador) 
            FROM {self.tabla_destino}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            alim_origen = pg_hook_origen.get_first(query_alim_origen)[0]
            alim_destino = pg_hook_destino.get_first(query_alim_destino)[0]
            
            # Ratio de alimentadores procesados
            ratio_alimentadores = (alim_destino / alim_origen * 100) if alim_origen > 0 else 0
            
            # 3. Verificar rangos de datos numéricos
            query_rangos_origen = f"""
            SELECT 
                MIN(potencia_activa) as min_activa_orig, 
                MAX(potencia_activa) as max_activa_orig,
                MIN(potencia_reactiva) as min_reactiva_orig, 
                MAX(potencia_reactiva) as max_reactiva_orig
            FROM {self.tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            query_rangos_destino = f"""
            SELECT 
                MIN(potencia_activa) as min_activa_dest, 
                MAX(potencia_activa) as max_activa_dest,
                MIN(potencia_reactiva) as min_reactiva_dest, 
                MAX(potencia_reactiva) as max_reactiva_dest
            FROM {self.tabla_destino}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            
            rangos_origen = pg_hook_origen.get_first(query_rangos_origen)
            rangos_destino = pg_hook_destino.get_first(query_rangos_destino)
            
            # Verificar si los rangos son similares
            rangos_similares = True
            diferencias_rangos = {}
            
            if None not in rangos_origen and None not in rangos_destino:
                min_activa_orig, max_activa_orig, min_reactiva_orig, max_reactiva_orig = rangos_origen
                min_activa_dest, max_activa_dest, min_reactiva_dest, max_reactiva_dest = rangos_destino
                
                # Calcular diferencias relativas
                if min_activa_orig is not None and min_activa_dest is not None:
                    dif_min_activa = abs(min_activa_orig - min_activa_dest) / max(1, abs(min_activa_orig)) * 100
                    diferencias_rangos["min_potencia_activa"] = dif_min_activa
                    rangos_similares = rangos_similares and dif_min_activa < 5  # 5% de diferencia permitida
                
                if max_activa_orig is not None and max_activa_dest is not None:
                    dif_max_activa = abs(max_activa_orig - max_activa_dest) / max(1, abs(max_activa_orig)) * 100
                    diferencias_rangos["max_potencia_activa"] = dif_max_activa
                    rangos_similares = rangos_similares and dif_max_activa < 5
                
                if min_reactiva_orig is not None and min_reactiva_dest is not None:
                    dif_min_reactiva = abs(min_reactiva_orig - min_reactiva_dest) / max(1, abs(min_reactiva_orig)) * 100
                    diferencias_rangos["min_potencia_reactiva"] = dif_min_reactiva
                    rangos_similares = rangos_similares and dif_min_reactiva < 5
                
                if max_reactiva_orig is not None and max_reactiva_dest is not None:
                    dif_max_reactiva = abs(max_reactiva_orig - max_reactiva_dest) / max(1, abs(max_reactiva_orig)) * 100
                    diferencias_rangos["max_potencia_reactiva"] = dif_max_reactiva
                    rangos_similares = rangos_similares and dif_max_reactiva < 5
            
            # Evaluar calidad general
            requisitos = [
                {"criterio": "Completitud registros", "valor": ratio_completitud >= 95, "detalles": f"{ratio_completitud:.2f}%"},
                {"criterio": "Completitud alimentadores", "valor": ratio_alimentadores >= 95, "detalles": f"{ratio_alimentadores:.2f}%"},
                {"criterio": "Rangos similares", "valor": rangos_similares, "detalles": diferencias_rangos}
            ]
            
            total_requisitos = len(requisitos)
            requisitos_cumplidos = sum(1 for req in requisitos if req["valor"])
            
            return {
                "valido": requisitos_cumplidos == total_requisitos,
                "mensaje": f"Validación completa: {requisitos_cumplidos}/{total_requisitos} requisitos cumplidos",
                "detalle_requisitos": requisitos,
                "estadisticas": {
                    "registros_origen": conteo_origen,
                    "registros_destino": conteo_destino,
                    "alimentadores_origen": alim_origen,
                    "alimentadores_destino": alim_destino,
                    "ratio_completitud": ratio_completitud,
                    "ratio_alimentadores": ratio_alimentadores,
                    "rangos_origen": rangos_origen,
                    "rangos_destino": rangos_destino,
                    "diferencias_rangos": diferencias_rangos
                }
            }
            
        except Exception as e:
            logger.error(f"Error validando datos procesados: {str(e)}")
            return {
                "valido": False,
                "mensaje": f"Error en validación: {str(e)}",
                "error": str(e)
            }
    def verificar_disponibilidad_datos_por_mes(self, tabla: str = "potencia_dep_original", 
                                             ano: int = 2019,
                                             conexion: str = "postgres_centrosur") -> Dict[int, int]:
        """
        Genera un informe de disponibilidad de datos por mes en la tabla origen.
        
        Args:
            tabla: Nombre de la tabla a verificar
            ano: Año a verificar
            conexion: Nombre de la conexión a la base de datos
            
        Returns:
            Dict[int, int]: Diccionario con {mes: cantidad_registros}
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=conexion)
            
            logger.info("📊 VERIFICACIÓN DE DISPONIBILIDAD DE DATOS POR MES EN TABLA ORIGEN")
            logger.info("=" * 60)
            
            disponibilidad = {}
            
            for mes in range(1, 13):
                # Calcular fechas para cada mes
                mes_siguiente = mes + 1 if mes < 12 else 1
                ano_siguiente = ano if mes < 12 else ano + 1
                fecha_inicio = f"{ano}-{mes:02d}-01"
                fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
                
                # Consulta para verificar datos
                query = f"""
                SELECT COUNT(*) 
                FROM {tabla}
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
    
    def crear_tabla_temporal_mes(self, mes: int, ano: int = 2019, 
                                tabla_origen: str = "potencia_dep",
                                conexion_origen: str = "postgres_centrosur") -> Optional[str]:
        """
        Crea una tabla temporal con datos solo del mes especificado.
        
        Args:
            mes: Número de mes (1-12)
            ano: Año a procesar
            tabla_origen: Nombre de la tabla origen
            conexion_origen: Nombre de la conexión a la base de datos origen
            
        Returns:
            Optional[str]: Nombre de la tabla temporal creada o None si hay error
        """
        try:
            pg_hook = PostgresHook(postgres_conn_id=conexion_origen)
            
            # Calcular fechas para el mes
            mes_siguiente = mes + 1 if mes < 12 else 1
            ano_siguiente = ano if mes < 12 else ano + 1
            fecha_inicio = f"{ano}-{mes:02d}-01"
            fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
            
            # Nombre único para la tabla temporal
            tabla_temporal = f"{tabla_origen}_mes{mes}_{int(time.time())}"
            
            # Verificar si hay datos para este mes
            query_verificar = f"""
            SELECT COUNT(*) FROM {tabla_origen} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            """
            count = pg_hook.get_first(query_verificar)[0]
            
            if count == 0:
                logger.warning(f"⚠️ No hay datos para el mes {mes} en {tabla_origen}")
                return None
            
            logger.info(f"✅ Encontrados {count} registros en {tabla_origen} para mes {mes}")
            
            # Crear tabla temporal con datos del mes
            create_query = f"""
            CREATE TABLE {tabla_temporal} AS
            SELECT * FROM {tabla_origen} 
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = {ano}
            AND fecha >= '{fecha_inicio}'
            AND fecha < '{fecha_fin}'
            """
            
            pg_hook.run(f"DROP TABLE IF EXISTS {tabla_temporal}")
            pg_hook.run(create_query)
            
            # Verificar creación
            count_temp = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_temporal}")[0]
            logger.info(f"✅ Tabla temporal {tabla_temporal} creada con {count_temp} registros")
            
            return tabla_temporal
            
        except Exception as e:
            logger.error(f"❌ Error creando tabla temporal: {str(e)}")
            return None
    
    def actualizar_consulta_procesador(self, query_processor_id: str, 
                                      consulta: str, 
                                      tabla_origen: str = None) -> bool:
        """
        Actualiza la consulta SQL del procesador QueryDatabaseTable.
        
        Args:
            query_processor_id: ID del procesador QueryDatabaseTable
            consulta: Consulta SQL a configurar
            tabla_origen: Nombre de la tabla origen (opcional)
            
        Returns:
            bool: True si la actualización fue exitosa
        """
        try:
            logger.info(f"🔄 Actualizando consulta procesador {query_processor_id}...")
            
            # Si no se proporcionó una consulta específica pero sí una tabla origen
            if not consulta and tabla_origen:
                # Crear consulta predeterminada
                consulta = f"""/* CONSULTA_GENERADA_{int(time.time())} */
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
            
            # Verificar que tenemos una consulta
            if not consulta:
                logger.error("❌ No se proporcionó una consulta SQL")
                return False
                
            # Usar NifiUtils para actualizar la consulta
            if self.nifi_utils.update_query_processor(query_processor_id, consulta, clear_table_name=True):
                logger.info("✅ Consulta actualizada correctamente")
                return True
            else:
                logger.error("❌ Error actualizando consulta")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error actualizando consulta: {str(e)}")
            return False
    
    def configurar_kafka_topics(self, mes: int, publish_processor_id: str, 
                              consume_processor_id: str) -> Tuple[bool, str]:
        """
        Configura topics de Kafka únicos para el mes especificado.
        
        Args:
            mes: Número de mes (1-12)
            publish_processor_id: ID del procesador PublishKafka
            consume_processor_id: ID del procesador ConsumeKafka
            
        Returns:
            Tuple[bool, str]: (éxito, nombre del topic)
        """
        try:
            # Crear nombre de topic único
            topic_name = f"potencia-mes{mes}-{int(time.time())}"
            logger.info(f"🔄 Configurando topic Kafka único: {topic_name}")
            
            # Configurar procesador PublishKafka
            publish_ok = self.nifi_utils.configure_kafka_topic(
                publish_processor_id, 
                "Topic Name", 
                topic_name
            )
            
            if not publish_ok:
                logger.error(f"❌ Error configurando topic en procesador PublishKafka")
                return False, topic_name
                
            # Configurar procesador ConsumeKafka
            consume_ok = self.nifi_utils.configure_kafka_topic(
                consume_processor_id, 
                "topic", 
                topic_name
            )
            
            if not consume_ok:
                logger.error(f"❌ Error configurando topic en procesador ConsumeKafka")
                return False, topic_name
                
            logger.info(f"✅ Topics Kafka configurados correctamente: {topic_name}")
            return True, topic_name
            
        except Exception as e:
            logger.error(f"❌ Error configurando topics Kafka: {str(e)}")
            return False, ""
    
    def iniciar_procesamiento_mes(self, 
                                 producer_group_id: str, 
                                 consumer_group_id: str,
                                 query_processor_id: str,
                                 processor_ids: List[str] = None) -> bool:
        """
        Inicia el procesamiento para un mes específico.
        
        Args:
            producer_group_id: ID del grupo productor
            consumer_group_id: ID del grupo consumidor
            query_processor_id: ID del procesador QueryDatabaseTable
            processor_ids: Lista de IDs de procesadores adicionales a limpiar
            
        Returns:
            bool: True si se inició correctamente
        """
        try:
            # 1. Limpiar estado de procesadores y colas
            logger.info("🧹 Limpiando estado de procesadores y colas...")
            if processor_ids is None:
                processor_ids = [query_processor_id]
                
            self.nifi_utils.clear_processor_state_and_queues(
                producer_group_id,
                consumer_group_id,
                processor_ids
            )
            
            # 2. Habilitar servicios controladores
            logger.info("🔌 Habilitando servicios controladores...")
            self.nifi_utils.enable_controller_services(producer_group_id)
            self.nifi_utils.enable_controller_services(consumer_group_id)
            
            # 3. Iniciar procesadores
            logger.info("▶️ Iniciando procesadores...")
            if hasattr(self.nifi_utils, 'iniciar_todos_procesadores_por_grupo'):
                result = self.nifi_utils.iniciar_todos_procesadores_por_grupo()
                if result:
                    logger.info("✅ Procesadores iniciados correctamente")
                    return True
                else:
                    logger.error("❌ Error iniciando procesadores")
                    return False
            else:
                logger.warning("⚠️ Método iniciar_todos_procesadores_por_grupo no disponible")
                return False
                
        except Exception as e:
            logger.error(f"❌ Error iniciando procesamiento: {str(e)}")
            return False
    
    def limpiar_tablas_temporales(self, patron: str = "potencia_dep_%", 
                                tablas_protegidas: List[str] = None, 
                                conexion: str = "postgres_centrosur",
                                **kwargs) -> int:
        """
        Limpia todas las tablas temporales que coincidan con el patrón.
        
        Args:
            patron: Patrón SQL LIKE para buscar tablas
            tablas_protegidas: Lista de nombres de tablas a proteger
            conexion: Nombre de la conexión a la base de datos
            **kwargs: Argumentos adicionales (para ti XCom)
            
        Returns:
            int: Cantidad de tablas eliminadas
        """
        try:
            if tablas_protegidas is None:
                tablas_protegidas = [
                    "potencia_dep", 
                    "potencia_dep_original", 
                    "potencia_dep_processed"
                ]
                
            pg_hook = PostgresHook(postgres_conn_id=conexion)
            
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
            WHERE table_name LIKE '{patron}'
            """
            
            tablas_db = pg_hook.get_records(query_tablas_temp)
            tablas_a_eliminar = set(tablas_trabajo)  # Uso set para evitar duplicados
            
            for tabla in tablas_db:
                nombre_tabla = tabla[0]
                # Proteger las tablas críticas
                if nombre_tabla not in tablas_protegidas:
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
            
            logger.info(f"✅ Limpiadas {count} tablas temporales")
            return count
            
        except Exception as e:
            logger.error(f"⚠️ Error en limpieza de tablas: {str(e)}")
            return 0
    
    def decidir_ruta_mes(self, ti: Any, mes: int, nombre_mes: str) -> str:
        """
        Determina la ruta correcta para el flujo basado en verificación.
        
        Args:
            ti: Task Instance para acceder a XCom
            mes: Número de mes (1-12)
            nombre_mes: Nombre del mes para construir la ruta
            
        Returns:
            str: Ruta para el siguiente nodo del DAG
        """
        # Verificar el resultado explícitamente
        resultado = ti.xcom_pull(task_ids=f"procesar_{nombre_mes}.verificar_si_{nombre_mes}_ya_procesado")
        logger.info(f"Resultado de verificación para mes {mes}: {resultado}")
        
        if resultado:
            logger.info(f"✅ DECISIÓN: Mes {mes} ya procesado, SALTANDO...")
            return f"procesar_{nombre_mes}.skip_y_verificar.skip_{nombre_mes}"
        else:
            logger.info(f"🔄 DECISIÓN: Mes {mes} necesita procesarse")
            return f"procesar_{nombre_mes}.camino_procesamiento.procesar_mes_{mes}"

    def verificar_completitud_anual(self, ano: int = 2019, 
                                   tabla_origen: str = "potencia_dep", 
                                   tabla_destino: str = "potencia_dep_processed") -> Dict[str, Any]:
        """
        Genera un informe de completitud del procesamiento anual.
        
        Args:
            ano: Año a verificar
            tabla_origen: Nombre de la tabla origen
            tabla_destino: Nombre de la tabla destino
            
        Returns:
            Dict[str, Any]: Informe detallado de completitud
        """
        try:
            # Usar PostgresHook con las conexiones correctas
            pg_hook_origen = PostgresHook(postgres_conn_id=self.origen_conn_id)
            pg_hook_destino = PostgresHook(postgres_conn_id=self.destino_conn_id)

            fecha_inicio = f"{ano}-01-01"
            fecha_fin = f"{ano+1}-01-01"
            
            # Conteo total de registros en origen para el año
            total_origen = pg_hook_origen.get_first(
                f"""
                SELECT COUNT(*) FROM {tabla_origen} 
                WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                """
            )[0]
            
            # Conteo total de registros en destino para el año
            total_destino = pg_hook_destino.get_first(
                f"""
                SELECT COUNT(*) FROM {tabla_destino} 
                WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                """
            )[0]
            
            # Conteo por mes en origen
            conteo_origen_por_mes = pg_hook_origen.get_records(
                f"""
                SELECT EXTRACT(MONTH FROM fecha) AS mes, COUNT(*)
                FROM {tabla_origen} 
                WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                GROUP BY EXTRACT(MONTH FROM fecha)
                ORDER BY mes
                """
            )

            # Conteo por mes en destino
            conteo_destino_por_mes = pg_hook_destino.get_records(
                f"""
                SELECT EXTRACT(MONTH FROM fecha) AS mes, COUNT(*)
                FROM {tabla_destino} 
                WHERE fecha BETWEEN '{fecha_inicio}' AND '{fecha_fin}'
                GROUP BY EXTRACT(MONTH FROM fecha)
                ORDER BY mes
                """
            )
            
            # Convertir a diccionarios para facilitar la búsqueda
            dict_origen = {int(float(mes)): count for mes, count in conteo_origen_por_mes}
            dict_destino = {int(float(mes)): count for mes, count in conteo_destino_por_mes}
            
            # Calcular completitud total
            completitud_total = (total_destino / total_origen * 100) if total_origen > 0 else 0
            
            # Preparar informe de meses
            informes_meses = []
            for mes in range(1, 13):
                origen = dict_origen.get(mes, 0)
                destino = dict_destino.get(mes, 0)
                
                if origen > 0:
                    completitud = (destino / origen * 100)
                    estado = "Completo" if completitud >= 99 else "Incompleto"
                else:
                    completitud = 0
                    estado = "Sin datos origen"
                
                informes_meses.append({
                    "mes": mes,
                    "nombre_mes": calendar.month_name[mes],
                    "registros_origen": origen,
                    "registros_destino": destino,
                    "completitud": completitud,
                    "estado": estado
                })
            
            # Generar informe completo
            informe = {
                "ano": ano,
                "total_registros_origen": total_origen,
                "total_registros_destino": total_destino,
                "completitud_total": completitud_total,
                "estado_general": "Completo" if completitud_total >= 99 else "Incompleto",
                "detalle_por_mes": informes_meses,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Registrar un resumen en los logs
            logger.info(f"📊 INFORME COMPLETITUD ANUAL {ano}: {completitud_total:.2f}%")
            logger.info(f"Total origen: {total_origen} | Total destino: {total_destino}")
            
            for informe_mes in informes_meses:
                if informe_mes["registros_origen"] > 0:
                    logger.info(
                        f"{informe_mes['nombre_mes']:>9}: {informe_mes['completitud']:.2f}% " +
                        f"({informe_mes['registros_destino']}/{informe_mes['registros_origen']})"
                    )
                else:
                    logger.info(f"{informe_mes['nombre_mes']:>9}: Sin datos origen")
            
            return informe
            
        except Exception as e:
            logger.error(f"❌ Error verificando completitud anual: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error en verificación anual: {str(e)}"
            }
    
def registrar_resultados_validacion(self, mes: int, ano: int, 
                              resultado: Dict[str, Any], 
                              tipo_validacion: str) -> bool:
    """
    Registra los resultados de validación en un archivo o base de datos.
    
    Args:
        mes: Número de mes validado (1-12)
        ano: Año validado
        resultado: Diccionario con resultados de validación
        tipo_validacion: Tipo de validación realizada
        
    Returns:
        bool: True si el registro fue exitoso
    """
    try:
        # Crear directorio de logs si no existe
        import os
        log_dir = os.path.join(os.getcwd(), "logs", "validaciones")
        os.makedirs(log_dir, exist_ok=True)
        
        # Nombre del archivo de log
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{log_dir}/validacion_{tipo_validacion}_mes{mes}_ano{ano}_{timestamp}.json"
        
        # Añadir metadatos
        resultado_completo = {
            "mes": mes,
            "ano": ano,
            "tipo_validacion": tipo_validacion,
            "timestamp": datetime.datetime.now().isoformat(),
            "resultado": resultado
        }
        
        # Guardar en archivo JSON
        import json
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(resultado_completo, f, ensure_ascii=False, indent=2)
            
        logger.info(f"✅ Resultados de validación guardados en {filename}")
        
        # También podríamos guardar en base de datos si fuera necesario
        
        return True
            
    except Exception as e:
        logger.error(f"❌ Error registrando resultados de validación: {str(e)}")
        return False

def validar_schema_tablas(self) -> Dict[str, Any]:
    """
    Valida que las tablas origen y destino tengan la estructura esperada.
    
    Returns:
        Dict con resultados de validación
    """
    try:
        pg_hook_origen = PostgresHook(postgres_conn_id=self.conexion_origen)
        pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
        
        # Columnas esperadas en la tabla origen
        columnas_esperadas_origen = {
            "fecha": "date",
            "hora": "integer",
            "alimentador": "character varying",
            "dia_semana": "integer",
            "potencia_activa": "numeric",
            "potencia_reactiva": "numeric"
        }
        
        # Columnas esperadas en la tabla destino
        columnas_esperadas_destino = {
            "fecha": "date",
            "hora": "integer",
            "alimentador": "character varying",
            "potencia_activa": "numeric",
            "potencia_reactiva": "numeric"
        }
        
        # Verificar tabla origen
        query_origen = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{self.tabla_origen}'
        """
        
        columnas_origen = {row[0]: row[1] for row in pg_hook_origen.get_records(query_origen)}
        
        # Verificar tabla destino
        query_destino = f"""
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_name = '{self.tabla_destino}'
        """
        
        columnas_destino = {row[0]: row[1] for row in pg_hook_destino.get_records(query_destino)}
        
        # Validar columnas en origen
        error_origen = False
        mensajes_origen = []
        
        for col, tipo in columnas_esperadas_origen.items():
            if col not in columnas_origen:
                error_origen = True
                mensajes_origen.append(f"Columna '{col}' no existe")
            elif columnas_origen[col] != tipo:
                error_origen = True
                mensajes_origen.append(f"Columna '{col}' tiene tipo '{columnas_origen[col]}' pero se esperaba '{tipo}'")
        
        # Validar columnas en destino
        error_destino = False
        mensajes_destino = []
        
        for col, tipo in columnas_esperadas_destino.items():
            if col not in columnas_destino:
                error_destino = True
                mensajes_destino.append(f"Columna '{col}' no existe")
            elif columnas_destino[col] != tipo:
                error_destino = True
                mensajes_destino.append(f"Columna '{col}' tiene tipo '{columnas_destino[col]}' pero se esperaba '{tipo}'")
        
        return {
            "valido": not (error_origen or error_destino),
            "mensaje": "Esquemas válidos" if not (error_origen or error_destino) else "Existen problemas en los esquemas",
            "origen": {
                "valido": not error_origen,
                "tabla": self.tabla_origen,
                "errores": mensajes_origen,
                "columnas_actuales": columnas_origen,
                "columnas_esperadas": columnas_esperadas_origen
            },
            "destino": {
                "valido": not error_destino,
                "tabla": self.tabla_destino,
                "errores": mensajes_destino,
                "columnas_actuales": columnas_destino,
                "columnas_esperadas": columnas_esperadas_destino
            }
        }
            
    except Exception as e:
        logger.error(f"❌ Error validando esquemas: {str(e)}")
        return {
            "valido": False,
            "mensaje": f"Error en validación de esquemas: {str(e)}"
        }

def verificar_consistencia_temporal(self, mes: int, ano: int = 2019, 
                                  tabla: str = None) -> Dict[str, Any]:
    """
    Verifica la consistencia temporal de los datos (huecos, secuencia, etc).
    
    Args:
        mes: Número de mes a verificar (1-12)
        ano: Año a verificar
        tabla: Nombre de la tabla a verificar (si None, se usa tabla_origen)
        
    Returns:
        Dict con resultados de verificación
    """
    try:
        if tabla is None:
            tabla = self.tabla_origen
            
        pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
        
        # Determinar fecha inicio y fin del mes
        mes_siguiente = mes + 1 if mes < 12 else 1
        ano_siguiente = ano if mes < 12 else ano + 1
        fecha_inicio = f"{ano}-{mes:02d}-01"
        fecha_fin = f"{ano_siguiente}-{mes_siguiente:02d}-01"
        
        # 1. Verificar huecos en la secuencia temporal
        query_huecos = f"""
        WITH expected_dates AS (
            SELECT generate_series(
                '{fecha_inicio}'::date, 
                '{fecha_fin}'::date - interval '1 day', 
                interval '1 day'
            )::date AS fecha
        ),
        expected_hours AS (
            SELECT fecha, generate_series(0, 23) AS hora
            FROM expected_dates
        ),
        actual_data AS (
            SELECT DISTINCT fecha, hora
            FROM {tabla}
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
        )
        SELECT COUNT(*) as total_faltantes
        FROM expected_hours e
        LEFT JOIN actual_data a 
            ON e.fecha = a.fecha AND e.hora = a.hora
        WHERE a.fecha IS NULL
        """
        
        total_huecos = pg_hook.get_first(query_huecos)[0]
        total_esperado = pg_hook.get_first(f"SELECT (DATE_PART('day', '{fecha_fin}'::date - '{fecha_inicio}'::date) * 24)")[0]
        
        # 2. Verificar duplicados temporales
        query_duplicados = f"""
        SELECT 
            COUNT(*) as total_combinaciones_duplicadas
        FROM (
            SELECT fecha, hora, COUNT(*) 
            FROM {tabla} 
            WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
            GROUP BY fecha, hora
            HAVING COUNT(*) > 1
        ) as duplicados
        """
        
        total_duplicados = pg_hook.get_first(query_duplicados)[0]
        
        # 3. Verificar valores atípicos en secuencia temporal
        query_saltos = f"""
        WITH data_ordered AS (
            SELECT 
                fecha, 
                hora,
                LAG(fecha) OVER (ORDER BY fecha, hora) as prev_fecha,
                LAG(hora) OVER (ORDER BY fecha, hora) as prev_hora
            FROM (
                SELECT DISTINCT fecha, hora
                FROM {tabla}
                WHERE fecha >= '{fecha_inicio}' AND fecha < '{fecha_fin}'
                ORDER BY fecha, hora
            ) t
        )
        SELECT COUNT(*) as saltos_temporales
        FROM data_ordered
        WHERE 
            prev_fecha IS NOT NULL AND (
                (fecha = prev_fecha AND hora > prev_hora + 1) OR
                (fecha > prev_fecha AND hora < 23 AND prev_hora < 23)
            )
        """
        
        saltos_temporales = pg_hook.get_first(query_saltos)[0]
        
        # Calcular porcentaje de completitud temporal
        porcentaje_completitud = ((total_esperado - total_huecos) / total_esperado * 100) if total_esperado > 0 else 0
        
        return {
            "valido": total_huecos == 0 and total_duplicados == 0 and saltos_temporales == 0,
            "mensaje": "Secuencia temporal correcta" if (total_huecos == 0 and total_duplicados == 0) 
                      else "Se encontraron problemas en la secuencia temporal",
            "completitud_temporal": porcentaje_completitud,
            "total_horas_esperadas": total_esperado,
            "total_huecos": total_huecos,
            "total_duplicados_temporales": total_duplicados,
            "saltos_temporales": saltos_temporales
        }
            
    except Exception as e:
        logger.error(f"❌ Error verificando consistencia temporal: {str(e)}")
        return {
            "valido": False,
            "mensaje": f"Error en verificación: {str(e)}",
            "error": str(e)
        }

def verificar_completitud_mes(self, mes: int, ano: int = 2019, 
                           umbral_completitud: float = 95.0) -> Tuple[bool, float]:
    """
    Verifica la completitud del procesamiento para un mes específico.
    
    Args:
        mes: Número de mes a verificar (1-12)
        ano: Año a verificar
        umbral_completitud: Porcentaje mínimo para considerar el procesamiento completo
        
    Returns:
        Tuple[bool, float]: (es_completo, porcentaje_completitud)
    """
    try:
        pg_hook_origen = PostgresHook(postgres_conn_id=self.conexion_origen)
        pg_hook_destino = PostgresHook(postgres_conn_id=self.conexion_destino)
        
        # Consulta para contar registros en origen
        query_origen = f"""
        SELECT COUNT(*) 
        FROM {self.tabla_origen}
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        
        # Consulta para contar registros en destino
        query_destino = f"""
        SELECT COUNT(*) 
        FROM {self.tabla_destino}
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        
        count_origen = pg_hook_origen.get_first(query_origen)[0]
        count_destino = pg_hook_destino.get_first(query_destino)[0]
        
        # Si no hay datos en origen, se considera completo
        if count_origen == 0:
            logger.warning(f"⚠️ No hay datos en origen para el mes {mes}")
            return True, 100.0
        
        # Calcular porcentaje de completitud
        porcentaje = (count_destino / count_origen) * 100
        
        # Verificar completitud según umbral
        es_completo = porcentaje >= umbral_completitud
        
        if es_completo:
            logger.info(f"✅ Mes {mes} procesado completamente: {porcentaje:.2f}%")
        else:
            logger.warning(f"⚠️ Mes {mes} procesado parcialmente: {porcentaje:.2f}%")
        
        return es_completo, porcentaje
        
    except Exception as e:
        logger.error(f"❌ Error verificando completitud del mes: {str(e)}")
        return False, 0.0

def verificar_existencia_datos_fuente(self, mes: int, ano: int = 2019) -> bool:
    """
    Verifica si existen datos para un mes específico en la tabla origen.
    
    Args:
        mes: Número de mes a verificar (1-12)
        ano: Año a verificar
        
    Returns:
        bool: True si existen datos, False en caso contrario
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=self.conexion_origen)
        
        query = f"""
        SELECT COUNT(*) 
        FROM {self.tabla_origen}
        WHERE EXTRACT(MONTH FROM fecha) = {mes}
        AND EXTRACT(YEAR FROM fecha) = {ano}
        """
        
        count = pg_hook.get_first(query)[0]
        
        if count > 0:
            logger.info(f"✅ Existen {count} registros para el mes {mes} año {ano}")
            return True
        else:
            logger.warning(f"⚠️ No existen datos para el mes {mes} año {ano}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error verificando existencia de datos: {str(e)}")
        return False

def generar_informe_meses_sin_datos(self, ano: int = 2019) -> List[int]:
    """
    Identifica qué meses no tienen datos disponibles en el origen.
    
    Args:
        ano: Año a verificar
        
    Returns:
        List[int]: Lista de números de meses sin datos
    """
    try:
        meses_sin_datos = []
        
        # Verificar disponibilidad para cada mes
        for mes in range(1, 13):
            tiene_datos = self.verificar_existencia_datos_fuente(mes, ano)
            if not tiene_datos:
                meses_sin_datos.append(mes)
                logger.info(f"⚠️ El mes {mes} ({calendar.month_name[mes]}) no tiene datos disponibles")
        
        # Generar mensaje informativo
        if meses_sin_datos:
            logger.info(f"ℹ️ Se encontraron {len(meses_sin_datos)} meses sin datos: {meses_sin_datos}")
            logger.info("Estos meses serán marcados como 'procesados' sin necesidad de procesamiento real")
        else:
            logger.info("✅ Todos los meses tienen datos disponibles para procesamiento")
        
        return meses_sin_datos
        
    except Exception as e:
        logger.error(f"❌ Error generando informe de meses sin datos: {str(e)}")
        return []
    
def verificar_integridad_tabla_original(self, 
                                      tabla_origen: str = "potencia_dep",
                                      tabla_original: str = "potencia_dep_original") -> Dict[str, Any]:
    """
    Verifica la integridad de la tabla original comparándola con su backup.
    
    Args:
        tabla_origen: Nombre de la tabla a verificar
        tabla_original: Nombre de la tabla respaldo original
        
    Returns:
        Dict[str, Any]: Resultado de la verificación
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
        
        # 1. Verificar existencia de tablas
        origen_existe = pg_hook.get_first(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_origen}')"
        )[0]
        
        original_existe = pg_hook.get_first(
            f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tabla_original}')"
        )[0]
        
        if not origen_existe and not original_existe:
            return {
                "estado": "error",
                "mensaje": "No existe ni la tabla original ni su respaldo",
                "accion_requerida": "creacion_manual"
            }
        
        if not original_existe and origen_existe:
            logger.info(f"⚠️ No se encontró respaldo {tabla_original}. Se creará desde la tabla existente.")
            return {
                "estado": "crear_respaldo",
                "mensaje": f"No existe respaldo {tabla_original}, pero sí la tabla {tabla_origen}",
                "accion_requerida": "crear_respaldo"
            }
        
        if not origen_existe and original_existe:
            logger.info(f"⚠️ Tabla {tabla_origen} no existe pero sí su respaldo. Restaurando...")
            return {
                "estado": "restaurar",
                "mensaje": f"No existe tabla {tabla_origen}, pero sí su respaldo",
                "accion_requerida": "restaurar_desde_respaldo"
            }
        
        # 2. Verificar integridad comparando conteos
        count_origen = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_origen}")[0]
        count_backup = pg_hook.get_first(f"SELECT COUNT(*) FROM {tabla_original}")[0]
        
        logger.info(f"📊 Conteo en {tabla_origen}: {count_origen}")
        logger.info(f"📊 Conteo en {tabla_original}: {count_backup}")
        
        # Si hay una gran diferencia, sugerir restauración
        if count_origen < count_backup * 0.9:  # Si tiene menos del 90% de registros
            logger.warning(f"⚠️ Tabla {tabla_origen} parece inconsistente ({count_origen} vs {count_backup})")
            
            return {
                "estado": "inconsistente",
                "mensaje": f"Tabla {tabla_origen} tiene menos del 90% de registros que su respaldo",
                "accion_requerida": "restaurar_desde_respaldo",
                "conteo_origen": count_origen,
                "conteo_backup": count_backup
            }
        
        # 3. Verificar datos atípicos por mes
        inconsistencias_mes = []
        for mes in range(1, 13):
            query_mes_origen = f"""
            SELECT COUNT(*) FROM {tabla_origen}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = 2019
            """
            
            query_mes_backup = f"""
            SELECT COUNT(*) FROM {tabla_original}
            WHERE EXTRACT(MONTH FROM fecha) = {mes}
            AND EXTRACT(YEAR FROM fecha) = 2019
            """
            
            count_mes_origen = pg_hook.get_first(query_mes_origen)[0]
            count_mes_backup = pg_hook.get_first(query_mes_backup)[0]
            
            # Si hay diferencia significativa en algún mes
            if count_mes_origen < count_mes_backup * 0.95:
                inconsistencias_mes.append({
                    "mes": mes,
                    "nombre_mes": calendar.month_name[mes],
                    "conteo_origen": count_mes_origen,
                    "conteo_backup": count_mes_backup,
                    "diferencia_porcentual": 
                        (1 - count_mes_origen / count_mes_backup) * 100 if count_mes_backup > 0 else 0
                })
        
        if inconsistencias_mes:
            logger.warning(f"⚠️ Se encontraron inconsistencias en {len(inconsistencias_mes)} meses")
            return {
                "estado": "inconsistencia_parcial",
                "mensaje": "Existen inconsistencias en algunos meses",
                "accion_requerida": "evaluar_restauracion",
                "conteo_origen": count_origen,
                "conteo_backup": count_backup,
                "inconsistencias_mes": inconsistencias_mes
            }
        
        return {
            "estado": "integro",
            "mensaje": f"Tabla {tabla_origen} íntegra y consistente con su respaldo",
            "accion_requerida": "ninguna",
            "conteo_origen": count_origen,
            "conteo_backup": count_backup
        }
        
    except Exception as e:
        logger.error(f"❌ Error verificando integridad de tabla original: {str(e)}")
        return {
            "estado": "error",
            "mensaje": f"Error en verificación: {str(e)}",
            "accion_requerida": "revisar_error"
        }
    
def aplicar_accion_integridad(self, accion: str, 
                            tabla_origen: str = "potencia_dep",
                            tabla_original: str = "potencia_dep_original") -> bool:
    """
    Aplica la acción recomendada tras verificar integridad.
    
    Args:
        accion: Acción a realizar (crear_respaldo, restaurar_desde_respaldo, etc)
        tabla_origen: Nombre de la tabla principal
        tabla_original: Nombre de la tabla respaldo
        
    Returns:
        bool: True si la acción se realizó correctamente
    """
    try:
        pg_hook = PostgresHook(postgres_conn_id=self.origen_conn_id)
        
        if accion == "crear_respaldo":
            logger.info(f"📋 Creando respaldo inicial {tabla_original}...")
            pg_hook.run(f"CREATE TABLE {tabla_original} AS SELECT * FROM {tabla_origen}")
            logger.info("✅ Respaldo inicial creado exitosamente")
            return True
            
        elif accion in ["restaurar_desde_respaldo", "restaurar"]:
            # Crear respaldo de emergencia por si acaso
            backup_temporal = f"{tabla_origen}_backup_{int(time.time())}"
            try:
                pg_hook.run(f"CREATE TABLE {backup_temporal} AS SELECT * FROM {tabla_origen}")
                logger.info(f"📋 Backup de emergencia creado: {backup_temporal}")
            except:
                logger.warning(f"⚠️ No se pudo crear backup de emergencia (posiblemente tabla origen no existe)")
            
            # Restaurar desde respaldo original
            logger.info(f"🔄 Restaurando {tabla_origen} desde respaldo {tabla_original}...")
            pg_hook.run(f"DROP TABLE IF EXISTS {tabla_origen}")
            pg_hook.run(f"CREATE TABLE {tabla_origen} AS SELECT * FROM {tabla_original}")
            logger.info(f"✅ Tabla {tabla_origen} restaurada completamente desde respaldo")
            return True
            
        elif accion == "ninguna":
            logger.info("✅ No se requiere acción, tablas íntegras")
            return True
            
        else:
            logger.warning(f"⚠️ Acción '{accion}' no reconocida o no implementada")
            return False
            
    except Exception as e:
        logger.error(f"❌ Error aplicando acción de integridad: {str(e)}")
        return False