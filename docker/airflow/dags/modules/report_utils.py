from typing import Dict, List, Optional, Union, Tuple, Any
import logging
import os
from datetime import datetime
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook

logger = logging.getLogger(__name__)

class ReportUtils:
    """Clase para generación de informes sin dependencia de matplotlib."""
    
    def __init__(self, 
                origen_conn_id: str = "postgres_centrosur", 
                destino_conn_id: str = "postgres_base_prueba",
                tabla_origen: str = "potencia_dep",
                tabla_destino: str = "potencia_dep_processed"):
        """
        Inicializa la clase ReportUtils.
        
        Args:
            origen_conn_id: ID de la conexión a la base de datos origen
            destino_conn_id: ID de la conexión a la base de datos destino
            tabla_origen: Nombre de la tabla origen de datos
            tabla_destino: Nombre de la tabla destino de datos procesados
        """
        self.origen_conn_id = origen_conn_id
        self.destino_conn_id = destino_conn_id
        self.tabla_origen = tabla_origen
        self.tabla_destino = tabla_destino
    
    def generar_grafico_tendencia(self, ano: int = 2019, 
                               meses: List[int] = None, 
                               alimentador: str = None) -> str:
        """
        Versión simplificada que no genera gráficos reales.
        
        Args:
            ano: Año a graficar
            meses: Lista de meses a incluir (si None, todos)
            alimentador: Filtrar por alimentador específico
            
        Returns:
            str: Representación en HTML de los datos (no imagen)
        """
        try:
            logger.info(f"ℹ️ Generación de gráfico de tendencia simplificada (sin matplotlib)")
            
            # Si no se especifican meses, usar todos
            if meses is None:
                meses = list(range(1, 13))
            
            # Obtener datos
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # Construir filtro de alimentador
            filtro_alimentador = f"AND alimentador = '{alimentador}'" if alimentador else ""
            
            # Consulta para obtener promedios mensuales
            query = f"""
            SELECT EXTRACT(MONTH FROM fecha) as mes,
                   AVG(potencia_activa) as promedio_activa,
                   AVG(potencia_reactiva) as promedio_reactiva
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            AND EXTRACT(MONTH FROM fecha) IN ({','.join(map(str, meses))})
            {filtro_alimentador}
            GROUP BY mes
            ORDER BY mes
            """
            
            # Ejecutar consulta
            resultados = pg_hook.get_records(query)
            
            # Convertir a HTML simple
            html = """
            <div class="tendencia-container">
                <h3>Tendencia de Potencia</h3>
                <table border="1" class="tendencia-table">
                    <thead>
                        <tr>
                            <th>Mes</th>
                            <th>Potencia Activa (Promedio)</th>
                            <th>Potencia Reactiva (Promedio)</th>
                        </tr>
                    </thead>
                    <tbody>
            """
            
            for mes, prom_activa, prom_reactiva in resultados:
                html += f"""
                        <tr>
                            <td>{int(mes)}</td>
                            <td>{prom_activa:.2f}</td>
                            <td>{prom_reactiva:.2f}</td>
                        </tr>
                """
            
            html += """
                    </tbody>
                </table>
                <p><i>Nota: Versión de texto simplificada. Los gráficos visuales están disponibles en Power BI.</i></p>
            </div>
            """
            
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generando tendencia: {str(e)}")
            return f"<div class='error'>Error al generar tendencia: {str(e)}</div>"
    
    def generar_informe_html(self, ano: int = 2019) -> str:
        """
        Genera un informe HTML completo del año.
        
        Args:
            ano: Año a incluir en el informe
            
        Returns:
            str: Contenido HTML del informe
        """
        try:
            logger.info(f"🔄 Generando informe HTML para el año {ano}")
            
            # 1. Obtener resumen general
            resumen = self._obtener_resumen_general(ano)
            
            # 2. Obtener estadísticas por mes
            stats_mes = self._obtener_estadisticas_por_mes(ano)
            
            # 3. Obtener top alimentadores
            top_alimentadores = self._obtener_top_alimentadores(ano)
            
            # 4. Generar HTML
            html = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Informe de Potencia {ano}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    h1, h2, h3 {{ color: #003366; }}
                    .container {{ max-width: 1200px; margin: 0 auto; }}
                    .resumen {{ background-color: #f0f0f0; padding: 15px; border-radius: 5px; margin-bottom: 20px; }}
                    table {{ border-collapse: collapse; width: 100%; margin-bottom: 20px; }}
                    th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #003366; color: white; }}
                    tr:hover {{ background-color: #f5f5f5; }}
                    .completitud-alta {{ color: green; }}
                    .completitud-media {{ color: orange; }}
                    .completitud-baja {{ color: red; }}
                    .footer {{ margin-top: 30px; font-size: 0.8em; color: #666; }}
                </style>
            </head>
            <body>
                <div class="container">
                    <h1>Informe de Procesamiento de Datos de Potencia - {ano}</h1>
                    <p>Generado el: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
                    
                    <div class="resumen">
                        <h2>Resumen General</h2>
                        <p>Total registros: {resumen.get('total_registros', 0):,}</p>
                        <p>Total alimentadores: {resumen.get('total_alimentadores', 0)}</p>
                        <p>Completitud global: {resumen.get('completitud_global', 0):.2f}%</p>
                        <p>Meses con datos: {resumen.get('meses_con_datos', 0)}/12</p>
                    </div>
                    
                    <h2>Estadísticas por Mes</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Mes</th>
                                <th>Registros</th>
                                <th>Alimentadores</th>
                                <th>Completitud</th>
                                <th>Estado</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # Agregar filas para cada mes
            for mes, stats in stats_mes.items():
                completitud = stats.get('completitud', 0)
                if completitud >= 90:
                    clase_completitud = "completitud-alta"
                    estado = "Completo"
                elif completitud >= 70:
                    clase_completitud = "completitud-media"
                    estado = "Parcial"
                else:
                    clase_completitud = "completitud-baja"
                    estado = "Incompleto"
                
                html += f"""
                            <tr>
                                <td>{mes}</td>
                                <td>{stats.get('registros', 0):,}</td>
                                <td>{stats.get('alimentadores', 0)}</td>
                                <td class="{clase_completitud}">{completitud:.2f}%</td>
                                <td>{estado}</td>
                            </tr>
                """
            
            html += """
                        </tbody>
                    </table>
                    
                    <h2>Top Alimentadores</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Alimentador</th>
                                <th>Registros</th>
                                <th>Potencia Activa (Promedio)</th>
                                <th>Potencia Reactiva (Promedio)</th>
                            </tr>
                        </thead>
                        <tbody>
            """
            
            # Agregar filas para cada alimentador
            for alim, stats in top_alimentadores.items():
                html += f"""
                            <tr>
                                <td>{alim}</td>
                                <td>{stats.get('registros', 0):,}</td>
                                <td>{stats.get('promedio_activa', 0):.2f}</td>
                                <td>{stats.get('promedio_reactiva', 0):.2f}</td>
                            </tr>
                """
            
            html += """
                        </tbody>
                    </table>
                    
                    <div class="footer">
                        <p>Informe generado automáticamente por CentroSur Utilities.</p>
                        <p>Para análisis gráficos detallados, consulte los informes en Power BI.</p>
                    </div>
                </div>
            </body>
            </html>
            """
            
            logger.info(f"✅ Informe HTML generado correctamente")
            return html
            
        except Exception as e:
            logger.error(f"❌ Error generando informe HTML: {str(e)}")
            return f"""
            <!DOCTYPE html>
            <html>
            <head><title>Error en Informe</title></head>
            <body>
                <h1>Error al generar informe</h1>
                <p>{str(e)}</p>
            </body>
            </html>
            """
    
    def _obtener_resumen_general(self, ano: int) -> Dict[str, Any]:
        """Obtiene resumen general de los datos del año."""
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            # Total de registros
            query_total = f"""
            SELECT COUNT(*)
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            total_registros = pg_hook.get_first(query_total)[0]
            
            # Total de alimentadores
            query_alimentadores = f"""
            SELECT COUNT(DISTINCT alimentador)
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            total_alimentadores = pg_hook.get_first(query_alimentadores)[0]
            
            # Meses con datos
            query_meses = f"""
            SELECT COUNT(DISTINCT EXTRACT(MONTH FROM fecha))
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            """
            meses_con_datos = pg_hook.get_first(query_meses)[0]
            
            # Completitud (estimación)
            completitud_global = (meses_con_datos / 12) * 100 if meses_con_datos else 0
            
            return {
                "total_registros": total_registros,
                "total_alimentadores": total_alimentadores,
                "meses_con_datos": meses_con_datos,
                "completitud_global": completitud_global
            }
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo resumen general: {str(e)}")
            return {}
    
    def _obtener_estadisticas_por_mes(self, ano: int) -> Dict[int, Dict[str, Any]]:
        """Obtiene estadísticas por mes."""
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            query = f"""
            SELECT 
                EXTRACT(MONTH FROM fecha)::int as mes,
                COUNT(*) as registros,
                COUNT(DISTINCT alimentador) as alimentadores,
                COUNT(*) * 100.0 / 
                    (COUNT(DISTINCT alimentador) * COUNT(DISTINCT fecha) * 24) as completitud
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            GROUP BY mes
            ORDER BY mes
            """
            
            resultados = pg_hook.get_records(query)
            
            # Convertir a diccionario
            stats = {}
            for mes, registros, alimentadores, completitud in resultados:
                stats[mes] = {
                    "registros": registros,
                    "alimentadores": alimentadores,
                    "completitud": completitud
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo estadísticas por mes: {str(e)}")
            return {}
    
    def _obtener_top_alimentadores(self, ano: int, limit: int = 10) -> Dict[str, Dict[str, Any]]:
        """Obtiene top alimentadores por volumen de datos."""
        try:
            pg_hook = PostgresHook(postgres_conn_id=self.destino_conn_id)
            
            query = f"""
            SELECT 
                alimentador,
                COUNT(*) as registros,
                AVG(potencia_activa) as promedio_activa,
                AVG(potencia_reactiva) as promedio_reactiva
            FROM {self.tabla_destino}
            WHERE EXTRACT(YEAR FROM fecha) = {ano}
            GROUP BY alimentador
            ORDER BY registros DESC
            LIMIT {limit}
            """
            
            resultados = pg_hook.get_records(query)
            
            # Convertir a diccionario
            stats = {}
            for alimentador, registros, prom_activa, prom_reactiva in resultados:
                stats[alimentador] = {
                    "registros": registros,
                    "promedio_activa": prom_activa,
                    "promedio_reactiva": prom_reactiva
                }
            
            return stats
            
        except Exception as e:
            logger.error(f"❌ Error obteniendo top alimentadores: {str(e)}")
            return {}