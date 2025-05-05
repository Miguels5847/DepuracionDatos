from typing import Dict, List, Optional, Union, Tuple, Any, Set
import logging
import os
from datetime import datetime

from modules.db_utils import DbUtils
from modules.validation_utils import ValidationUtils
from modules.report_utils import ReportUtils
# Comentado para evitar error de importación
# from modules.report_utils import ReportUtils
# from modules.data_utils import DataUtils

logger = logging.getLogger(__name__)

class CentroSurUtils:
    """
    Clase integradora para todas las utilidades del sistema.
    
    Proporciona una interfaz unificada a todas las funcionalidades:
    - Operaciones de base de datos (DbUtils)
    - Validación de datos (ValidationUtils)
    - Generación de informes (ReportUtils)
    - Manipulación de datos (DataUtils)
    """
    
    def __init__(self, 
                 origen_conn_id: str = "postgres_centrosur",
                 destino_conn_id: str = "postgres_base_prueba",
                 tabla_origen: str = "potencia_dep",
                 tabla_destino: str = "potencia_dep_processed"):
        """
        Inicializa la clase CentroSurUtils con instancias de todas las utilidades.
        
        Args:
            origen_conn_id: ID de la conexión a la base de datos origen
            destino_conn_id: ID de la conexión a la base de datos destino
            tabla_origen: Nombre de la tabla origen
            tabla_destino: Nombre de la tabla destino
        """
        # Inicializar todas las utilidades con parámetros comunes
        self.db_utils = DbUtils(
            origen_conn_id=origen_conn_id,
            destino_conn_id=destino_conn_id
        )
        
        self.validation_utils = ValidationUtils(
            tabla_origen=tabla_origen,
            tabla_destino=tabla_destino,
            conexion_origen=origen_conn_id,
            conexion_destino=destino_conn_id
        )
        
        self.report_utils = ReportUtils(
            origen_conn_id=origen_conn_id,
            destino_conn_id=destino_conn_id,
            tabla_origen=tabla_origen,
            tabla_destino=tabla_destino
        )
        
        self.data_utils = DataUtils(
            origen_conn_id=origen_conn_id,
            destino_conn_id=destino_conn_id
        )
        
        # Guardar parámetros principales para uso futuro
        self.origen_conn_id = origen_conn_id
        self.destino_conn_id = destino_conn_id
        self.tabla_origen = tabla_origen
        self.tabla_destino = tabla_destino
        
        logger.info("✅ CentroSurUtils inicializado correctamente")
    
    def setup_initial_environment(self) -> bool:
        """
        Configura el entorno inicial para el procesamiento.
        
        - Verifica conexiones
        - Valida esquemas de tablas
        - Genera carpetas necesarias
        
        Returns:
            bool: True si la configuración fue exitosa
        """
        try:
            # 1. Verificar conexiones
            if not self.db_utils.verificar_conexiones():
                logger.error("❌ Error verificando conexiones a bases de datos")
                return False
            
            # 2. Validar esquemas de tablas
            validacion_schema = self.validation_utils.validar_schema_tablas()
            if not validacion_schema.get("valido", False):
                logger.error(f"❌ Error en esquemas de tablas: {validacion_schema.get('mensaje', '')}")
                return False
            
            # 3. Crear directorios para informes y exportaciones
            directorios = ["logs", "informes", "exports", "logs/validaciones"]
            for directorio in directorios:
                os.makedirs(os.path.join(os.getcwd(), directorio), exist_ok=True)
            
            logger.info("✅ Entorno inicial configurado correctamente")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error configurando entorno inicial: {str(e)}")
            return False
    
    def process_month(self, mes: int, ano: int = 2019, 
                     forzar_reproceso: bool = False) -> Dict[str, Any]:
        """
        Procesa un mes específico de forma integral.
        
        Args:
            mes: Número de mes a procesar (1-12)
            ano: Año a procesar
            forzar_reproceso: Si True, reprocesa incluso si ya está procesado
            
        Returns:
            Dict con resultados del procesamiento
        """
        try:
            logger.info(f"🔄 Iniciando procesamiento del mes {mes} año {ano}")
            resultado = {
                "mes": mes,
                "ano": ano,
                "inicio_proceso": datetime.now().isoformat(),
                "completado": False,
                "pasos_completados": [],
                "errores": []
            }
            
            # 1. Verificar si el mes ya está procesado
            if not forzar_reproceso:
                ya_procesado = self.db_utils.verificar_mes_procesado(mes)
                if ya_procesado:
                    logger.info(f"ℹ️ El mes {mes} ya está procesado. Use forzar_reproceso=True para reprocesar")
                    resultado["completado"] = True
                    resultado["mensaje"] = "Mes ya procesado"
                    return resultado
            
            # 2. Verificar existencia de datos
            existe_datos = self.validation_utils.verificar_existencia_datos_fuente(mes, ano)
            if not existe_datos:
                logger.warning(f"⚠️ No existen datos para el mes {mes}")
                resultado["completado"] = True
                resultado["mensaje"] = "No existen datos para este mes"
                return resultado
                
            resultado["pasos_completados"].append("verificacion_datos")
            
            # 3. Crear tabla temporal de trabajo
            nombre_tabla_temp = self.db_utils.crear_tabla_temporal(mes)
            if not nombre_tabla_temp:
                error_msg = "No se pudo crear tabla temporal"
                logger.error(f"❌ {error_msg}")
                resultado["errores"].append(error_msg)
                return resultado
                
            resultado["tabla_temporal"] = nombre_tabla_temp
            resultado["pasos_completados"].append("creacion_tabla_temporal")
            
            # 4. Validar datos antes de procesamiento
            validacion_previa = self.validation_utils.validar_datos_procesados(mes, ano)
            resultado["validacion_previa"] = validacion_previa
            resultado["pasos_completados"].append("validacion_previa")
            
            # 5. Configurar y ejecutar procesamiento en NiFi
            # Nota: Esta parte normalmente se implementaría como parte de un DAG de Airflow
            # y se invocaría desde allí, no directamente desde CentroSurUtils
            
            # 6. Verificar completitud del procesamiento
            es_completo, porcentaje = self.validation_utils.verificar_completitud_mes(mes, ano)
            resultado["completitud"] = {
                "completo": es_completo,
                "porcentaje": porcentaje
            }
            resultado["pasos_completados"].append("verificacion_completitud")
            
            # 7. Generar informe del mes
            try:
                informe_html = self.report_utils.generar_informe_html(ano)
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                nombre_archivo = f"informe_mes{mes}_ano{ano}_{timestamp}.html"
                ruta_informe = os.path.join(os.getcwd(), "informes", nombre_archivo)
                
                with open(ruta_informe, "w", encoding="utf-8") as f:
                    f.write(informe_html)
                    
                resultado["informe_generado"] = ruta_informe
                resultado["pasos_completados"].append("generacion_informe")
            except Exception as e:
                logger.error(f"⚠️ Error generando informe, pero el procesamiento continuará: {str(e)}")
                resultado["errores"].append(f"Error en informe: {str(e)}")
            
            # 8. Limpiar recursos temporales
            if nombre_tabla_temp:
                self.db_utils.eliminar_tabla(nombre_tabla_temp)
                resultado["pasos_completados"].append("limpieza_recursos")
            
            # Marcar como completado
            resultado["completado"] = True
            resultado["fin_proceso"] = datetime.now().isoformat()
            logger.info(f"✅ Procesamiento del mes {mes} completado exitosamente")
            
            return resultado
            
        except Exception as e:
            logger.error(f"❌ Error procesando mes {mes}: {str(e)}")
            if "resultado" in locals():
                resultado["errores"].append(str(e))
                resultado["completado"] = False
                resultado["fin_proceso"] = datetime.now().isoformat()
                return resultado
            else:
                return {
                    "mes": mes,
                    "ano": ano,
                    "completado": False,
                    "errores": [str(e)]
                }
    
    def generate_complete_report(self, ano: int = 2019) -> Dict[str, Any]:
        """
        Genera un informe completo del año incluyendo todos los meses.
        
        Args:
            ano: Año a analizar
            
        Returns:
            Dict con información del informe
        """
        try:
            # 1. Generar informe HTML
            informe_html = self.report_utils.generar_informe_html(ano)
            
            # 2. Guardar en archivo
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_archivo = f"informe_anual_{ano}_{timestamp}.html"
            ruta_informe = os.path.join(os.getcwd(), "informes", nombre_archivo)
            
            with open(ruta_informe, "w", encoding="utf-8") as f:
                f.write(informe_html)
            
            # 3. Generar gráficos adicionales
            grafico_tendencia = self.report_utils.generar_grafico_tendencia(ano)
            
            if grafico_tendencia:
                # Guardar gráfico por separado
                ruta_grafico = os.path.join(os.getcwd(), "informes", f"tendencia_{ano}_{timestamp}.png")
                
                # Decodificar y guardar
                import base64
                with open(ruta_grafico, "wb") as f:
                    f.write(base64.b64decode(grafico_tendencia))
            
            # 4. Exportar datos de soporte
            datos_exportados = {}
            
            for mes in range(1, 13):
                # Verificar si hay datos para el mes
                hay_datos = self.validation_utils.verificar_existencia_datos_fuente(mes, ano)
                
                if hay_datos:
                    # Exportar a CSV
                    ruta_csv = self.data_utils.exportar_datos_a_csv(mes, ano)
                    if ruta_csv:
                        datos_exportados[mes] = ruta_csv
            
            logger.info(f"✅ Informe anual para {ano} generado en {ruta_informe}")
            
            # 5. Resultado
            return {
                "ano": ano,
                "informe_html": ruta_informe,
                "grafico_tendencia": ruta_grafico if grafico_tendencia else None,
                "datos_exportados": datos_exportados,
                "timestamp": timestamp
            }
            
        except Exception as e:
            logger.error(f"❌ Error generando informe completo: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error generando informe: {str(e)}"
            }
    
    def execute_end_to_end_process(self, ano: int = 2019, 
                                 meses: List[int] = None,
                                 forzar_reproceso: bool = False) -> Dict[str, Any]:
        """
        Ejecuta el proceso completo de principio a fin para los meses especificados.
        
        Args:
            ano: Año a procesar
            meses: Lista de meses a procesar (si None, procesa todos)
            forzar_reproceso: Si True, reprocesa incluso si ya está procesado
            
        Returns:
            Dict con resultados del procesamiento
        """
        try:
            # Si no se especifican meses, procesar todos
            if meses is None:
                meses = list(range(1, 13))
                
            # Resultados generales
            resultados = {
                "ano": ano,
                "inicio_proceso": datetime.now().isoformat(),
                "meses_procesados": {},
                "meses_con_error": [],
                "meses_sin_datos": []
            }
            
            # 1. Configurar entorno inicial
            if not self.setup_initial_environment():
                return {
                    "error": True,
                    "mensaje": "Error configurando entorno inicial"
                }
            
            # 2. Procesamiento por mes
            for mes in meses:
                logger.info(f"🔄 Procesando mes {mes}/{ano}")
                resultado_mes = self.process_month(mes, ano, forzar_reproceso)
                
                resultados["meses_procesados"][mes] = resultado_mes
                
                if not resultado_mes.get("completado", False):
                    resultados["meses_con_error"].append(mes)
                    
                if "No existen datos para este mes" in resultado_mes.get("mensaje", ""):
                    resultados["meses_sin_datos"].append(mes)
            
            # 3. Verificar completitud anual
            completitud_anual = self.db_utils.verificar_completitud_anual()
            resultados["completitud_anual"] = completitud_anual
            
            # 4. Generar informe completo
            if len(resultados["meses_con_error"]) == 0:
                # Solo generar informe si no hubo errores
                informe_anual = self.generate_complete_report(ano)
                resultados["informe_anual"] = informe_anual
            
            # 5. Limpiar tablas temporales
            self.db_utils.limpiar_tablas_temporales()
            
            resultados["fin_proceso"] = datetime.now().isoformat()
            logger.info(f"✅ Procesamiento anual completado. Meses procesados: {len(meses) - len(resultados['meses_con_error'])}/{len(meses)}")
            
            return resultados
            
        except Exception as e:
            logger.error(f"❌ Error en procesamiento completo: {str(e)}")
            return {
                "error": True,
                "mensaje": f"Error en procesamiento: {str(e)}"
            }