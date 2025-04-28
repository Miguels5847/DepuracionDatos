#!/usr/bin/env python3
# mini_proxy_debug.py - Versión con depuración completa
from flask import Flask, request, jsonify
import subprocess
import os
import logging
import json
import traceback
import re
import datetime
import time
import functools
# Configuración básica
app = Flask(__name__)
logging.basicConfig(
    level=logging.DEBUG,  # Se Cambia el nivel de logging a DEBUG para más detalles
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("mini-nifi-proxy")

# Variables de entorno 
NIFI_HOST = os.environ.get('NIFI_HOST', 'nifi_centrosur')
NIFI_PORT = os.environ.get('NIFI_PORT', '8443')
NIFI_USERNAME = os.environ.get('NIFI_USERNAME', 'admin')
NIFI_PASSWORD = os.environ.get('NIFI_PASSWORD', 'centrosur123')

# Variable para seguimiento de problemas
DEBUG_MODE = True

def execute_curl(cmd, log_prefix=""):
    """Ejecuta un comando curl y captura la salida detallada con depuración"""
    try:
        # Convertir la lista cmd a un string para logging
        cmd_str = " ".join(cmd)
        logger.debug(f"{log_prefix} Ejecutando comando: {cmd_str}")
        
        # Ejecutar el comando
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        # Loguear resultado
        logger.debug(f"{log_prefix} Código de salida: {result.returncode}")
        
        if result.stdout:
            if len(result.stdout) > 500:
                logger.debug(f"{log_prefix} Stdout (primeros 500 chars): {result.stdout[:500]}...")
            else:
                logger.debug(f"{log_prefix} Stdout completo: {result.stdout}")
        else:
            logger.debug(f"{log_prefix} Stdout: <vacío>")
            
        if result.stderr:
            logger.debug(f"{log_prefix} Stderr: {result.stderr}")
        
        return result
    except Exception as e:
        logger.error(f"{log_prefix} Excepción ejecutando curl: {str(e)}")
        logger.error(traceback.format_exc())
        return None


def get_token():
    """Obtener token de NiFi mediante curl (método que sabemos funciona)"""
    try:
        # Comando curl que funciona según el proxy original
        cmd = [
            "curl", "-k", "-s", "-v",  # Agregado -v para más detalles
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "POST",
            "-d", f"username={NIFI_USERNAME}&password={NIFI_PASSWORD}",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "https://localhost:8443/nifi-api/access/token"
        ]
        
        logger.info("Obteniendo token de NiFi...")
        result = execute_curl(cmd, "AUTH")
        
        if result and result.returncode == 0 and result.stdout:
            token = result.stdout.strip()
            logger.info(f"Token obtenido: {len(token)} caracteres")
            return token
        
        logger.error(f"Error obteniendo token: {result.returncode if result else 'N/A'} - {result.stderr if result else 'N/A'}")
        return None
    except Exception as e:
        logger.error(f"Excepción obteniendo token: {str(e)}")
        logger.error(traceback.format_exc())
        return None

def test_nifi_api_paths(token):
    """Prueba diferentes rutas de API para determinar la correcta"""
    if not token:
        return {}
    
    test_processor_id = "fead0861-8e64-3fe1-7aaf-031a8eba1e2e"  # Usar uno de los procesadores
    
    paths_to_test = [
        "/processors/{id}",
        "/nifi-api/processors/{id}",
        "/nifi/{id}",
        "/flow/processors/{id}"
    ]
    
    results = {}
    
    for path in paths_to_test:
        try:
            test_path = path.replace("{id}", test_processor_id)
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{test_path}"
            ]
            
            logger.debug(f"Probando ruta: {test_path}")
            result = execute_curl(cmd, f"TEST {test_path}")
            
            if result and result.returncode == 0:
                # Verificar si la respuesta parece válida
                if result.stdout and len(result.stdout.strip()) > 5:  # Al menos algo de contenido
                    results[test_path] = {
                        "status": "success",
                        "content_length": len(result.stdout),
                        "sample": result.stdout[:100] + "..." if len(result.stdout) > 100 else result.stdout
                    }
                else:
                    results[test_path] = {
                        "status": "empty",
                        "content_length": len(result.stdout) if result.stdout else 0
                    }
            else:
                results[test_path] = {
                    "status": "error",
                    "returncode": result.returncode if result else "N/A",
                    "error": result.stderr if result and result.stderr else "Unknown error"
                }
        except Exception as e:
            results[test_path] = {
                "status": "exception",
                "error": str(e)
            }
    
    # Determinar la mejor ruta
    working_paths = [p for p, r in results.items() if r.get("status") == "success"]
    if working_paths:
        results["best_path"] = working_paths[0]
    else:
        results["best_path"] = None
    
    return results

def get_processor_direct(processor_id, token=None):
    """Intenta obtener un procesador usando múltiples métodos para depuración"""
    if not token:
        token = get_token()
        if not token:
            return {"error": "No se pudo obtener token"}
    
    results = {}
    
    # Método 1: Ruta estándar con verbosidad
    cmd1 = [
        "curl", "-k", "-s", "-v",
        "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
        "-H", f"Authorization: Bearer {token}",
        "-H", "Accept: application/json",
        f"https://localhost:8443/processors/{processor_id}"
    ]
    
    result1 = execute_curl(cmd1, "GET_PROCESSOR_1")
    results["method1"] = {
        "returncode": result1.returncode if result1 else "N/A",
        "stdout_length": len(result1.stdout) if result1 and result1.stdout else 0,
        "stderr_length": len(result1.stderr) if result1 and result1.stderr else 0,
        "response_sample": result1.stdout[:200] + "..." if result1 and result1.stdout and len(result1.stdout) > 200 else (result1.stdout if result1 and result1.stdout else "")
    }
    
    # Método 2: Ruta con /nifi-api
    cmd2 = [
        "curl", "-k", "-s", "-v",
        "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
        "-H", f"Authorization: Bearer {token}",
        "-H", "Accept: application/json",
        f"https://localhost:8443/nifi-api/processors/{processor_id}"
    ]
    
    result2 = execute_curl(cmd2, "GET_PROCESSOR_2")
    results["method2"] = {
        "returncode": result2.returncode if result2 else "N/A",
        "stdout_length": len(result2.stdout) if result2 and result2.stdout else 0,
        "stderr_length": len(result2.stderr) if result2 and result2.stderr else 0,
        "response_sample": result2.stdout[:200] + "..." if result2 and result2.stdout and len(result2.stdout) > 200 else (result2.stdout if result2 and result2.stdout else "")
    }
    
    # Método 3: Búsqueda del procesador
    cmd3 = [
        "curl", "-k", "-s", "-v",
        "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
        "-H", f"Authorization: Bearer {token}",
        "-H", "Accept: application/json",
        f"https://localhost:8443/flow/search-results?q={processor_id}"
    ]
    
    result3 = execute_curl(cmd3, "GET_PROCESSOR_3")
    results["method3"] = {
        "returncode": result3.returncode if result3 else "N/A",
        "stdout_length": len(result3.stdout) if result3 and result3.stdout else 0,
        "stderr_length": len(result3.stderr) if result3 and result3.stderr else 0,
        "response_sample": result3.stdout[:200] + "..." if result3 and result3.stdout and len(result3.stdout) > 200 else (result3.stdout if result3 and result3.stdout else "")
    }
    
    # Determinar qué método funcionó mejor
    methods_by_content = sorted(
        ["method1", "method2", "method3"],
        key=lambda m: results[m]["stdout_length"],
        reverse=True
    )
    
    best_method = methods_by_content[0]
    best_result = None
    
    if best_method == "method1" and results["method1"]["stdout_length"] > 10:
        best_result = result1
    elif best_method == "method2" and results["method2"]["stdout_length"] > 10:
        best_result = result2
    elif best_method == "method3" and results["method3"]["stdout_length"] > 10:
        best_result = result3
    
    # Intentar extraer el procesador si se encontró
    if best_result and best_result.stdout:
        try:
            data = json.loads(best_result.stdout)
            
            # Para el caso de búsqueda, intentar extraer el procesador
            if best_method == "method3" and "searchResultsDTO" in data:
                processor_found = False
                if "processorResults" in data["searchResultsDTO"]:
                    for proc in data["searchResultsDTO"]["processorResults"]:
                        if proc.get("id") == processor_id:
                            return {"processor_found": True, "processor_data": proc}
                    
                    if not processor_found:
                        return {"processor_found": False, "search_results": data, "debug_info": results}
                else:
                    return {"processor_found": False, "search_results": data, "debug_info": results}
            else:
                return data
        except json.JSONDecodeError:
            logger.error(f"Error decodificando JSON de la respuesta: {best_result.stdout[:200]}")
            # Intentar extraer información útil de la respuesta de texto
            if "not found" in best_result.stdout.lower() or "does not exist" in best_result.stdout.lower():
                return {"error": "Procesador no encontrado", "debug_info": results}
            else:
                return {"raw_response": best_result.stdout, "debug_info": results}
    
    # No se encontró información útil
    return {"error": "No se pudo obtener información del procesador", "debug_info": results}

@app.route('/api/system/status')
def system_status():
    """Endpoint unificado para diagnóstico del sistema con múltiples niveles de detalle"""
    try:
        # Parámetros para controlar el nivel de detalle
        detail_level = request.args.get('detail', 'basic').lower()  # 'basic', 'complete', 'advanced'
        specific_checks = request.args.get('checks', '').split(',')  # Verificaciones específicas
        
        # Información básica que siempre se incluye
        status_data = {
            "timestamp": datetime.datetime.now().isoformat(),
            "service": "mini-nifi-proxy",
            "environment": {
                "NIFI_HOST": NIFI_HOST,
                "NIFI_PORT": NIFI_PORT,
                "DEBUG_MODE": DEBUG_MODE
            }
        }
        
        # Verificación de conectividad básica (ping)
        if detail_level in ['basic', 'complete', 'advanced'] or 'connectivity' in specific_checks:
            ping_cmd = [
                "curl", "-k", "-s", "--head", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "https://localhost:8443/"
            ]
            ping_result = execute_curl(ping_cmd, "PING")
            
            status_data["connectivity"] = {
                "success": ping_result is not None and ping_result.returncode == 0,
                "status_code": ping_result.returncode if ping_result else None,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Agregar resumen general basado en conectividad
            status_data["status"] = "healthy" if status_data["connectivity"]["success"] else "unhealthy"
        
        # Verificación de autenticación (health)
        if detail_level in ['basic', 'complete', 'advanced'] or 'auth' in specific_checks:
            token = get_token()
            
            status_data["auth"] = {
                "working": token is not None,
                "token_obtained": token is not None,
                "token_length": len(token) if token else 0,
                "timestamp": datetime.datetime.now().isoformat()
            }
            
            # Actualizar el estado general si la autenticación falla
            if not token and "status" in status_data and status_data["status"] == "healthy":
                status_data["status"] = "degraded"
        
        # Verificaciones de API para niveles complete y advanced
        if (detail_level in ['complete', 'advanced'] or 'api' in specific_checks) and 'auth' in status_data and status_data["auth"]["working"]:
            token = get_token()  # Reutilizar token si ya existe
            
            # Probar rutas de API
            api_tests = test_nifi_api_paths(token)
            status_data["api_paths"] = {
                "test_results": api_tests,
                "best_path": api_tests.get("best_path"),
                "timestamp": datetime.datetime.now().isoformat()
            }
        
        # Verificaciones avanzadas solo para nivel advanced
        if detail_level == 'advanced' or any(check in specific_checks for check in ['processor', 'search']):
            token = get_token()  # Reutilizar token si ya existe
            
            if token:
                # Verificación de un procesador específico
                if 'processor' in specific_checks or detail_level == 'advanced':
                    processor_id = request.args.get('processor_id', 'fead0861-8e64-3fe1-7aaf-031a8eba1e2e')
                    processor_test = get_processor_direct(processor_id, token)
                    
                    status_data["processor_test"] = {
                        "processor_id": processor_id,
                        "success": "error" not in processor_test,
                        "response_type": "json" if isinstance(processor_test, dict) and "error" not in processor_test else "error",
                        "timestamp": datetime.datetime.now().isoformat()
                    }
                
                # Verificación de búsqueda
                if 'search' in specific_checks or detail_level == 'advanced':
                    search_term = request.args.get('search', 'Database')
                    try:
                        search_cmd = [
                            "curl", "-k", "-s",
                            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                            "-H", f"Authorization: Bearer {token}",
                            "-H", "Accept: application/json",
                            f"https://localhost:8443/flow/search-results?q={search_term}"
                        ]
                        
                        search_result = execute_curl(search_cmd, "DIAG_SEARCH")
                        
                        if search_result and search_result.returncode == 0 and search_result.stdout:
                            try:
                                search_data = json.loads(search_result.stdout)
                                status_data["search_test"] = {
                                    "success": True,
                                    "term": search_term,
                                    "has_results": "searchResultsDTO" in search_data,
                                    "timestamp": datetime.datetime.now().isoformat()
                                }
                            except json.JSONDecodeError:
                                status_data["search_test"] = {
                                    "success": False,
                                    "error": "JSON inválido en respuesta",
                                    "timestamp": datetime.datetime.now().isoformat()
                                }
                        else:
                            status_data["search_test"] = {
                                "success": False,
                                "error": "Error en curl o respuesta vacía",
                                "timestamp": datetime.datetime.now().isoformat()
                            }
                    except Exception as e:
                        status_data["search_test"] = {
                            "success": False,
                            "error": str(e),
                            "timestamp": datetime.datetime.now().isoformat()
                        }
        
        # Incluir recomendaciones de acción basadas en los resultados
        if 'recommendations' in specific_checks or detail_level in ['complete', 'advanced']:
            recommendations = []
            
            if "connectivity" in status_data and not status_data["connectivity"]["success"]:
                recommendations.append("Verificar conectividad de red con NiFi")
                recommendations.append(f"Confirmar que el host NiFi ({NIFI_HOST}) es accesible")
            
            if "auth" in status_data and not status_data["auth"]["working"]:
                recommendations.append("Verificar credenciales de autenticación")
                recommendations.append("Confirmar que el usuario tiene permisos válidos")
            
            if "api_paths" in status_data and not status_data["api_paths"]["best_path"]:
                recommendations.append("Verificar estructura de API de NiFi")
                recommendations.append("La versión de NiFi puede ser incompatible con el proxy")
            
            status_data["recommendations"] = recommendations
        
        # Agregar un resumen ejecutivo para facilitar la interpretación
        status_data["summary"] = {
            "overall": status_data.get("status", "unknown"),
            "components_checked": len([k for k in status_data.keys() if k not in ["timestamp", "environment", "status", "summary", "recommendations"]]),
            "components_healthy": len([k for k in status_data.keys() if k in ["connectivity", "auth", "api_paths", "processor_test", "search_test"] 
                                       and isinstance(status_data[k], dict) and status_data[k].get("success", False)]),
            "detail_level": detail_level
        }
        
        return jsonify(status_data)
        
    except Exception as e:
        logger.error(f"Error en system_status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "status": "error", 
            "error": str(e),
            "timestamp": datetime.datetime.now().isoformat()
        }), 500

@app.route('/health')
def health():
    """Endpoint legado de verificación de salud (redirige a /api/system/status)"""
    return system_status()

@app.route('/diagnostics')
def diagnostics():
    """Endpoint legado de diagnóstico completo (redirige a /api/system/status con más detalle)"""
    # Modificar args para solicitar detalle completo
    request.args = dict(request.args)
    request.args['detail'] = 'complete'
    return system_status()

@app.route('/ping-nifi')
def ping_nifi():
    """Endpoint legado para verificar conectividad (redirige a /api/system/status con verificación específica)"""
    # Modificar args para solicitar solo la verificación de conectividad
    request.args = dict(request.args)
    request.args['checks'] = 'connectivity'
    return system_status()
    """Verifica la conectividad básica con NiFi (sin autenticación)"""
    try:
        # Intentar una conexión básica a NiFi
        cmd = [
            "curl", "-k", "-s", "--head", "-v",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "https://localhost:8443/"
        ]
        
        result = execute_curl(cmd, "PING")
        
        return jsonify({
            "success": result is not None and result.returncode == 0,
            "status_code": result.returncode if result else None,
            "response": result.stderr if result and result.stderr else "No response",
            "nifi_host": NIFI_HOST,
            "nifi_port": NIFI_PORT
        })
    except Exception as e:
        logger.error(f"Error en ping-nifi: {str(e)}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/api/access/token', methods=['POST'])
def get_access_token():
    """Endpoint para obtener token de autenticación"""
    try:
        username = request.form.get('username', NIFI_USERNAME)
        password = request.form.get('password', NIFI_PASSWORD)
        
        # Comando curl para obtener token
        cmd = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "POST",
            "-d", f"username={username}&password={password}",
            "-H", "Content-Type: application/x-www-form-urlencoded",
            "https://localhost:8443/nifi-api/access/token"
        ]
        
        result = execute_curl(cmd, "TOKEN")
        
        if result and result.returncode == 0 and result.stdout:
            token = result.stdout.strip()
            return token, 201
        
        logger.error(f"Error obteniendo token: {result.returncode if result else 'N/A'} - {result.stderr if result else 'N/A'}")
        return jsonify({"error": "No se pudo obtener el token"}), 401
        
    except Exception as e:
        logger.error(f"Excepción obteniendo token: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

@app.route('/api/processors/<processor_id>', methods=['GET'])
def get_processor(processor_id):
    """Endpoint para acceder a procesadores con manejo de errores mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Consultando procesador: {processor_id}")
        
        # Usar el método directo con múltiples intentos
        processor_data = get_processor_direct(processor_id, token)
        
        # Verificar si obtuvimos datos válidos
        if processor_data and isinstance(processor_data, dict):
            if "error" in processor_data:
                logger.warning(f"Error obteniendo procesador: {processor_data.get('error')}")
                return jsonify(processor_data), 404
            elif "raw_response" in processor_data:
                logger.info(f"Procesador obtenido con respuesta no-JSON: {len(processor_data.get('raw_response', ''))} caracteres")
                return jsonify(processor_data), 200
            else:
                logger.info(f"Procesador obtenido correctamente: {processor_id}")
                return processor_data, 200
        else:
            logger.error(f"Respuesta inesperada al obtener procesador: {type(processor_data)}")
            return jsonify({"error": "Respuesta inesperada", "data": str(processor_data)}), 500
            
    except Exception as e:
        logger.error(f"Error en get_processor: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/processors/<processor_id>', methods=['PUT'])
def update_processor(processor_id):
    """Endpoint para actualizar procesadores con diagnóstico mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        # Verificar contenido no vacío
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length == 0:
            return jsonify({"error": "Cuerpo de solicitud vacío"}), 400
            
        request_body = request.get_data(as_text=True)
        
        # Validar estructura JSON
        try:
            update_data = json.loads(request_body)
            if 'component' not in update_data or 'revision' not in update_data:
                logger.warning("Estructura de actualización incorrecta")
        except:
            logger.warning("Cuerpo no es JSON válido")
        
        # Loguear el cuerpo para depuración
        if len(request_body) > 500:
            logger.debug(f"Cuerpo de solicitud (primeros 500 chars): {request_body[:500]}...")
        else:
            logger.debug(f"Cuerpo de solicitud completo: {request_body}")
        
        # Guardar con timestamp único
        temp_file = f"/tmp/processor_update_{processor_id}_{int(time.time())}.json"
        with open(temp_file, "w") as f:
            f.write(request_body)
        
        logger.info(f"Actualizando procesador: {processor_id}")
        
        # Comando curl principal
        cmd = [
            "curl", "-k", "-s", "-v",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/json",
            "-H", "Accept: application/json",
            "-d", "@" + temp_file,
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result = execute_curl(cmd, "UPDATE")
        
        # Limpiar archivo temporal
        try:
            os.remove(temp_file)
        except:
            logger.warning(f"No se pudo eliminar archivo temporal: {temp_file}")
        
        # Verificar resultado y hacer verificación adicional
        if result and result.returncode == 0 and result.stdout:
            try:
                update_result = json.loads(result.stdout)
                
                # Verificación adicional
                verify_cmd = [
                    "curl", "-k", "-s",
                    "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                    "-H", f"Authorization: Bearer {token}",
                    "-H", "Accept: application/json",
                    f"https://localhost:8443/nifi-api/processors/{processor_id}"
                ]
                verify_result = execute_curl(verify_cmd, "VERIFY_UPDATE")
                
                # Añadir metadata de verificación
                update_result['_verification'] = {
                    "verification_performed": True,
                    "verification_time": time.time()
                }
                
                logger.info(f"Procesador actualizado correctamente: {processor_id}")
                return update_result
            except json.JSONDecodeError:
                logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
                return jsonify({"raw_response": result.stdout}), 200
        
        # Si falla el primer intento, probar ruta alternativa
        if not result or result.returncode != 0 or not result.stdout:
            logger.warning("Respuesta vacía o error, intentando ruta alternativa")
            
            cmd_alt = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "PUT",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Content-Type: application/json",
                "-H", "Accept: application/json",
                "-d", request_body,
                f"https://localhost:8443/processors/{processor_id}"
            ]
            
            result_alt = execute_curl(cmd_alt, "UPDATE_ALT")
            # Procesar resultado alternativo...
            
        # Resto del código de manejo de errores...
            
    except Exception as e:
        logger.error(f"Error en update_processor: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/processors/<processor_id>/run-status', methods=['PUT'])
def update_processor_status(processor_id):
    """Endpoint para actualizar el estado de un procesador con diagnóstico mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        # Leer datos del cuerpo de la solicitud
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length == 0:
            return jsonify({"error": "Cuerpo de solicitud vacío"}), 400
            
        request_body = request.get_data(as_text=True)
        
        # Verificar si se está tratando de iniciar o detener el procesador
        try:
            body_json = json.loads(request_body)
            target_state = body_json.get('state', '').upper()
            logger.info(f"Cambiando estado del procesador {processor_id} a {target_state}")
        except:
            target_state = "UNKNOWN"
            logger.warning(f"No se pudo determinar el estado objetivo para {processor_id}")
        
        # Guardar temporalmente el cuerpo
        temp_file = f"/tmp/processor_status_{processor_id}.json"
        with open(temp_file, "w") as f:
            f.write(request_body)
        
        # Probar rutas alternativas para cambiar el estado
        api_paths = [
            f"/nifi-api/processors/{processor_id}/run-status",
            f"/processors/{processor_id}/run-status",
            # Alternativa utilizando la actualización directa del procesador
            f"/nifi-api/processors/{processor_id}"
        ]
        
        success = False
        response_data = None
        
        for api_path in api_paths:
            logger.info(f"Intentando cambiar estado usando ruta: {api_path}")
            
            # Para la tercera alternativa, necesitamos modificar el cuerpo
            if api_path.endswith(processor_id):
                try:
                    # Obtener el procesador primero
                    processor_data = get_processor_direct(processor_id, token)
                    
                    if "error" in processor_data:
                        logger.warning(f"No se pudo obtener datos del procesador para actualización alternativa: {processor_data.get('error')}")
                        continue
                    
                    # Preparar cuerpo alternativo
                    alt_body = {
                        "revision": processor_data.get("revision", {}),
                        "component": {
                            "id": processor_id,
                            "state": target_state
                        }
                    }
                    
                    alt_body_str = json.dumps(alt_body)
                    logger.debug(f"Cuerpo alternativo: {alt_body_str}")
                    
                    # Guardar en archivo temporal
                    with open(temp_file, "w") as f:
                        f.write(alt_body_str)
                        
                except Exception as e:
                    logger.error(f"Error preparando cuerpo alternativo: {str(e)}")
                    continue
            
            # Comando curl para actualizar el estado del procesador
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "PUT",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Content-Type: application/json",
                "-H", "Accept: application/json",
                "-d", "@" + temp_file,
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"RUN_STATUS_{api_paths.index(api_path)}")
            
            # Verificar resultado
            if result and result.returncode == 0:
                if result.stdout and len(result.stdout.strip()) > 0:
                    try:
                        response_data = json.loads(result.stdout)
                        success = True
                        logger.info(f"Estado del procesador actualizado correctamente usando {api_path}")
                        break
                    except json.JSONDecodeError:
                        logger.warning(f"Respuesta no es JSON válido para {api_path}: {result.stdout[:100]}...")
                        if not response_data:
                            response_data = {"raw_response": result.stdout}
                else:
                    logger.warning(f"Respuesta vacía para {api_path}")
            else:
                logger.warning(f"Error en curl para {api_path}: {result.returncode if result else 'N/A'}")
        
        # Eliminar archivo temporal
        try:
            os.remove(temp_file)
        except:
            pass
        
        # Devolver respuesta basada en resultados
        if success:
            return response_data, 200
        elif response_data:
            return jsonify(response_data), 200
        else:
            # Si todos los intentos fallaron, intentar un enfoque directo
            simplified_body = {
                "revision": {
                    "version": 0
                },
                "state": target_state
            }
            
            logger.info("Intentando enfoque simplificado final")
            cmd_final = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "PUT",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Content-Type: application/json",
                "-H", "Accept: application/json",
                "-d", json.dumps(simplified_body),
                f"https://localhost:8443/processors/{processor_id}/run-status"
            ]
            
            result_final = execute_curl(cmd_final, "FINAL_ATTEMPT")
            
            if result_final and result_final.returncode == 0 and result_final.stdout:
                try:
                    final_data = json.loads(result_final.stdout)
                    return final_data, 200
                except json.JSONDecodeError:
                    return jsonify({"raw_response": result_final.stdout}), 200
            else:
                return jsonify({
                    "error": "No se pudo actualizar el estado del procesador",
                    "processor_id": processor_id,
                    "target_state": target_state
                }), 500
            
    except Exception as e:
        logger.error(f"Error en update_processor_status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/flow/search-results')
def search_processors():
    """Endpoint para buscar procesadores con diagnóstico mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
            
        query = request.args.get('q', '')
        logger.info(f"Buscando procesadores con query: {query}")
        
        # Probar diferentes rutas para búsqueda
        api_paths = [
            "/flow/search-results",
            "/nifi-api/flow/search-results"
        ]
        
        for api_path in api_paths:
            # Comando curl para buscar procesadores
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}?q={query}"
            ]
            
            result = execute_curl(cmd, f"SEARCH_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                # Parece que obtuvimos una respuesta válida
                try:
                    search_results = json.loads(result.stdout)
                    logger.info(f"Búsqueda completada usando {api_path}. Resultados encontrados.")
                    
                    # Verificar si tiene la estructura esperada
                    if "searchResultsDTO" in search_results:
                        return search_results
                    else:
                        logger.warning(f"Respuesta de búsqueda con estructura inesperada: {json.dumps(search_results)[:200]}...")
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta de búsqueda no es JSON válido: {result.stdout[:100]}...")
        
        # Si llegamos aquí, ninguna ruta funcionó bien
        logger.error("Ninguna ruta de búsqueda devolvió resultados válidos")
        return jsonify({"error": "No se pudieron obtener resultados de búsqueda válidos"}), 500
            
    except Exception as e:
        logger.error(f"Error en search_processors: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/process-groups/<group_id>/processors', methods=['POST'])
def create_processor(group_id):
    """Endpoint para crear un nuevo procesador con diagnóstico mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        # Leer datos del cuerpo de la solicitud
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length == 0:
            return jsonify({"error": "Cuerpo de solicitud vacío"}), 400
            
        request_body = request.get_data(as_text=True)
        
        # Loguear el cuerpo para depuración
        logger.debug(f"Cuerpo para crear procesador: {request_body[:500]}..." if len(request_body) > 500 else request_body)
        
        # Guardar temporalmente el cuerpo
        temp_file = f"/tmp/create_processor_{group_id}.json"
        with open(temp_file, "w") as f:
            f.write(request_body)
        
        logger.info(f"Creando procesador en grupo: {group_id}")
        
        # Probar diferentes rutas para creación
        api_paths = [
            f"/process-groups/{group_id}/processors",
            f"/nifi-api/process-groups/{group_id}/processors"
        ]
        
        for api_path in api_paths:
            # Comando curl para crear un procesador
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "POST",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Content-Type: application/json",
                "-H", "Accept: application/json",
                "-d", "@" + temp_file,
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"CREATE_{api_paths.index(api_path)}")
            
            # Eliminar archivo temporal después del primer intento
            if api_paths.index(api_path) == 0:
                try:
                    os.remove(temp_file)
                except:
                    pass
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                # Parece que obtuvimos una respuesta válida
                try:
                    create_result = json.loads(result.stdout)
                    logger.info(f"Procesador creado correctamente en grupo {group_id} usando {api_path}")
                    
                    # Verificar que tiene un ID (indicación de creación exitosa)
                    if "id" in create_result:
                        return create_result, 201
                    else:
                        logger.warning(f"Respuesta de creación con estructura inesperada: {json.dumps(create_result)[:200]}...")
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta de creación no es JSON válido: {result.stdout[:100]}...")
        
        # Si llegamos aquí, ninguna ruta funcionó bien
        logger.error("Ninguna ruta de creación funcionó correctamente")
        return jsonify({"error": "No se pudo crear el procesador"}), 500
            
    except Exception as e:
        logger.error(f"Error en create_processor: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500



@app.route('/api/processors/<processor_id>', methods=['DELETE'])
def delete_processor(processor_id):
    """Endpoint para eliminar un procesador con diagnóstico mejorado"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        version = request.args.get('version', '-1')
        logger.info(f"Eliminando procesador: {processor_id} (versión: {version})")
        
        # Probar diferentes rutas para eliminación
        api_paths = [
            f"/processors/{processor_id}?version={version}",
            f"/nifi-api/processors/{processor_id}?version={version}"
        ]
        
        for api_path in api_paths:
            # Comando curl para eliminar un procesador
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "DELETE",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"DELETE_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0:
                # La eliminación podría devolver una respuesta vacía o un json
                if not result.stdout or len(result.stdout.strip()) == 0:
                    logger.info(f"Procesador {processor_id} eliminado correctamente (respuesta vacía)")
                    return jsonify({"success": True}), 200
                else:
                    try:
                        delete_result = json.loads(result.stdout)
                        logger.info(f"Procesador {processor_id} eliminado correctamente")
                        return delete_result, 200
                    except json.JSONDecodeError:
                        logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
                        return jsonify({"raw_response": result.stdout}), 200
        
        # Si llegamos aquí, ninguna ruta funcionó bien
        logger.error("Ninguna ruta de eliminación funcionó correctamente")
        return jsonify({"error": "No se pudo eliminar el procesador"}), 500
            
    except Exception as e:
        logger.error(f"Error en delete_processor: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/process-groups/<group_id>/controller-services')
def get_controller_services(group_id):
    """Endpoint para obtener servicios de controlador en un grupo"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Obteniendo servicios de controlador para grupo: {group_id}")
        
        # Probar diferentes rutas
        api_paths = [
            f"/process-groups/{group_id}/controller-services",
            f"/nifi-api/process-groups/{group_id}/controller-services"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"CONTROLLER_SERVICES_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                try:
                    services = json.loads(result.stdout)
                    logger.info(f"Servicios de controlador obtenidos para grupo {group_id}")
                    return services
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
        
        # Si ninguna ruta funcionó
        logger.error("No se pudieron obtener servicios de controlador")
        return jsonify({"error": "No se pudieron obtener servicios de controlador"}), 500
        
    except Exception as e:
        logger.error(f"Error en get_controller_services: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/controller-services/<service_id>')
def get_controller_service(service_id):
    """Endpoint para obtener un servicio de controlador específico"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Obteniendo servicio de controlador: {service_id}")
        
        # Probar diferentes rutas
        api_paths = [
            f"/controller-services/{service_id}",
            f"/nifi-api/controller-services/{service_id}"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"CONTROLLER_SERVICE_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                try:
                    service = json.loads(result.stdout)
                    logger.info(f"Servicio de controlador obtenido: {service_id}")
                    return service
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
        
        # Si ninguna ruta funcionó
        logger.error("No se pudo obtener el servicio de controlador")
        return jsonify({"error": "No se pudo obtener el servicio de controlador"}), 500
        
    except Exception as e:
        logger.error(f"Error en get_controller_service: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/controller-services/<service_id>/run-status', methods=['PUT'])
def update_controller_service_status(service_id):
    """Endpoint para actualizar el estado de un servicio de controlador"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        # Leer datos del cuerpo de la solicitud
        content_length = int(request.headers.get('Content-Length', 0))
        if content_length == 0:
            return jsonify({"error": "Cuerpo de solicitud vacío"}), 400
            
        request_body = request.get_data(as_text=True)
        
        # Guardar temporalmente el cuerpo
        temp_file = f"/tmp/service_status_{service_id}.json"
        with open(temp_file, "w") as f:
            f.write(request_body)
        
        logger.info(f"Actualizando estado del servicio de controlador: {service_id}")
        
        # Probar diferentes rutas
        api_paths = [
            f"/controller-services/{service_id}/run-status",
            f"/nifi-api/controller-services/{service_id}/run-status"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "PUT",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Content-Type: application/json",
                "-H", "Accept: application/json",
                "-d", "@" + temp_file,
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"CONTROLLER_SERVICE_STATUS_{api_paths.index(api_path)}")
            
            # Eliminar archivo temporal después del primer intento
            if api_paths.index(api_path) == 0:
                try:
                    os.remove(temp_file)
                except:
                    pass
            
            if result and result.returncode == 0:
                if result.stdout and len(result.stdout.strip()) > 0:
                    try:
                        status_result = json.loads(result.stdout)
                        logger.info(f"Estado del servicio de controlador actualizado: {service_id}")
                        return status_result
                    except json.JSONDecodeError:
                        logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
                        return jsonify({"raw_response": result.stdout}), 200
                else:
                    # Algunas APIs pueden devolver respuesta vacía en caso de éxito
                    logger.info(f"Estado del servicio actualizado (respuesta vacía): {service_id}")
                    return jsonify({"success": True}), 200
        
        # Si ninguna ruta funcionó
        logger.error("No se pudo actualizar el estado del servicio de controlador")
        return jsonify({"error": "No se pudo actualizar el estado del servicio de controlador"}), 500
        
    except Exception as e:
        logger.error(f"Error en update_controller_service_status: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500



  
 

@app.route('/api/connections/<connection_id>/drop-requests', methods=['POST'])
def create_drop_request(connection_id):
    """Endpoint para iniciar la limpieza de colas en una conexión"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Iniciando solicitud de limpieza para conexión: {connection_id}")
        
        # Probar diferentes rutas para crear solicitud de limpieza
        api_paths = [
            f"/connections/{connection_id}/drop-requests",
            f"/nifi-api/connections/{connection_id}/drop-requests"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "POST",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"DROP_REQUEST_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                try:
                    drop_request = json.loads(result.stdout)
                    logger.info(f"Solicitud de limpieza creada para conexión {connection_id}")
                    return drop_request, 201
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
        
        # Si ninguna ruta funcionó
        logger.error(f"No se pudo crear solicitud de limpieza para conexión {connection_id}")
        return jsonify({"error": "No se pudo crear solicitud de limpieza"}), 500
        
    except Exception as e:
        logger.error(f"Error en create_drop_request: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/connections/<connection_id>/drop-requests/<request_id>', methods=['GET'])
def get_drop_request(connection_id, request_id):
    """Endpoint para verificar el estado de una solicitud de limpieza"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Verificando estado de solicitud de limpieza {request_id} para conexión {connection_id}")
        
        # Probar diferentes rutas para obtener estado
        api_paths = [
            f"/connections/{connection_id}/drop-requests/{request_id}",
            f"/nifi-api/connections/{connection_id}/drop-requests/{request_id}"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"GET_DROP_REQUEST_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0 and result.stdout and len(result.stdout.strip()) > 10:
                try:
                    status_data = json.loads(result.stdout)
                    logger.info(f"Estado de solicitud obtenido para conexión {connection_id}")
                    
                    # Registrar progreso
                    if "dropRequest" in status_data:
                        drop_request = status_data["dropRequest"]
                        current = drop_request.get("currentCount", 0)
                        original = drop_request.get("originalCount", 0)
                        finished = drop_request.get("finished", False)
                        
                        if finished:
                            logger.info(f"Limpieza completada: {current}/{original} flowfiles procesados")
                        else:
                            logger.info(f"Limpieza en progreso: {current}/{original} flowfiles procesados")
                    
                    return status_data
                except json.JSONDecodeError:
                    logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
        
        # Si ninguna ruta funcionó
        logger.error(f"No se pudo obtener estado de solicitud {request_id}")
        return jsonify({"error": "No se pudo obtener estado de solicitud"}), 500
        
    except Exception as e:
        logger.error(f"Error en get_drop_request: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

@app.route('/api/connections/<connection_id>/drop-requests/<request_id>', methods=['DELETE'])
def delete_drop_request(connection_id, request_id):
    """Endpoint para eliminar una solicitud de limpieza"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        
        logger.info(f"Eliminando solicitud de limpieza {request_id} para conexión {connection_id}")
        
        # Probar diferentes rutas para eliminar
        api_paths = [
            f"/connections/{connection_id}/drop-requests/{request_id}",
            f"/nifi-api/connections/{connection_id}/drop-requests/{request_id}"
        ]
        
        for api_path in api_paths:
            cmd = [
                "curl", "-k", "-s", "-v",
                "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                "-X", "DELETE",
                "-H", f"Authorization: Bearer {token}",
                "-H", "Accept: application/json",
                f"https://localhost:8443{api_path}"
            ]
            
            result = execute_curl(cmd, f"DELETE_DROP_REQUEST_{api_paths.index(api_path)}")
            
            if result and result.returncode == 0:
                if not result.stdout or len(result.stdout.strip()) == 0:
                    logger.info(f"Solicitud de limpieza {request_id} eliminada correctamente (respuesta vacía)")
                    return jsonify({"success": True}), 200
                else:
                    try:
                        response_data = json.loads(result.stdout)
                        logger.info(f"Solicitud de limpieza {request_id} eliminada correctamente")
                        return response_data, 200
                    except json.JSONDecodeError:
                        logger.warning(f"Respuesta no es JSON válido: {result.stdout[:100]}...")
                        return jsonify({"raw_response": result.stdout}), 200
        
        # Si ninguna ruta funcionó
        logger.error(f"No se pudo eliminar solicitud {request_id}")
        return jsonify({"error": "No se pudo eliminar solicitud"}), 500
        
    except Exception as e:
        logger.error(f"Error en delete_drop_request: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

# Endpoint adicional para facilitar la limpieza de todas las colas en una lista
@app.route('/api/clean-all-queues', methods=['POST'])
def clean_all_queues():
    """Endpoint para limpiar múltiples colas en una sola operación"""
    try:
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
            
        # Obtener la lista de conexiones del cuerpo o usar una lista predefinida
        request_data = request.get_json() or {}
        connections = request_data.get('connection_ids', [])
        
        # Si no se proporciona lista, usar las conexiones conocidas
        if not connections:
            connections = [
                # Conexiones del grupo productor
                {"id": "4dcc0bed-9061-3a4c-e930-c375f77615eb", "name": "QueryDB-to-LogAttribute", "group": "Productor"},
                {"id": "723a3709-dabb-3fae-b467-2892da95b9a6", "name": "LogAttribute-to-ConvertRecord", "group": "Productor"},
                {"id": "42f8934e-7af2-39e2-e0ff-8db121f90208", "name": "ConvertRecord-to-LogAttribute", "group": "Productor"},
                {"id": "da3c24c2-2c5b-3fd1-28dd-28d8ecf5b827", "name": "LogAttribute-to-PublishKafka", "group": "Productor"},
                {"id": "1f03f57d-f9b9-38ef-9f1b-d8511b663819", "name": "PublishKafka-to-LogAttribute", "group": "Productor"},
                {"id": "e9cb565f-d631-392c-cd11-e65aa719180f", "name": "LogAttribute-to-success", "group": "Productor"},
                
                # Conexiones del grupo consumidor
                {"id": "3724acc6-adcb-323c-46b2-34915d2c68af", "name": "ConsumeKafka-to-LogAttribute", "group": "Consumidor"},
                {"id": "285c7970-148d-3ef0-7e15-8e4bd0c7e92f", "name": "LogAttribute-to-SplitJson", "group": "Consumidor"},
                {"id": "fdec765f-29a0-3359-7287-99df2f0e7922", "name": "SplitJson-to-LogAttribute", "group": "Consumidor"},
                {"id": "55b03c9a-783e-3234-a008-3fc6e723ec52", "name": "LogAttribute-to-EvaluateJsonPath", "group": "Consumidor"},
                {"id": "15965cd6-7f38-342a-9e16-001a8abff5ed", "name": "EvaluateJsonPath-to-ReplaceText", "group": "Consumidor"},
                {"id": "1999fec5-2a9e-333b-167e-de1bf204e4d7", "name": "ReplaceText-to-LogAttribute", "group": "Consumidor"},
                {"id": "d2f78d49-b967-3e3a-6a3d-2387ef8131a1", "name": "LogAttribute-to-PutDatabaseRecord", "group": "Consumidor"},
                {"id": "f403830e-56b4-3a7f-6148-fb30015edce9", "name": "PutDatabaseRecord-to-success", "group": "Consumidor"},
            ]
        
        results = {}
        
        # Limpiar cada conexión
        for connection in connections:
            connection_id = connection.get("id")
            connection_name = connection.get("name", connection_id)
            
            if not connection_id:
                continue
                
            logger.info(f"Limpiando conexión: {connection_name} (ID: {connection_id})")
            
            try:
                # Iniciar solicitud de limpieza
                api_path = f"/nifi-api/connections/{connection_id}/drop-requests"
                cmd = [
                    "curl", "-k", "-s",
                    "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                    "-X", "POST",
                    "-H", f"Authorization: Bearer {token}",
                    "-H", "Accept: application/json",
                    f"https://localhost:8443{api_path}"
                ]
                
                result = execute_curl(cmd, f"DROP_CONNECTION_{connection_id[:8]}")
                
                if result and result.returncode == 0 and result.stdout:
                    drop_request = json.loads(result.stdout)
                    drop_request_id = drop_request.get('dropRequest', {}).get('id')
                    
                    if drop_request_id:
                        results[connection_id] = {
                            "name": connection_name,
                            "status": "initiated",
                            "request_id": drop_request_id
                        }
                    else:
                        results[connection_id] = {
                            "name": connection_name,
                            "status": "error",
                            "error": "No se obtuvo ID de solicitud"
                        }
                else:
                    results[connection_id] = {
                        "name": connection_name,
                        "status": "error",
                        "error": "Error al iniciar limpieza"
                    }
            except Exception as e:
                results[connection_id] = {
                    "name": connection_name,
                    "status": "exception",
                    "error": str(e)
                }
        
        return jsonify({
            "success": True,
            "message": f"Limpieza iniciada para {len(results)} conexiones",
            "results": results
        })
    
    except Exception as e:
        logger.error(f"Error en clean_all_queues: {str(e)}")
        logger.error(traceback.format_exc())
        return jsonify({
            "error": f"Error completo: {str(e)}"
        }), 500

# Decoradores reutilizables para simplificar el código
def require_token(f):
    """Decorador para verificar token antes de ejecutar función"""
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        token = get_token()
        if not token:
            return jsonify({"error": "No se pudo obtener token"}), 401
        kwargs['token'] = token
        return f(*args, **kwargs)
    return decorated_function

def handle_errors(f):
    """Decorador para manejo consistente de errores"""
    @functools.wraps(f)
    def decorated_function(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as e:
            func_name = f.__name__
            logger.error(f"Error en {func_name}: {str(e)}")
            logger.error(traceback.format_exc())
            return jsonify({"error": f"Error en {func_name}: {str(e)}"}), 500
    return decorated_function

@app.route('/api/processors/<processor_id>/sql', methods=['GET', 'POST', 'PUT', 'DELETE'])
@handle_errors
@require_token
def processor_sql_endpoint(processor_id, token):
    """Endpoint unificado para operaciones relacionadas con SQL en procesadores"""
    method = request.method
    
    # Variables comunes que podrían necesitarse en varias operaciones
    diagnostics = request.args.get('diagnostics', 'false').lower() == 'true'
    clean_state = request.args.get('clean_state', 'false').lower() == 'true'
    
    logger.info(f"Operación SQL [{method}] en procesador {processor_id} (diagnóstico: {diagnostics}, limpiar estado: {clean_state})")
    
    # GET - Obtener consulta SQL actual (opcionalmente con diagnóstico)
    if method == 'GET':
        # Obtener la consulta SQL del procesador
        cmd = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result = execute_curl(cmd, "GET_SQL")
        
        if not result or result.returncode != 0 or not result.stdout:
            return jsonify({"error": "No se pudo obtener información del procesador"}), 500
            
        try:
            data = json.loads(result.stdout)
            properties = data.get('component', {}).get('properties', {})
            query = properties.get('Custom Query', '')
            
            # Preparar respuesta básica
            response = {
                "processor_id": processor_id,
                "query_found": bool(query),
                "query": query,
                "query_length": len(query) if query else 0,
                "properties": {k: v for k, v in properties.items() if 'query' in k.lower() or 'sql' in k.lower()}
            }
            
            # Si se solicitan diagnósticos, agregar información adicional
            if diagnostics:
                response["diagnostics"] = {
                    "processor_type": data.get('component', {}).get('type', 'Unknown'),
                    "processor_name": data.get('component', {}).get('name', 'Unknown'),
                    "state": data.get('component', {}).get('state', 'Unknown'),
                    "properties_count": len(properties),
                    "revision": data.get('revision', {}),
                    "has_sql": 'Custom Query' in properties or any('sql' in k.lower() for k in properties.keys()),
                    "timestamp": datetime.datetime.now().isoformat()
                }
            
            return jsonify(response)
            
        except json.JSONDecodeError:
            return jsonify({"error": "La respuesta no es JSON válido"}), 500
            
    # POST - Actualizar consulta SQL (comportamiento similar a update_processor_sql)
    elif method == 'POST':
        # Datos para actualización pueden venir en JSON o como parámetros
        data = request.get_json() or {}
        
        # Si no hay datos en JSON, intentar obtener de form o query params
        if not data:
            query = request.form.get('query') or request.args.get('query')
            if query:
                data = {"properties": {"Custom Query": query}}
        
        properties = data.get('properties', {})
        
        if not properties:
            return jsonify({"error": "No se especificaron propiedades para actualizar"}), 400
            
        # 1. Obtener estado actual del procesador
        cmd_get = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_get = execute_curl(cmd_get, "GET_FOR_SQL_UPDATE")
        
        if not result_get or result_get.returncode != 0 or not result_get.stdout:
            return jsonify({"error": "No se pudo obtener estado del procesador"}), 500
        
        # 2. Extraer datos necesarios
        processor_data = json.loads(result_get.stdout)
        revision = processor_data.get('revision', {})
        component = processor_data.get('component', {})
        current_properties = dict(component.get('properties', {}))  # Crear copia
        
        # 3. Actualizar propiedades
        for key, value in properties.items():
            current_properties[key] = value
        
        # Eliminar propiedades problemáticas
        problematic_props = ["Maximum-value Columns", "start_date", "end_date"]
        for prop in problematic_props:
            if prop in current_properties:
                del current_properties[prop]
        
        # 4. Crear datos para actualización
        update_data = {
            'revision': revision,
            'component': {
                'id': processor_id,
                'properties': current_properties
            }
        }
        
        # Guardar en archivo para curl
        temp_file = f"/tmp/sql_update_{processor_id}_{int(time.time())}.json"
        with open(temp_file, "w") as f:
            f.write(json.dumps(update_data))
        
        # 5. Enviar actualización con curl
        cmd_update = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/json",
            "-H", "Accept: application/json",
            "-d", "@" + temp_file,
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_update = execute_curl(cmd_update, "UPDATE_SQL")
        
        # Eliminar archivo temporal
        try:
            os.remove(temp_file)
        except:
            pass
        
        # Si se solicita limpiar estado
        if clean_state:
            logger.info(f"Limpiando estado del procesador {processor_id}...")
            execute_curl(
                ["curl", "-k", "-s", 
                 "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                 "-X", "POST",
                 "-H", f"Authorization: Bearer {token}",
                 "-H", "Accept: application/json",
                 f"https://localhost:8443/nifi-api/processors/{processor_id}/state/clear-requests"],
                "CLEAR_STATE"
            )
        
        # 6. Verificar actualización
        time.sleep(2)  # Esperar un poco para que se apliquen los cambios
        cmd_verify = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_verify = execute_curl(cmd_verify, "VERIFY_SQL_UPDATE")
        
        if result_verify and result_verify.returncode == 0 and result_verify.stdout:
            verify_data = json.loads(result_verify.stdout)
            updated_properties = verify_data.get('component', {}).get('properties', {})
            
            # Verificar si las propiedades se actualizaron correctamente
            success = True
            details = {}
            
            for key, value in properties.items():
                if key in updated_properties:
                    if "query" in key.lower() and len(value) > 50:
                        # Para consultas largas, comparar los primeros 50 caracteres
                        expected_start = value[:50].strip()
                        actual_start = updated_properties[key][:50].strip() if len(updated_properties[key]) >= 50 else updated_properties[key].strip()
                        
                        match = expected_start in updated_properties[key]
                        details[key] = {
                            "updated": match,
                            "expected_start": expected_start,
                            "actual_start": actual_start
                        }
                        
                        if not match:
                            success = False
                    else:
                        # Para otros campos, verificación exacta
                        match = updated_properties[key] == value
                        details[key] = {"updated": match}
                        
                        if not match:
                            success = False
                else:
                    details[key] = {"updated": False, "error": "Propiedad no encontrada en respuesta"}
                    success = False
            
            return jsonify({
                "success": success,
                "message": "SQL actualizado exitosamente" if success else "Algunas propiedades no se actualizaron correctamente",
                "processor_id": processor_id,
                "details": details
            })
        else:
            return jsonify({"error": "No se pudo verificar la actualización"}), 500
            
    # PUT - Forzar actualización de consulta (comportamiento similar a force_update_processor_query)
    elif method == 'PUT':
        data = request.get_json() or {}
        
        # Verificar que tenemos los datos necesarios
        query = data.get('query')
        if not query:
            query = request.form.get('query') or request.args.get('query')
            
        if not query:
            return jsonify({"error": "Se requiere consulta SQL (query) para forzar actualización"}), 400
        
        # Obtener procesador
        cmd_get = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_get = execute_curl(cmd_get, "GET_FOR_FORCE_UPDATE")
        
        if not result_get or result_get.returncode != 0 or not result_get.stdout:
            return jsonify({"error": "No se pudo obtener estado del procesador"}), 500
        
        # Preparar datos para actualización forzada
        processor_data = json.loads(result_get.stdout)
        revision = processor_data.get('revision', {})
        component = processor_data.get('component', {})
        properties = dict(component.get('properties', {}))  # Crear copia
        
        # Forzar la consulta SQL
        properties['Custom Query'] = query
        
        # Eliminar propiedades problemáticas
        problematic_props = ["Maximum-value Columns", "start_date", "end_date"]
        for prop in problematic_props:
            if prop in properties:
                del properties[prop]
        
        # Datos para forzar actualización
        update_data = {
            'revision': revision,
            'component': {
                'id': processor_id,
                'properties': properties
            }
        }
        
        # Usar un archivo temporal
        temp_file = f"/tmp/force_sql_{processor_id}_{int(time.time())}.json"
        with open(temp_file, "w") as f:
            f.write(json.dumps(update_data))
        
        # Actualizar con fuerza
        cmd_update = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "PUT",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Content-Type: application/json",
            "-H", "Accept: application/json",
            "-d", "@" + temp_file,
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_update = execute_curl(cmd_update, "FORCE_SQL_UPDATE")
        
        # Eliminar temporal
        try:
            os.remove(temp_file)
        except:
            pass
        
        # Limpiar estado si se solicitó
        if clean_state:
            logger.info(f"Limpiando estado del procesador {processor_id}...")
            execute_curl(
                ["curl", "-k", "-s", 
                 "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
                 "-X", "POST",
                 "-H", f"Authorization: Bearer {token}",
                 "-H", "Accept: application/json",
                 f"https://localhost:8443/nifi-api/processors/{processor_id}/state/clear-requests"],
                "FORCE_CLEAR_STATE"
            )
        
        # Verificar con más tiempo de espera (crucial para actualizaciones forzadas)
        time.sleep(5)  # Mayor espera para forzar actualización
        
        cmd_verify = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}"
        ]
        
        result_verify = execute_curl(cmd_verify, "VERIFY_FORCE_UPDATE")
        
        if result_verify and result_verify.returncode == 0 and result_verify.stdout:
            verify_data = json.loads(result_verify.stdout)
            current_query = verify_data.get('component', {}).get('properties', {}).get('Custom Query', '')
            
            # Comparar eliminando espacios en blanco
            query_clean = ' '.join(query.strip().split())
            current_clean = ' '.join(current_query.strip().split())
            
            # Para consultas largas, comparar los primeros 100 caracteres
            if len(query_clean) > 100 and len(current_clean) > 100:
                success = query_clean[:100] == current_clean[:100]
            else:
                success = query_clean == current_clean
            
            return jsonify({
                "success": success,
                "query_updated": success,
                "processor_id": processor_id,
                "current_query": current_query[:200] + "..." if len(current_query) > 200 else current_query,
                "state_cleared": clean_state
            })
        else:
            return jsonify({"error": "No se pudo verificar la actualización forzada"}), 500
            
    # DELETE - Limpiar estado relacionado con la consulta
    elif method == 'DELETE':
        # Esta operación limpia el estado del procesador
        cmd = [
            "curl", "-k", "-s",
            "--connect-to", f"localhost:8443:{NIFI_HOST}:8443",
            "-X", "POST", # POST para la operación de limpieza, a pesar del método DELETE
            "-H", f"Authorization: Bearer {token}",
            "-H", "Accept: application/json",
            f"https://localhost:8443/nifi-api/processors/{processor_id}/state/clear-requests"
        ]
        
        result = execute_curl(cmd, "CLEAR_SQL_STATE")
        
        if result and result.returncode == 0:
            # Verificar si la limpieza se realizó correctamente
            try:
                if result.stdout and len(result.stdout) > 5:
                    clear_data = json.loads(result.stdout)
                    if 'request' in clear_data:
                        return jsonify({
                            "success": True,
                            "processor_id": processor_id,
                            "message": "Estado limpiado exitosamente",
                            "details": clear_data
                        })
                
                # Si no hay respuesta detallada pero el comando fue exitoso
                return jsonify({
                    "success": True,
                    "processor_id": processor_id,
                    "message": "Estado limpiado exitosamente"
                })
                
            except json.JSONDecodeError:
                # Si la respuesta no es JSON pero el comando fue exitoso
                return jsonify({
                    "success": True,
                    "processor_id": processor_id,
                    "message": "Comando de limpieza ejecutado",
                    "raw_response": result.stdout[:100] if result.stdout else "<vacío>"
                })
        else:
            return jsonify({
                "success": False,
                "processor_id": processor_id,
                "error": "No se pudo limpiar el estado",
                "details": result.stderr if result and result.stderr else "Error desconocido"
            }), 500


@app.route('/api/processors/<processor_id>/get-query')
@handle_errors
def get_processor_query(processor_id):
    """Endpoint legado para obtener consulta SQL (redirige a /api/processors/<id>/sql)"""
    return processor_sql_endpoint(processor_id=processor_id, token=get_token())

@app.route('/api/processors/<processor_id>/query-diagnostic')
@handle_errors
def diagnose_processor_query(processor_id):
    """Endpoint legado para diagnóstico de SQL (redirige a /api/processors/<id>/sql con diagnóstico)"""
    # Modificar args para solicitar diagnóstico
    request.args = dict(request.args)
    request.args['diagnostics'] = 'true'
    return processor_sql_endpoint(processor_id=processor_id, token=get_token())

@app.route('/api/processors/<processor_id>/force-update-query', methods=['POST'])
@handle_errors
def force_update_processor_query(processor_id):
    """Endpoint legado para forzar actualización (redirige a /api/processors/<id>/sql con PUT)"""
    # Cambiar método a PUT para forzar actualización
    request.method = 'PUT'
    return processor_sql_endpoint(processor_id=processor_id, token=get_token())

@app.route('/api/update-processor-sql', methods=['POST'])
@handle_errors
def update_processor_sql():
    """Endpoint legado para actualizar SQL (redirige a /api/processors/<id>/sql)"""
    data = request.get_json()
    if not data or 'processorId' not in data:
        return jsonify({"error": "Se requiere processorId"}), 400
        
    processor_id = data['processorId']
    
    # Agregar limpiar estado a los parámetros si se solicita
    request.args = dict(request.args)
    if data.get('cleanState'):
        request.args['clean_state'] = 'true'
        
    return processor_sql_endpoint(processor_id=processor_id, token=get_token())

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5001))
    logger.info(f"Iniciando mini-proxy NiFi en puerto {port}")
    logger.info(f"Configurado para conectar a {NIFI_HOST}:{NIFI_PORT}")
    logger.info(f"Modo de depuración: {'Activado' if DEBUG_MODE else 'Desactivado'}")
    app.run(host='0.0.0.0', port=port)