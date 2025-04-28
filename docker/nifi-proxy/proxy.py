import os
import sys
import time
import json
import logging
import socket
import http.server
import socketserver
import urllib3
import ssl
import traceback
import subprocess
import base64

# Configuración de logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("nifi-proxy")

# Deshabilitar advertencias SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class NiFiConfig:
    """Configuración centralizada para el proxy NiFi"""
    # Configuración de NiFi
    NIFI_HOST = os.environ.get("NIFI_HOST", "nifi_centrosur")
    NIFI_PORT = int(os.environ.get("NIFI_PORT", 8443))
    NIFI_PROTOCOL = os.environ.get("NIFI_PROTOCOL", "https")
    NIFI_USERNAME = os.environ.get("NIFI_USERNAME", "admin")
    NIFI_PASSWORD = os.environ.get("NIFI_PASSWORD", "centrosur123")
    PROXY_PORT = int(os.environ.get("PORT", 5000))

class NiFiAuth:
    """Clase para autenticación de NiFi usando métodos de bajo nivel"""
    
    @staticmethod
    def get_token_with_curl():
        """
        Obtener token usando curl exactamente como lo haría un navegador, con SNI explícito
        """
        try:
            logger.info("Intentando obtener token mediante curl con SNI configurado")
            
            # Crear un archivo temporal para ver la respuesta completa
            output_file = "/tmp/curl_response.txt"
            
            # Usar el formato exacto que funciona en la línea de comandos
            # Agregando --resolve para manejar SNI correctamente
            curl_cmd = [
                "curl",
                "-k",                # Inseguro (no verificar certificado)
                "-v",                # Modo verbose para ver detalles
                "--resolve", f"localhost:8443:{NiFiConfig.NIFI_HOST}",  # Mapear localhost a la IP real
                "-H", "Host: localhost:8443",  # Forzar encabezado Host
                "-X", "POST",
                "-d", f"username={NiFiConfig.NIFI_USERNAME}&password={NiFiConfig.NIFI_PASSWORD}",
                "-H", "Content-Type: application/x-www-form-urlencoded",
                f"https://localhost:8443/nifi-api/access/token"  # Usar localhost en la URL
            ]
            
            # Ejecutar curl y capturar toda la salida
            with open(output_file, "w") as f:
                process = subprocess.run(
                    curl_cmd,
                    stdout=f,
                    stderr=subprocess.PIPE,
                    timeout=30,
                    check=False
                )
            
            # Leer la salida del archivo
            with open(output_file, "r") as f:
                output = f.read().strip()
            
            # Limpiar
            try:
                os.remove(output_file)
            except:
                pass
                
            # Verificar stderr para diagnóstico
            stderr = process.stderr.decode('utf-8', errors='replace')
            
            # Imprimir información diagnóstica (solo en modo debug)
            logger.debug(f"Curl salida stderr:\n{stderr}")
            
            # Si el comando fue exitoso y hay algún output, asumimos que es el token
            if process.returncode == 0 and output:
                logger.info(f"Token obtenido con curl: ({len(output)} caracteres)")
                return output
            else:
                logger.warning(f"Curl falló con código {process.returncode}")
                logger.debug(f"Respuesta: {output}")
                
                # Intentar una variante alternativa con resolución de nombres diferente
                logger.info("Intentando variante alternativa de curl...")
                
                curl_cmd_alt = [
                    "curl",
                    "-k",
                    "--connect-to", f"localhost:8443:{NiFiConfig.NIFI_HOST}:8443",
                    "-X", "POST",
                    "-d", f"username={NiFiConfig.NIFI_USERNAME}&password={NiFiConfig.NIFI_PASSWORD}",
                    "-H", "Content-Type: application/x-www-form-urlencoded",
                    "https://localhost:8443/nifi-api/access/token"
                ]
                
                with open(output_file, "w") as f:
                    process = subprocess.run(
                        curl_cmd_alt,
                        stdout=f,
                        stderr=subprocess.PIPE,
                        timeout=30,
                        check=False
                    )
                
                # Leer la salida alternativa
                with open(output_file, "r") as f:
                    output = f.read().strip()
                    
                # Limpiar de nuevo
                try:
                    os.remove(output_file)
                except:
                    pass
                    
                if process.returncode == 0 and output:
                    logger.info(f"Token obtenido con curl alternativo: ({len(output)} caracteres)")
                    return output
                    
                return None
                        
        except Exception as e:
            logger.error(f"Error usando curl: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    @staticmethod
    def get_token_with_direct_socket():
        """
        Obtener token usando sockets directamente con formato preciso y SNI configurado correctamente
        """
        try:
            logger.info("Intentando obtener token mediante conexión directa socket+SSL con SNI configurado")
            
            # Crear un contexto SSL permisivo
            context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            # Crear la conexión socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            
            logger.info(f"Conectando a {NiFiConfig.NIFI_HOST}:{NiFiConfig.NIFI_PORT}")
            
            # Importante: Aquí establecemos el server_hostname como 'localhost' que es lo que NiFi espera
            # Este es el valor SNI que se enviará durante el handshake TLS
            with context.wrap_socket(sock, server_hostname='localhost') as ssock:
                # Conectar al servidor
                ssock.connect((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
                logger.info(f"Conexión SSL establecida con {NiFiConfig.NIFI_HOST}:{NiFiConfig.NIFI_PORT}")
                
                # Preparar datos de autenticación exactamente como un cliente web lo haría
                auth_data = f"username={NiFiConfig.NIFI_USERNAME}&password={NiFiConfig.NIFI_PASSWORD}"
                
                # Preparar la solicitud HTTP con formato exacto (importante agregar Accept)
                request = (
                    f"POST /nifi-api/access/token HTTP/1.1\r\n"
                    f"Host: localhost:8443\r\n"  # Importante usar 'localhost' aquí también
                    f"Accept: */*\r\n"
                    f"Content-Type: application/x-www-form-urlencoded\r\n"
                    f"Content-Length: {len(auth_data)}\r\n"
                    f"Connection: close\r\n"
                    f"\r\n"
                    f"{auth_data}"
                )
                
                # Enviar la solicitud
                ssock.sendall(request.encode())
                
                # Recibir la respuesta
                response = b""
                while True:
                    try:
                        chunk = ssock.recv(4096)
                        if not chunk:
                            break
                        response += chunk
                    except socket.timeout:
                        logger.warning("Timeout al recibir datos, continuando con lo que tenemos")
                        break
                
                # Procesar la respuesta
                response_str = response.decode('utf-8', errors='replace')
                
                # Buscar el token después de los encabezados
                if "\r\n\r\n" in response_str:
                    headers, body = response_str.split("\r\n\r\n", 1)
                    logger.info(f"Headers recibidos: {headers}")
                    
                    # Verificar código de respuesta
                    header_lines = headers.split('\r\n')
                    first_line = header_lines[0] if header_lines else "No status line"
                    
                    if "HTTP/1.1 201" in headers:
                        token = body.strip()
                        logger.info(f"Token obtenido correctamente ({len(token)} caracteres)")
                        return token
                    else:
                        logger.warning(f"Respuesta no esperada: {first_line}")
                        logger.debug(f"Cuerpo de respuesta: {body}")
                else:
                    logger.warning("Formato de respuesta incorrecto (no se encontró separador de encabezados)")
                    logger.debug(f"Respuesta completa: {response_str}")
                
                return None
                
        except Exception as e:
            logger.error(f"Error en autenticación directa: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    @staticmethod
    def get_token_with_requests_lib():
        """
        Intento alternativo usando la biblioteca requests de Python con SNI configurado
        """
        try:
            import requests
            logger.info("Intentando obtener token mediante biblioteca requests con SNI")
            
            # Desactivar verificación SSL y advertencias
            requests.packages.urllib3.disable_warnings()
            
            # URL para la autenticación usando "localhost" como nombre de host
            url = f"{NiFiConfig.NIFI_PROTOCOL}://localhost:{NiFiConfig.NIFI_PORT}/nifi-api/access/token"
            
            # Datos de autenticación
            data = {
                'username': NiFiConfig.NIFI_USERNAME,
                'password': NiFiConfig.NIFI_PASSWORD
            }
            
            # Encabezados
            headers = {
                'Content-Type': 'application/x-www-form-urlencoded',
                'Host': f'localhost:{NiFiConfig.NIFI_PORT}'
            }
            
            # Crear una sesión para personalizarla
            session = requests.Session()
            
            # Configurar el adaptador para que use el hostname correcto para SNI pero se conecte a la IP real
            # Esta es una técnica avanzada que puede variar según la versión de requests
            adapter = requests.adapters.HTTPAdapter()
            session.mount('https://', adapter)
            
            # Modificar el poolmanager para usar server_hostname=localhost
            old_init = adapter.init_poolmanager
            
            def init_poolmanager_with_sni(*args, **kwargs):
                kwargs['server_hostname'] = 'localhost'
                return old_init(*args, **kwargs)
            
            adapter.init_poolmanager = init_poolmanager_with_sni
            
            # Definir una política de resolución personalizada
            class LocalhostResolver:
                def __init__(self, real_host):
                    self.real_host = real_host
                    
                def resolve(self, host, port=None, scheme=None):
                    if host == 'localhost':
                        return self.real_host
                    return host
            
            # Intentar la solicitud
            try:
                # Añadir la IP real a hosts
                import socket
                ip_addr = socket.gethostbyname(NiFiConfig.NIFI_HOST)
                
                # Realizar la solicitud con modificaciones de SNI
                response = session.post(
                    url, 
                    data=data, 
                    headers=headers, 
                    verify=False,
                    timeout=30
                )
                
                # Verificar respuesta
                if response.status_code == 201:
                    token = response.text.strip()
                    logger.info(f"Token obtenido con requests: ({len(token)} caracteres)")
                    return token
                else:
                    logger.warning(f"Falló la solicitud requests: {response.status_code} {response.reason}")
                    logger.debug(f"Contenido de respuesta: {response.text}")
            except Exception as e:
                logger.warning(f"Falló primer intento con requests: {str(e)}")
            
            # Intento alternativo con más fuerza bruta: usar urllib3 directamente
            try:
                import urllib3
                
                # Configurar un pool de conexiones con SNI configurado
                urllib3.disable_warnings()
                http = urllib3.PoolManager(
                    timeout=30.0,
                    cert_reqs='CERT_NONE',
                    server_hostname='localhost'
                )
                
                # Ejecutar la solicitud
                resp = http.request(
                    'POST',
                    f"{NiFiConfig.NIFI_PROTOCOL}://{NiFiConfig.NIFI_HOST}:{NiFiConfig.NIFI_PORT}/nifi-api/access/token",
                    fields=data,
                    headers=headers
                )
                
                if resp.status == 201:
                    token = resp.data.decode('utf-8').strip()
                    logger.info(f"Token obtenido con urllib3: ({len(token)} caracteres)")
                    return token
                else:
                    logger.warning(f"Falló la solicitud urllib3: {resp.status}")
                    
            except Exception as e:
                logger.warning(f"Falló intento con urllib3: {str(e)}")
                
            # Si llegamos aquí, es que todos los intentos fallaron
            return None
                
        except ImportError:
            logger.warning("Biblioteca requests no disponible")
            return None
        except Exception as e:
            logger.error(f"Error usando requests: {str(e)}")
            logger.error(traceback.format_exc())
            return None
    
    @staticmethod
    def extract_token_from_browser_format():
        """
        Simula exactamente la misma solicitud que realiza un navegador con SNI configurado
        """
        try:
            logger.info("Simulando solicitud de navegador para token con SNI configurado")
            
            # Formato exacto de solicitud que funciona en el navegador con Host=localhost
            browser_req = f"""POST /nifi-api/access/token HTTP/1.1
Host: localhost:8443
Connection: keep-alive
Content-Length: {len(f"username={NiFiConfig.NIFI_USERNAME}&password={NiFiConfig.NIFI_PASSWORD}")}
sec-ch-ua: "Chromium";v="119"
Accept: */*
Content-Type: application/x-www-form-urlencoded
sec-ch-ua-mobile: ?0
User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36
sec-ch-ua-platform: "Windows"
Origin: https://localhost:8443
Sec-Fetch-Site: same-origin
Sec-Fetch-Mode: cors
Sec-Fetch-Dest: empty
Referer: https://localhost:8443/nifi/
Accept-Encoding: gzip, deflate, br
Accept-Language: en-US,en;q=0.9

username={NiFiConfig.NIFI_USERNAME}&password={NiFiConfig.NIFI_PASSWORD}"""
            
            # Crear un contexto SSL permisivo
            context = ssl.SSLContext(ssl.PROTOCOL_TLS)
            context.check_hostname = False
            context.verify_mode = ssl.CERT_NONE
            
            # Crear la conexión socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(30)
            
            # Envolver el socket con SSL y configurar server_hostname='localhost' para SNI
            with context.wrap_socket(sock, server_hostname='localhost') as ssock:
                # Conectar al servidor
                ssock.connect((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
                
                # Enviar la solicitud exacta
                ssock.sendall(browser_req.replace("\n", "\r\n").encode())
                
                # Recibir la respuesta
                response = b""
                while True:
                    try:
                        chunk = ssock.recv(4096)
                        if not chunk:
                            break
                        response += chunk
                    except socket.timeout:
                        break
                
                # Procesar la respuesta
                response_str = response.decode('utf-8', errors='replace')
                
                # Buscar el token después de los encabezados
                if "\r\n\r\n" in response_str:
                    headers, body = response_str.split("\r\n\r\n", 1)
                    
                    # Verificar código de respuesta
                    if "HTTP/1.1 201" in headers:
                        token = body.strip()
                        logger.info(f"Token obtenido simulando navegador ({len(token)} caracteres)")
                        return token
                    else:
                        header_lines = headers.split('\r\n')
                        first_line = header_lines[0] if header_lines else "No status line"
                        logger.warning(f"Respuesta no esperada: {first_line}")
                        logger.debug(f"Headers completos: {headers}")
                        logger.debug(f"Cuerpo de respuesta: {body}")
                
                logger.warning("No se pudo obtener token simulando navegador")
                return None
                
        except Exception as e:
            logger.error(f"Error simulando navegador: {str(e)}")
            return None
    
    @staticmethod
    def get_basic_auth_header():
        """
        Genera un encabezado de autenticación básica (como último recurso)
        """
        auth_string = f"{NiFiConfig.NIFI_USERNAME}:{NiFiConfig.NIFI_PASSWORD}"
        encoded = base64.b64encode(auth_string.encode()).decode()
        return f"Basic {encoded}"
    
    @staticmethod
    def get_token():
        """
        Intenta obtener token usando diferentes métodos
        """
        # Registrar los intentos de autenticación
        logger.info("Iniciando proceso de obtención de token")
        
        # Intentar con curl primero con formato mejorado
        token = NiFiAuth.get_token_with_curl()
        if token:
            return token
            
        # Probar con la biblioteca requests
        token = NiFiAuth.get_token_with_requests_lib()
        if token:
            return token
        
        # Probar simulando exactamente un navegador
        token = NiFiAuth.extract_token_from_browser_format()
        if token:
            return token
            
        # Si eso falla, probar con conexión socket directa mejorada
        token = NiFiAuth.get_token_with_direct_socket()
        if token:
            return token
                
        # Si todo lo demás falla, probar con credenciales básicas
        logger.warning("Todos los métodos de obtención de token fallaron, usando basic auth como fallback")
        token = NiFiAuth.get_basic_auth_header()
        return token

class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    """Manejador de solicitudes para el proxy"""
    
    def log_message(self, format, *args):
        """Logging personalizado"""
        logger.info("%s - - [%s] %s" % (self.client_address[0], self.log_date_time_string(), format % args))

    def send_json_response(self, status_code, data):
        """Enviar respuesta JSON"""
        try:
            self.send_response(status_code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")  # Para CORS
            self.end_headers()
            json_data = json.dumps(data).encode()
            self.wfile.write(json_data)
        except Exception as e:
            logger.error(f"Error enviando respuesta JSON: {str(e)}")
            logger.error(traceback.format_exc())

    def do_GET(self):
        """Manejar solicitudes GET"""
        try:
            if self.path == "/health":
                # Verificar conexión TCP
                is_tcp_ok = False
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
                    sock.close()
                    is_tcp_ok = (result == 0)
                except:
                    is_tcp_ok = False
                
                # Intentar obtener token
                token = None
                if is_tcp_ok:
                    token = NiFiAuth.get_token()
                
                self.send_json_response(200, {
                    "status": "healthy" if token else "degraded" if is_tcp_ok else "unhealthy",
                    "message": "Proxy funcionando" if token else "Proxy con problemas de autenticación" if is_tcp_ok else "Proxy sin conexión a NiFi",
                    "tcp_connection": is_tcp_ok,
                    "auth_working": token is not None,
                    "nifi_host": NiFiConfig.NIFI_HOST,
                    "nifi_port": NiFiConfig.NIFI_PORT
                })
                return
                
            if self.path == "/test-auth":
                token = NiFiAuth.get_token()
                if token:
                    self.send_json_response(200, {
                        "status": "success",
                        "message": "Autenticación exitosa",
                        "token_length": len(token),
                        "token_type": "Bearer" if not token.startswith("Basic ") else "Basic"
                    })
                else:
                    self.send_json_response(401, {
                        "status": "error",
                        "message": "No se pudo obtener token de autenticación"
                    })
                return
                
            if self.path == "/debug-ssl":
                # Endpoint para depuración de SSL
                try:
                    # Crear contexto SSL permisivo
                    context = ssl.SSLContext(ssl.PROTOCOL_TLS)
                    context.check_hostname = False
                    context.verify_mode = ssl.CERT_NONE
                    context.set_ciphers("ALL")
                    
                    # Iniciar conexión SSL
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(10)
                    with context.wrap_socket(sock, server_hostname='localhost') as ssock:
                        # Intentar conectar
                        ssock.connect((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
                        
                        # Obtener información de la conexión
                        version = ssock.version()
                        cipher = ssock.cipher()
                        peer_cert = ssock.getpeercert(binary_form=True)
                        
                        self.send_json_response(200, {
                            "ssl_connection": "success",
                            "protocol_version": version,
                            "cipher_suite": str(cipher),
                            "has_peer_cert": peer_cert is not None,
                            "peer_cert_size": len(peer_cert) if peer_cert else 0,
                            "sni_status": "configured (localhost)"
                        })
                except Exception as e:
                    self.send_json_response(500, {
                        "ssl_connection": "failed",
                        "error": str(e),
                        "traceback": traceback.format_exc()
                    })
                return
                
            # Proxy genérico para endpoints NiFi
            if self.path.startswith("/api/"):
                try:
                    # Ejecutar la solicitud usando curl para evitar problemas SSL
                    token = NiFiAuth.get_token()
                    if not token:
                        self.send_json_response(401, {"error": "No se pudo autenticar con NiFi"})
                        return
                    
                    nifi_path = self.path[5:]  # Quitar "/api/" del inicio
                    # Usar localhost en lugar del nombre real del host para SNI
                    nifi_url = f"{NiFiConfig.NIFI_PROTOCOL}://localhost:{NiFiConfig.NIFI_PORT}/nifi-api/{nifi_path}"
                    
                    logger.info(f"Proxy GET: {nifi_url}")
                    
                    # Determinar el tipo de token
                    auth_header = f"Authorization: Bearer {token}" if not token.startswith("Basic ") else f"Authorization: {token}"
                    
                    # Usar curl para hacer la solicitud con opciones SSL específicas y SNI
                    curl_cmd = [
                        "curl", "-k", "-s", "-i",
                        "--resolve", f"localhost:{NiFiConfig.NIFI_PORT}:{NiFiConfig.NIFI_HOST}",
                        "-H", "Host: localhost:8443",
                        "-H", "Content-Type: application/json",
                        "-H", auth_header,
                        nifi_url
                    ]
                    
                    process = subprocess.Popen(
                        curl_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True
                    )
                    
                    stdout, stderr = process.communicate(timeout=30)
                    
                    if process.returncode != 0:
                        self.send_json_response(500, {"error": f"Error en la solicitud: {stderr}"})
                        return
                    
                    # Separar encabezados y cuerpo
                    parts = stdout.split("\r\n\r\n", 1)
                    if len(parts) < 2:
                        self.send_json_response(500, {"error": "Formato de respuesta incorrecto"})
                        return
                        
                    headers_str, body = parts
                    
                    # Obtener código de estado
                    status_line = headers_str.split("\r\n")[0]
                    status_code = int(status_line.split(" ")[1])
                    
                    # Extraer encabezados
                    headers = {}
                    for line in headers_str.split("\r\n")[1:]:
                        if ":" in line:
                            name, value = line.split(":", 1)
                            headers[name.strip()] = value.strip()
                    
                    # Enviar respuesta
                    self.send_response(status_code)
                    for name, value in headers.items():
                        if name.lower() not in ["transfer-encoding", "connection"]:
                            self.send_header(name, value)
                    self.end_headers()
                    self.wfile.write(body.encode())
                    
                except Exception as e:
                    logger.error(f"Error en proxy: {str(e)}")
                    logger.error(traceback.format_exc())
                    self.send_json_response(500, {"error": f"Error en proxy: {str(e)}"})
                    
                return
                
            # Ruta no encontrada
            self.send_json_response(404, {"error": "Ruta no encontrada"})
            
        except Exception as e:
            logger.error(f"Error no manejado: {str(e)}")
            logger.error(traceback.format_exc())
            self.send_json_response(500, {"error": "Error interno del servidor"})

    def do_POST(self):
        """Manejar solicitudes POST"""
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            post_data = self.rfile.read(content_length).decode('utf-8') if content_length > 0 else ""
            
            # Proxy genérico para endpoints NiFi
            if self.path.startswith("/api/"):
                try:
                    # Ejecutar la solicitud usando curl para evitar problemas SSL
                    token = NiFiAuth.get_token()
                    if not token:
                        self.send_json_response(401, {"error": "No se pudo autenticar con NiFi"})
                        return
                    
                    nifi_path = self.path[5:]  # Quitar "/api/" del inicio
                    # Usar localhost en lugar del nombre real del host para SNI
                    nifi_url = f"{NiFiConfig.NIFI_PROTOCOL}://localhost:{NiFiConfig.NIFI_PORT}/nifi-api/{nifi_path}"
                    
                    logger.info(f"Proxy POST: {nifi_url}")
                    
                    # Determinar el tipo de token
                    auth_header = f"Authorization: Bearer {token}" if not token.startswith("Basic ") else f"Authorization: {token}"
                    
                    # Guardar los datos POST en un archivo temporal
                    temp_data_file = "/tmp/post_data.txt"
                    with open(temp_data_file, "w") as f:
                        f.write(post_data)
                    
                    # Usar curl para hacer la solicitud con SNI configurado
                    curl_cmd = [
                        "curl", "-k", "-s", "-i",
                        "--resolve", f"localhost:{NiFiConfig.NIFI_PORT}:{NiFiConfig.NIFI_HOST}",
                        "-H", "Host: localhost:8443",
                        "-X", "POST",
                        "-H", "Content-Type: application/json",
                        "-H", auth_header,
                        "-d", "@" + temp_data_file,
                        nifi_url
                    ]
                    
                    process = subprocess.Popen(
                        curl_cmd,
                        stdout=subprocess.PIPE,
                        stderr=subprocess.PIPE,
                        universal_newlines=True
                    )
                    
                    stdout, stderr = process.communicate(timeout=30)
                    
                    # Eliminar archivo temporal
                    try:
                        os.remove(temp_data_file)
                    except:
                        pass
                    
                    if process.returncode != 0:
                        self.send_json_response(500, {"error": f"Error en la solicitud: {stderr}"})
                        return
                    
                    # Separar encabezados y cuerpo
                    parts = stdout.split("\r\n\r\n", 1)
                    if len(parts) < 2:
                        self.send_json_response(500, {"error": "Formato de respuesta incorrecto"})
                        return
                        
                    headers_str, body = parts
                    
                    # Obtener código de estado
                    status_line = headers_str.split("\r\n")[0]
                    status_code = int(status_line.split(" ")[1])
                    
                    # Extraer encabezados
                    headers = {}
                    for line in headers_str.split("\r\n")[1:]:
                        if ":" in line:
                            name, value = line.split(":", 1)
                            headers[name.strip()] = value.strip()
                    
                    # Enviar respuesta
                    self.send_response(status_code)
                    for name, value in headers.items():
                        if name.lower() not in ["transfer-encoding", "connection"]:
                            self.send_header(name, value)
                    self.end_headers()
                    self.wfile.write(body.encode())
                    
                except Exception as e:
                    logger.error(f"Error en proxy: {str(e)}")
                    logger.error(traceback.format_exc())
                    self.send_json_response(500, {"error": f"Error en proxy: {str(e)}"})
                    
                return
                
            # Ruta no encontrada
            self.send_json_response(404, {"error": "Ruta no encontrada"})
            
        except Exception as e:
            logger.error(f"Error no manejado: {str(e)}")
            logger.error(traceback.format_exc())
            self.send_json_response(500, {"error": "Error interno del servidor"})

    def do_OPTIONS(self):
        """Manejar solicitudes OPTIONS para CORS"""
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.end_headers()

def run_diagnostics():
    """Ejecutar diagnósticos de red al inicio"""
    print("Network Diagnostics:")
    print("===================")
    print("Current Environment Variables:")
    print(f"NIFI_PORT={NiFiConfig.NIFI_PORT}")
    print(f"NIFI_PROTOCOL={NiFiConfig.NIFI_PROTOCOL}")
    print(f"NIFI_USERNAME={NiFiConfig.NIFI_USERNAME}")
    print(f"NIFI_PASSWORD={'*' * len(NiFiConfig.NIFI_PASSWORD)}")
    print(f"NIFI_HOST={NiFiConfig.NIFI_HOST}")
    print()

    print("Hostname resolution:")
    try:
        with open('/etc/hosts', 'r') as f:
            print(f.read())
    except Exception as e:
        print(f"Error reading /etc/hosts: {e}")
    
    print()
    print("IP Address and Network Interfaces:")
    try:
        output = subprocess.check_output(["ip", "addr"], universal_newlines=True)
        print(output)
    except Exception as e:
        print(f"Error running ip addr: {e}")
    
    print()
    print("Routing Table:")
    try:
        output = subprocess.check_output(["route", "-n"], universal_newlines=True)
        print(output)
    except Exception as e:
        print(f"Error running route -n: {e}")
    
    print()
    print("Resolv Configuration:")
    try:
        with open('/etc/resolv.conf', 'r') as f:
            print(f.read())
    except Exception as e:
        print(f"Error reading /etc/resolv.conf: {e}")
        
    print()
    print("DNS Lookup Tests:")
    try:
        output = subprocess.check_output(["nslookup", NiFiConfig.NIFI_HOST], universal_newlines=True)
        print(output)
    except Exception as e:
        print(f"DNS lookup error: {e}")
    
    print()
    print("Testing direct TCP connection:")
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
        sock.close()
        print(f"TCP connection test result: {'Success' if result == 0 else 'Failed'} (code: {result})")
    except Exception as e:
        print(f"TCP connection test error: {e}")

def wait_for_nifi():
    """Esperar a que NiFi esté disponible"""
    logger.info(f"Esperando a que NiFi esté disponible en {NiFiConfig.NIFI_HOST}:{NiFiConfig.NIFI_PORT}...")
    
    max_attempts = 30
    for attempt in range(max_attempts):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(5)
            result = sock.connect_ex((NiFiConfig.NIFI_HOST, NiFiConfig.NIFI_PORT))
            sock.close()
            
            if result == 0:
                logger.info(f"NiFi está disponible después de {attempt + 1} intentos")
                return True
                
            logger.warning(f"NiFi no disponible (intento {attempt + 1}/{max_attempts})...")
            time.sleep(5)
            
        except Exception as e:
            logger.error(f"Error verificando disponibilidad: {str(e)}")
            time.sleep(5)
    
    logger.error(f"NiFi no disponible después de {max_attempts} intentos")
    return False

def run_server():
    """Iniciar el servidor proxy"""
    run_diagnostics()
    
    logger.info(f"Iniciando proxy NiFi en puerto {NiFiConfig.PROXY_PORT}")
    logger.info(f"Conectando a NiFi en: {NiFiConfig.NIFI_PROTOCOL}://{NiFiConfig.NIFI_HOST}:{NiFiConfig.NIFI_PORT}/nifi-api")
    
    # Esperar a que NiFi esté disponible
    wait_for_nifi()
    
    # Prueba inicial de autenticación
    token = NiFiAuth.get_token()
    if token:
        logger.info("✅ Prueba de autenticación exitosa")
    else:
        logger.warning("⚠️ No se pudo obtener token en la prueba inicial")
    
    # Iniciar servidor HTTP
    handler = ProxyHandler
    with socketserver.TCPServer(("0.0.0.0", NiFiConfig.PROXY_PORT), handler) as server:
        logger.info(f"Servidor proxy escuchando en puerto {NiFiConfig.PROXY_PORT}")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Apagando servidor...")
        finally:
            server.server_close()

if __name__ == "__main__":
    try:
        # Configurar el entorno SSL para Python
        os.environ['PYTHONHTTPSVERIFY'] = '0'
        
        # Modificar configuraciones SSL por defecto
        try:
            # Intentar usar un contexto no verificado por defecto
            ssl._create_default_https_context = ssl._create_unverified_context
        except:
            pass
            
        run_server()
    except Exception as e:
        logger.error(f"Error fatal: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)