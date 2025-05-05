"""
Configuraciones compartidas entre todos los DAGs
"""

# Configuración de proxy NiFi
MINI_PROXY_URL = "http://mini_nifi_proxy_centrosur:5001"
HTTP_TIMEOUT = 60  # segundos

# Configuración de base de datos
DB_CONNECTIONS = {
    "origen": "postgres_centrosur",
    "destino": "postgres_base_prueba"
}

# Configuración de reintentos
DEFAULT_RETRIES = 2
RETRY_DELAY_MINUTES = 2

# Configuración de procesamiento
DEFAULT_WAIT_TIME_MINUTES = 12
EXTENDED_WAIT_TIME_MINUTES = 20  # Para meses con más datos (ej: diciembre)
VERIFICATION_INTERVAL_SECONDS = 60
STABILITY_THRESHOLD = 5  # Verificaciones con conteo estable para considerar completado

# Configuración de depuración
OUTLIER_THRESHOLD = 3.0  # Desviaciones estándar para detectar outliers
NULL_REPLACEMENT_STRATEGY = "historical_mean"  # Estrategia para reemplazar nulos