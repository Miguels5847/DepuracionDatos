"""
Configuraciones específicas para el DAG de potencia
"""

# Configuración específica de tablas
SOURCE_TABLE = "potencia_dep"
PROCESSED_TABLE = "potencia_dep_processed"
BACKUP_TABLE = "potencia_dep_original"

# Estructura de procesadores por grupo (sin IDs, solo nombres y tipos)
PRODUCER_PROCESSORS = [
    {"name": "P.1-QueryDatabaseTable", "type": "QueryDatabaseTable", "order": 1},
    {"name": "P.2-ConvertRecord", "type": "ConvertRecord", "order": 2},
    {"name": "P.3-SplitJson", "type": "SplitJson", "order": 3},
    {"name": "P.4-MergeContent", "type": "MergeContent", "order": 4},
    {"name": "P.5-PublishKafka", "type": "PublishKafka", "order": 5}
]

CONSUMER_PROCESSORS = [
    {"name": "C.1-ConsumeKafka", "type": "ConsumeKafka", "order": 1},
    {"name": "C.2-EvaluateJsonPath", "type": "EvaluateJsonPath", "order": 2},
    {"name": "C.3-ReplaceText", "type": "ReplaceText", "order": 3},
    {"name": "C.4-PutDatabaseRecord", "type": "PutDatabaseRecord", "order": 4}
]

# Cambiar estos IDs por los que funcionan en dag_prueba.py
PRODUCER_GROUP_ID = "4dcc0bed-9061-3a4c-0000-c375f77615eb"  # ID correcto
CONSUMER_GROUP_ID = "723a3709-dabb-3fae-0000-2892da95b9a6"  # ID correcto

# Configuración para la depuración de datos
COLUMNS_TO_VALIDATE = {
    "potencia_activa": {
        "min_value": 0,
        "max_value": 10000,
        "outlier_method": "std_dev",
        "replacement_method": "historical"
    },
    "potencia_reactiva": {
        "min_value": -5000,
        "max_value": 5000,
        "outlier_method": "iqr",
        "replacement_method": "historical"
    }
}

# Configuración para el análisis de outliers
OUTLIER_CONFIG = {
    "std_dev_threshold": 3.0,
    "iqr_factor": 1.5,
    "min_samples": 10
}

# Configuración para el reemplazo con datos históricos
HISTORICAL_REPLACEMENT = {
    "lookback_days": 7,
    "window_hours": 1,
    "fallback_strategy": "mean"
}

# Configuración de consulta SQL para cada mes (para personalización)
SQL_QUERIES_BY_MONTH = {
    1: """/* CONSULTA ENERO */
    SELECT fecha, hora, alimentador, dia_semana, potencia_activa, potencia_reactiva 
    FROM potencia_dep
    WHERE EXTRACT(MONTH FROM fecha) = 1 AND EXTRACT(YEAR FROM fecha) = 2019
    ORDER BY fecha, hora, alimentador""",
    
    # Agrega más meses según necesites
    # ...
}