"""
DAG para diagnosticar el proxy NiFi - ENFOQUE SIMPLIFICADO
"""
from datetime import datetime, timedelta
import json
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

# Configuración
PROXY_BASE_URL = "http://nifi_proxy_centrosur:5000"
HTTP_TIMEOUT = 60

# Función para probar diferentes endpoints
def probar_endpoint(endpoint, description):
    """Prueba un endpoint específico del proxy"""
    full_url = f"{PROXY_BASE_URL}/{endpoint}"
    print(f"Probando endpoint: {full_url} ({description})")
    
    try:
        response = requests.get(full_url, timeout=HTTP_TIMEOUT)
        status = response.status_code
        print(f"Código de estado: {status}")
        
        if status == 200:
            try:
                data = response.json()
                # Imprimir solo los primeros 500 caracteres para evitar logs enormes
                print(f"Respuesta: {json.dumps(data)[:500]}... (truncada)")
                return f"Éxito ({status}): {description}"
            except Exception as json_err:
                print(f"Error al procesar JSON: {str(json_err)}")
                return f"Error de formato ({status}): {description}"
        else:
            error_text = response.text[:200] if response.text else "No hay texto de respuesta"
            print(f"Error en la respuesta: {error_text}")
            return f"Error ({status}): {description}"
    except Exception as e:
        print(f"Excepción: {str(e)}")
        return f"Excepción: {description} - {str(e)}"

# DAG
dag = DAG(
    'nifi_proxy_diagnostico',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 2, 1),
        'retries': 1,
    },
    description='Diagnóstico básico del proxy NiFi',
    schedule_interval=None,
    catchup=False,
    tags=['centrosur', 'diagnostico'],
)

# Tareas
with dag:
    health_check = PythonOperator(
        task_id='health_check',
        python_callable=lambda: probar_endpoint("health", "Verificar salud del proxy")
    )
    
    flow_check = PythonOperator(
        task_id='flow_check',
        python_callable=lambda: probar_endpoint("api/flow/root", "Obtener flujo raíz de NiFi")
    )
    
    controller_check = PythonOperator(
        task_id='controller_check',
        python_callable=lambda: probar_endpoint("api/controller/services", "Listar servicios controladores")
    )
    
    # Configuración de dependencias
    health_check >> flow_check >> controller_check