import os
import json
import time
import requests
from datetime import datetime
from airflow.hooks.base import BaseHook
from airflow.models import Variable

class NiFiDiscovery:
    """
    Clase para descubrir y persistir IDs de procesadores NiFi,
    proporcionando una capa de abstracción sobre los mecanismos de búsqueda
    y garantizando consistencia entre ejecuciones.
    """
    
    def __init__(self, proxy_url=None, timeout=30):
        """
        Inicializa el descubridor de NiFi con configuración básica.
        
        Args:
            proxy_url: URL base del proxy NiFi
            timeout: Tiempo máximo de espera para peticiones HTTP
        """
        self.proxy_url = proxy_url or "http://mini_nifi_proxy_centrosur:5001"
        self.timeout = timeout
        self.cache_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "cache")
        self.cache_file = os.path.join(self.cache_dir, "nifi_processors.json")
        
        # Asegurar que el directorio de caché existe
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
    
    def get_processor_id(self, key, search_terms=None, fallback_id=None, force_refresh=False):
        """
        Obtiene el ID de un procesador con una estrategia de múltiples capas:
        1. Buscar en Variables de Airflow
        2. Buscar en caché local
        3. Buscar en el API de NiFi
        4. Usar ID fallback
        
        Args:
            key: Clave única para identificar el procesador (ej: 'query_db_table')
            search_terms: Lista de términos para buscar (ej: ['QueryDatabaseTable', 'Query'])
            fallback_id: ID por defecto a usar si no se encuentra
            force_refresh: Si True, ignorar caché y forzar búsqueda en NiFi
            
        Returns:
            str: ID del procesador encontrado o fallback
        """
        # Convertir key a nombres estándar
        airflow_var_name = f"nifi_processor_id_{key}"
        
        # 1. Intentar obtener de Variables de Airflow
        try:
            airflow_id = Variable.get(airflow_var_name, default_var=None)
            if airflow_id and not force_refresh:
                print(f"✅ ID para {key} obtenido de Variable Airflow: {airflow_id}")
                return airflow_id
        except Exception as e:
            print(f"⚠️ Error leyendo Variable Airflow: {str(e)}")
        
        # 2. Buscar en caché local
        if not force_refresh:
            cached_id = self._get_from_cache(key)
            if cached_id:
                print(f"✅ ID para {key} obtenido de caché local: {cached_id}")
                return cached_id
        
        # 3. Buscar en la API de NiFi
        if search_terms:
            nifi_id = self._search_in_nifi(key, search_terms)
            if nifi_id:
                # Guardar en variable Airflow y caché local para futuro uso
                self._save_id(key, nifi_id)
                return nifi_id
        
        # 4. Usar ID de fallback
        print(f"⚠️ No se encontró ID para {key}, usando fallback: {fallback_id}")
        return fallback_id
    
    def _search_in_nifi(self, key, search_terms):
        """Busca un procesador en NiFi usando múltiples términos de búsqueda"""
        try:
            for term in search_terms:
                print(f"🔍 Buscando procesador {key} usando término: {term}")
                response = requests.get(
                    f"{self.proxy_url}/api/flow/search-results?q={term}",
                    timeout=self.timeout
                )
                
                if response.status_code == 200:
                    search_data = response.json()
                    if ("searchResultsDTO" in search_data and 
                        "processorResults" in search_data["searchResultsDTO"]):
                        results = search_data["searchResultsDTO"]["processorResults"]
                        if results:
                            # Buscar coincidencia exacta primero
                            for proc in results:
                                if term.lower() == proc.get("name", "").lower():
                                    print(f"✓ Coincidencia exacta para {key}: {proc.get('name')} -> {proc.get('id')}")
                                    return proc.get("id")
                            
                            # Si no hay exacta, buscar parcial
                            for proc in results:
                                if term.lower() in proc.get("name", "").lower():
                                    print(f"✓ Coincidencia parcial para {key}: {proc.get('name')} -> {proc.get('id')}")
                                    return proc.get("id")
                                    
                            # Si no, tomar el primero
                            print(f"✓ Usando primer resultado para {key}: {results[0].get('name')} -> {results[0].get('id')}")
                            return results[0].get("id")
            
            return None
        except Exception as e:
            print(f"⚠️ Error buscando en NiFi: {str(e)}")
            return None
    
    def _get_from_cache(self, key):
        """Obtiene un ID desde la caché local"""
        try:
            if not os.path.exists(self.cache_file):
                return None
                
            with open(self.cache_file, 'r') as f:
                cache_data = json.load(f)
                
            # Verificar si el ID existe y no es muy antiguo (< 7 días)
            if key in cache_data:
                entry = cache_data[key]
                cached_time = datetime.fromisoformat(entry["timestamp"])
                current_time = datetime.now()
                if (current_time - cached_time).days < 7:
                    return entry["id"]
                    
            return None
        except Exception as e:
            print(f"⚠️ Error leyendo caché: {str(e)}")
            return None
    
    def _save_id(self, key, processor_id):
        """Guarda un ID en Variable de Airflow y caché local"""
        try:
            # 1. Guardar en Variable de Airflow
            var_name = f"nifi_processor_id_{key}"
            try:
                Variable.set(var_name, processor_id)
                print(f"✅ ID para {key} guardado en Variable Airflow")
            except Exception as e:
                print(f"⚠️ Error guardando Variable Airflow: {str(e)}")
            
            # 2. Guardar en caché local
            try:
                cache_data = {}
                if os.path.exists(self.cache_file):
                    with open(self.cache_file, 'r') as f:
                        cache_data = json.load(f)
                
                cache_data[key] = {
                    "id": processor_id,
                    "timestamp": datetime.now().isoformat()
                }
                
                with open(self.cache_file, 'w') as f:
                    json.dump(cache_data, f)
                    
                print(f"✅ ID para {key} guardado en caché local")
            except Exception as e:
                print(f"⚠️ Error guardando caché local: {str(e)}")
                
        except Exception as e:
            print(f"⚠️ Error general guardando ID: {str(e)}")