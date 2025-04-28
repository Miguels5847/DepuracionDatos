# Guía de Solución de Problemas

Esta guía proporciona soluciones para los problemas más comunes que pueden surgir durante la implementación y operación del sistema de depuración de datos de la Empresa Eléctrica Regional Centro Sur C.A.

## Tabla de Contenidos

- [Problemas de Instalación](#problemas-de-instalación)
- [Problemas con NiFi](#problemas-con-nifi)
- [Problemas con PostgreSQL](#problemas-con-postgresql)
- [Problemas con Kafka](#problemas-con-kafka)
- [Problemas con Airflow](#problemas-con-airflow)
- [Problemas con NiFi Proxy](#problemas-con-nifi-proxy)
- [Problemas de Comunicación entre Servicios](#problemas-de-comunicación-entre-servicios)
- [Problemas con DAGs de Airflow](#problemas-con-dags-de-airflow)
- [Problemas de Rendimiento](#problemas-de-rendimiento)
- [Cómo Solicitar Ayuda](#cómo-solicitar-ayuda)

## Problemas de Instalación

### Problema: Docker Compose no inicia correctamente

**Síntomas:** Errores durante `docker-compose up -d` o algunos contenedores fallan inmediatamente.

**Soluciones:**

1. Verificar la versión de Docker y Docker Compose:
   ```bash
   docker --version
   docker-compose --version
   ```
   Asegúrate de tener Docker 20.10.0+ y Docker Compose 2.0.0+.

2. Verificar espacio en disco:
   ```bash
   df -h
   ```
   Debes tener al menos 10GB libres.

3. Verificar que los puertos no estén en uso:
   ```bash
   sudo netstat -tulpn | grep -E '5432|5050|8080|8081|8443|9092|5000|5001'
   ```

4. Reiniciar Docker:
   ```bash
   # Linux
   sudo systemctl restart docker

   # Windows (PowerShell como administrador)
   Restart-Service docker
   ```

5. Reconstruir los contenedores:
   ```bash
   docker-compose down
   docker-compose build --no-cache
   docker-compose up -d
   ```

### Problema: Conflictos de red

**Síntomas:** Errores sobre direcciones IP duplicadas o conflictos de red.

**Soluciones:**

1. Verificar redes existentes:
   ```bash
   docker network ls
   ```

2. Eliminar la red conflictiva:
   ```bash
   docker network rm centrosur_network
   ```

3. Modificar la subred en docker-compose.yml:
   ```yaml
   networks:
     centrosur-network:
       ipam:
         config:
           - subnet: 172.20.0.0/16  # Cambia a otra subred
   ```

## Problemas con NiFi

### Problema: NiFi no inicia o se reinicia continuamente

**Síntomas:** El contenedor de NiFi se reinicia repetidamente o no se puede acceder a la interfaz web.

**Soluciones:**

1. Verificar logs:
   ```bash
   docker logs nifi_centrosur
   ```
   Busca mensajes de error específicos.

2. Verificar permisos:
   ```bash
   docker exec -it nifi_centrosur bash -c "ls -la /opt/nifi/nifi-current"
   ```
   El usuario nifi debe tener permisos de escritura.

3. Reiniciar con volúmenes limpios:
   ```bash
   docker-compose down
   docker volume rm nifi_conf nifi_state nifi_database_repository nifi_flowfile_repository
   docker-compose up -d nifi_centrosur
   ```

4. Aumentar memoria asignada:
   Modifica en docker-compose.yml:
   ```yaml
   nifi_centrosur:
     environment:
       - NIFI_JVM_HEAP_INIT=2g
       - NIFI_JVM_HEAP_MAX=4g
   ```

### Problema: No se puede acceder a NiFi a través de HTTPS

**Síntomas:** Errores de certificado o conexión rechazada al intentar acceder a https://localhost:8443/nifi.

**Soluciones:**

1. Verificar configuración HTTPS:
   ```bash
   docker exec -it nifi_centrosur bash -c "cat /opt/nifi/nifi-current/conf/nifi.properties | grep https"
   ```

2. Verificar certificados:
   ```bash
   docker exec -it nifi_centrosur bash -c "ls -la /opt/nifi/nifi-current/conf/certs"
   ```

3. Aceptar certificado autofirmado:
   - Acceder a https://localhost:8443/nifi
   - Haz clic en "Avanzado" o "Más información"
   - Selecciona "Continuar a localhost (no seguro)"

4. Restaurar configuración predeterminada:
   ```bash
   docker-compose down
   docker volume rm nifi_conf
   docker-compose up -d nifi_centrosur
   ```

### Problema: Procesadores en estado inválido

**Síntomas:** Procesadores en NiFi muestran estado "Invalid" y no se pueden iniciar.

**Soluciones:**

1. Verificar configuración del procesador:
   - Haz clic derecho en el procesador > Configurar
   - Asegúrate de que todas las propiedades requeridas estén configuradas
   - Verifica las relaciones (todas deben estar conectadas o auto-terminadas)

2. Verificar servicios de controlador:
   - Menú hamburguesa > Controller Settings
   - Asegúrate de que los servicios necesarios estén habilitados

3. Vaciar colas de conexión:
   - Haz clic derecho en la conexión > Empty queue

4. Eliminar y recrear el procesador:
   - Si todo lo demás falla, copia la configuración, elimina y recrea el procesador

## Problemas con PostgreSQL

### Problema: PostgreSQL no inicia correctamente

**Síntomas:** El contenedor de PostgreSQL falla al iniciar o se reinicia continuamente.

**Soluciones:**

1. Verificar logs:
   ```bash
   docker logs postgres_centrosur
   ```

2. Verificar espacio en disco:
   ```bash
   df -h
   ```
   PostgreSQL necesita espacio suficiente para iniciar.

3. Verificar permisos de volúmenes:
   ```bash
   docker-compose down
   docker volume inspect postgres_data
   ```

4. Reiniciar con volúmenes limpios:
   ```bash
   docker-compose down
   docker volume rm postgres_data
   docker-compose up -d postgres_centrosur
   ```

### Problema: Error al restaurar la base de datos

**Síntomas:** Errores durante la restauración de la base de datos o la tabla potencia_dep.

**Soluciones:**

1. Verificar formato del archivo de respaldo:
   ```bash
   file bdcentrosur.sql
   ```
   Asegúrate de que sea un archivo de texto plano SQL.

2. Verificar versión de PostgreSQL:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -c "SELECT version();"
   ```
   Si hay incompatibilidad de versiones, convierte el respaldo:
   ```bash
   # Instala pgAdmin o usa PostgreSQL local
   # Restaura en PostgreSQL local y exporta en formato plano
   pg_dump -U postgres -d centrosur -f bdcentrosur_new.sql
   ```

3. Restaurar manualmente:
   ```bash
   docker cp bdcentrosur.sql postgres_centrosur:/tmp/
   docker exec -it postgres_centrosur bash
   psql -U postgres -d centrosur -f /tmp/bdcentrosur.sql
   ```

4. Verificar errores específicos:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "\d potencia_dep"
   ```

### Problema: No se pueden consultar los datos en las tablas

**Síntomas:** Errores al consultar las tablas o no se encuentran los datos esperados.

**Soluciones:**

1. Verificar existencia de tablas:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "\dt"
   ```

2. Verificar estructura de tablas:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "\d potencia_dep"
   ```

3. Verificar datos:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "SELECT COUNT(*) FROM potencia_dep;"
   ```

4. Verificar permisos de usuario:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO postgres;"
   ```

## Problemas con Kafka

### Problema: Kafka no inicia correctamente

**Síntomas:** El contenedor de Kafka falla al iniciar o no es accesible.

**Soluciones:**

1. Verificar logs:
   ```bash
   docker logs kafka_centrosur
   ```

2. Verificar conectividad con ZooKeeper:
   ```bash
   docker exec -it kafka_centrosur bash -c "ping zookeeper_centrosur"
   ```

3. Reiniciar servicios en orden:
   ```bash
   docker-compose restart zookeeper_centrosur
   # Esperar 30 segundos
   docker-compose restart kafka_centrosur
   ```

4. Verificar configuración de Kafka:
   ```bash
   docker exec -it kafka_centrosur bash -c "cat /etc/kafka/server.properties"
   ```
   Verifica la configuración de listeners y advertised.listeners.

### Problema: No se pueden crear o listar temas

**Síntomas:** Errores al intentar crear, listar o publicar en temas de Kafka.

**Soluciones:**

1. Verificar que Kafka esté en ejecución:
   ```bash
   docker ps | grep kafka_centrosur
   ```

2. Verificar temas existentes:
   ```bash
   docker exec -it kafka_centrosur kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

3. Crear un tema de prueba:
   ```bash
   docker exec -it kafka_centrosur kafka-topics.sh --create --topic test-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
   ```

4. Verificar conectividad desde otros servicios:
   ```bash
   docker exec -it nifi_centrosur ping kafka_centrosur
   docker exec -it airflow_centrosur ping kafka_centrosur
   ```

### Problema: No se pueden consumir mensajes

**Síntomas:** No hay mensajes en Kafka o los consumidores no pueden leer mensajes.

**Soluciones:**

1. Verificar existencia del tema:
   ```bash
   docker exec -it kafka_centrosur kafka-topics.sh --describe --topic sube-alim-data --bootstrap-server localhost:9092
   ```

2. Publicar mensaje de prueba:
   ```bash
   docker exec -it kafka_centrosur bash -c "echo '{\"test\":\"data\"}' | kafka-console-producer.sh --bootstrap-server localhost:9092 --topic sube-alim-data"
   ```

3. Consumir mensaje de prueba:
   ```bash
   docker exec -it kafka_centrosur kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic sube-alim-data --from-beginning --max-messages 5
   ```

4. Verificar grupo de consumidores:
   ```bash
   docker exec -it kafka_centrosur kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
   ```

## Problemas con Airflow

### Problema: Airflow no inicia correctamente

**Síntomas:** El contenedor de Airflow falla al iniciar o la interfaz web no está disponible.

**Soluciones:**

1. Verificar logs:
   ```bash
   docker logs airflow_centrosur
   ```

2. Verificar conexión a PostgreSQL:
   ```bash
   docker exec -it airflow_centrosur python -c "from airflow.hooks.postgres_hook import PostgresHook; hook = PostgresHook(postgres_conn_id='postgres_centrosur'); print(hook.get_conn())"
   ```

3. Inicializar la base de datos manualmente:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow db init"
   ```

4. Crear usuario administrador manualmente:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com"
   ```

### Problema: DAGs no aparecen en la interfaz web

**Síntomas:** Los DAGs no aparecen en la interfaz web de Airflow o están en estado "paused".

**Soluciones:**

1. Verificar ruta de DAGs:
   ```bash
   docker exec -it airflow_centrosur bash -c "ls -la /opt/airflow/dags"
   ```

2. Verificar errores de sintaxis en DAGs:
   ```bash
   docker exec -it airflow_centrosur bash -c "python /opt/airflow/dags/nifi_kafka_dag.py"
   ```

3. Reiniciar el scheduler:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow scheduler -D"
   ```

4. Verificar permisos de archivos:
   ```bash
   docker exec -it airflow_centrosur bash -c "chmod 644 /opt/airflow/dags/*.py"
   ```

### Problema: Errores en las conexiones de Airflow

**Síntomas:** Los DAGs fallan debido a problemas de conexión con otros servicios.

**Soluciones:**

1. Verificar conexiones configuradas:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow connections list"
   ```

2. Crear o actualizar conexiones manualmente:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow connections add postgres_centrosur --conn-type postgres --conn-host postgres_centrosur --conn-schema centrosur --conn-login postgres --conn-password 1150040812 --conn-port 5432"
   ```

3. Ejecutar script setup_connections.py:
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/setup_connections.py
   ```

4. Verificar conectividad desde Airflow:
   ```bash
   docker exec -it airflow_centrosur bash -c "ping postgres_centrosur"
   docker exec -it airflow_centrosur bash -c "ping kafka_centrosur"
   docker exec -it airflow_centrosur bash -c "ping nifi_centrosur"
   ```

## Problemas con NiFi Proxy

### Problema: NiFi Proxy no inicia correctamente

**Síntomas:** El contenedor del proxy no inicia o no responde a peticiones.

**Soluciones:**

1. Verificar logs:
   ```bash
   docker logs nifi_proxy_centrosur
   docker logs mini_nifi_proxy_centrosur
   ```

2. Verificar conectividad con NiFi:
   ```bash
   docker exec -it nifi_proxy_centrosur bash -c "curl -k -v https://nifi_centrosur:8443"
   ```

3. Reiniciar el proxy:
   ```bash
   docker-compose restart nifi_proxy_centrosur
   docker-compose restart mini_nifi_proxy_centrosur
   ```

4. Verificar variables de entorno:
   ```bash
   docker exec -it nifi_proxy_centrosur bash -c "env | grep NIFI"
   ```

### Problema: Errores de autenticación en el proxy

**Síntomas:** El proxy no puede autenticarse con NiFi o devuelve errores 401/403.

**Soluciones:**

1. Verificar credenciales:
   ```bash
   docker exec -it nifi_proxy_centrosur bash -c "env | grep USERNAME"
   docker exec -it nifi_proxy_centrosur bash -c "env | grep PASSWORD"
   ```

2. Probar autenticación manualmente:
   ```bash
   docker exec -it nifi_proxy_centrosur bash -c "curl -k -v -X POST -d 'username=admin&password=centrosur123' https://nifi_centrosur:8443/nifi-api/access/token"
   ```

3. Ejecutar script de diagnóstico:
   ```bash
   docker exec -it nifi_proxy_centrosur python /app/auth_test.py
   ```

4. Verificar el endpoint de salud del proxy:
   ```bash
   docker exec -it airflow_centrosur curl http://nifi_proxy_centrosur:5000/health
   docker exec -it airflow_centrosur curl http://mini_nifi_proxy_centrosur:5001/health
   ```

### Problema: Error al iniciar o detener procesadores a través del proxy

**Síntomas:** No se pueden iniciar o detener procesadores a través del proxy.

**Soluciones:**

1. Verificar endpoint de procesadores:
   ```bash
   docker exec -it airflow_centrosur curl http://mini_nifi_proxy_centrosur:5001/api/processors/PROCESSOR_ID
   ```

2. Verificar permisos en NiFi:
   - Asegúrate de que el usuario utilizado tiene permisos de lectura/escritura en NiFi

3. Usar IDs de procesadores correctos:
   - Ejecutar script de diagnóstico para obtener IDs actualizados:
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/nifi_diagnostic.py
   ```

4. Reiniciar el proxy y NiFi:
   ```bash
   docker-compose restart nifi_centrosur
   docker-compose restart mini_nifi_proxy_centrosur
   ```

## Problemas de Comunicación entre Servicios

### Problema: Los servicios no pueden comunicarse entre sí

**Síntomas:** Errores de conexión entre servicios, como NiFi no puede conectar a PostgreSQL o Kafka.

**Soluciones:**

1. Verificar que todos los servicios estén en la misma red:
   ```bash
   docker network inspect centrosur_network
   ```

2. Verificar resolución de nombres:
   ```bash
   docker exec -it nifi_centrosur bash -c "ping -c 3 postgres_centrosur"
   docker exec -it airflow_centrosur bash -c "ping -c 3 kafka_centrosur"
   ```

3. Verificar accesibilidad de puertos:
   ```bash
   docker exec -it nifi_centrosur bash -c "nc -zv postgres_centrosur 5432"
   docker exec -it airflow_centrosur bash -c "nc -zv kafka_centrosur 9092"
   ```

4. Verificar reglas de firewall:
   ```bash
   docker exec -it nifi_centrosur bash -c "iptables -L"
   ```

### Problema: Errores de conexión a la base de datos

**Síntomas:** Los servicios no pueden conectarse a PostgreSQL o reciben errores de autenticación.

**Soluciones:**

1. Verificar configuración de PostgreSQL:
   ```bash
   docker exec -it postgres_centrosur bash -c "cat /var/lib/postgresql/data/pg_hba.conf"
   ```

2. Verificar que PostgreSQL esté escuchando en la IP correcta:
   ```bash
   docker exec -it postgres_centrosur bash -c "cat /var/lib/postgresql/data/postgresql.conf | grep listen_addresses"
   ```

3. Verificar credenciales:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -c "SELECT usename, passwd FROM pg_shadow;"
   ```

4. Reiniciar la base de datos:
   ```bash
   docker-compose restart postgres_centrosur
   ```

### Problema: Problemas con el almacenamiento de datos

**Síntomas:** Errores al insertar o leer datos en las tablas destino.

**Soluciones:**

1. Verificar estructura de la tabla destino:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "\d potencia_dep_processed"
   ```

2. Verificar permisos:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "GRANT ALL ON SCHEMA public TO postgres;"
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "GRANT ALL ON ALL TABLES IN SCHEMA public TO postgres;"
   ```

3. Crear la tabla si no existe:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "
   CREATE TABLE IF NOT EXISTS potencia_dep_processed (
       id SERIAL PRIMARY KEY,
       fecha DATE,
       hora TIME,
       alimentador VARCHAR(10),
       dia_semana INTEGER,
       potencia_activa NUMERIC(10,3),
       potencia_reactiva NUMERIC(10,3),
       procesado_en TIMESTAMP DEFAULT CURRENT_TIMESTAMP
   );"
   ```

4. Verificar espacio disponible:
   ```bash
   docker exec -it postgres_centrosur bash -c "df -h"
   ```

## Problemas con DAGs de Airflow

### Problema: DAGs fallan al ejecutarse

**Síntomas:** Los DAGs inician pero fallan en alguna tarea específica.

**Soluciones:**

1. Verificar logs de tareas:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow tasks test [dag_id] [task_id] [execution_date]"
   ```

2. Verificar dependencias:
   ```bash
   docker exec -it airflow_centrosur pip list | grep kafka
   docker exec -it airflow_centrosur pip list | grep postgres
   ```

3. Instalar dependencias faltantes:
   ```bash
   docker exec -it airflow_centrosur pip install kafka-python psycopg2-binary
   ```

4. Verificar variables y conexiones en Airflow:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow variables list"
   docker exec -it airflow_centrosur bash -c "airflow connections list"
   ```

### Problema: DAG para tabla sube_alim no procesa datos

**Síntomas:** El DAG se ejecuta pero no se ven datos en la tabla destino.

**Soluciones:**

1. Verificar IDs de procesadores:
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/nifi_diagnostic.py
   ```

2. Actualizar IDs en el código del DAG:
   ```bash
   docker exec -it airflow_centrosur sed -i 's/SPLIT_JSON_ID = "old_id"/SPLIT_JSON_ID = "new_id"/' /opt/airflow/dags/tabla_sube_alim_dag.py
   ```

3. Verificar temas Kafka:
   ```bash
   docker exec -it kafka_centrosur kafka-topics.sh --list --bootstrap-server localhost:9092
   ```

4. Reparar procesadores problemáticos:
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/repair_processors.py
   ```

### Problema: DAG para tabla potencia_dep no procesa datos

**Síntomas:** El DAG para potencia_dep se ejecuta pero no hay datos en la tabla destino.

**Soluciones:**

1. Verificar datos en la tabla origen:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "SELECT COUNT(*) FROM potencia_dep WHERE fecha >= '2019-01-01' AND fecha < '2019-02-01';"
   ```

2. Verificar procesadores en NiFi:
   - Asegúrate de que QueryDatabaseTable esté configurado con la SQL correcta
   - Verifica que ConsumeKafka use el mismo tema que PublishKafka

3. Ejecutar script de reparación completa:
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/repair_nifi_flow.py
   ```

4. Limpiar el estado y vaciar las colas:
   - Usa la tarea clear_processor_state_and_empty_all_queues() en el DAG

### Problema: Errores con SplitJson o EvaluateJsonPath

**Síntomas:** Errores específicos relacionados con los procesadores SplitJson o EvaluateJsonPath.

**Soluciones:**

1. Reparar SplitJson:
   ```bash
   docker exec -it airflow_centrosur python -c "from tabla_sube_alim_dag import repair_processor; repair_processor('SplitJson', 'org.apache.nifi.processors.standard.SplitJson', '52f64be2-88ea-3462-1f4e-ac288a33b828', 24, 16, {'JsonPath Expression': '$.*', 'Null Value Representation': 'empty string', 'Max String Length': '20 MB'})"
   ```

2. Reparar EvaluateJsonPath:
   ```bash
   docker exec -it airflow_centrosur python -c "from tabla_sube_alim_dag import repair_processor; repair_processor('EvaluateJsonPath', 'org.apache.nifi.processors.standard.EvaluateJsonPath', '52f64be2-88ea-3462-1f4e-ac288a33b828', 48, 272, {'Destination': 'flowfile-attribute', 'id_subestacion': '$.id_subestacion', 'Max String Length': '20 MB', 'Return Type': 'auto-detect', 'Null Value Representation': 'empty string', 'alimentador': '$.alimentador', 'Path Not Found Behavior': 'warn'})"
   ```

3. Corregir campo "alimentador" en lugar de "alimetador":
   ```bash
   docker exec -it airflow_centrosur python /opt/airflow/scripts/fix_processors.py
   ```

## Problemas de Rendimiento

### Problema: Procesamiento lento en NiFi

**Síntomas:** El flujo de datos en NiFi es muy lento o se atasca.

**Soluciones:**

1. Aumentar la concurrencia de los procesadores:
   - Configurar > Scheduling > Concurrent tasks
   - Aumentar a un valor mayor (5-10 para la mayoría de procesadores)

2. Aumentar el tamaño de lote para PublishKafka y PutDatabaseRecord:
   - Configurar > Properties > Batch Size
   - Aumentar a 100-1000 según el caso

3. Optimizar las conexiones de base de datos:
   - Aumentar el pool de conexiones en DBCPConnectionPool
   - Establecer un fetch size adecuado en QueryDatabaseTable

4. Verificar los recursos del sistema:
   ```bash
   docker stats nifi_centrosur postgres_centrosur kafka_centrosur
   ```

### Problema: Kafka se sobrecarga con muchos mensajes

**Síntomas:** Kafka consume demasiada memoria o CPU, o pierde mensajes.

**Soluciones:**

1. Aumentar recursos para Kafka:
   ```yaml
   kafka_centrosur:
     deploy:
       resources:
         limits:
           memory: 4G
         reservations:
           memory: 2G
   ```

2. Optimizar la configuración de Kafka:
   ```bash
   docker exec -it kafka_centrosur bash -c "echo 'num.partitions=3' >> /etc/kafka/server.properties"
   docker exec -it kafka_centrosur bash -c "echo 'log.retention.hours=24' >> /etc/kafka/server.properties"
   docker-compose restart kafka_centrosur
   ```

3. Particionar los temas:
   ```bash
   docker exec -it kafka_centrosur kafka-topics.sh --alter --topic potencia-data --partitions 3 --bootstrap-server localhost:9092
   ```

4. Configurar temas con retención por tiempo:
   ```bash
   docker exec -it kafka_centrosur kafka-configs.sh --alter --add-config "retention.ms=86400000" --bootstrap-server localhost:9092 --topic potencia-data
   ```

### Problema: Base de datos lenta al procesar grandes volúmenes

**Síntomas:** Inserciones lentas o consultas lentas en PostgreSQL.

**Soluciones:**

1. Optimizar la configuración de PostgreSQL:
   ```bash
   docker exec -it postgres_centrosur bash -c "echo 'shared_buffers = 512MB' >> /var/lib/postgresql/data/postgresql.conf"
   docker exec -it postgres_centrosur bash -c "echo 'work_mem = 16MB' >> /var/lib/postgresql/data/postgresql.conf"
   docker exec -it postgres_centrosur bash -c "echo 'maintenance_work_mem = 128MB' >> /var/lib/postgresql/data/postgresql.conf"
   docker exec -it postgres_centrosur bash -c "echo 'effective_cache_size = 1GB' >> /var/lib/postgresql/data/postgresql.conf"
   docker-compose restart postgres_centrosur
   ```

2. Añadir índices a las tablas:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "CREATE INDEX IF NOT EXISTS idx_fecha_hora_alimentador ON potencia_dep_processed(fecha, hora, alimentador);"
   ```

3. Optimizar inserciones por lotes:
   - Configurar PutDatabaseRecord para usar batch size mayor
   - Usar transacciones para insertar múltiples registros

4. Monitorear rendimiento de PostgreSQL:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -c "SELECT * FROM pg_stat_activity WHERE state = 'active';"
   docker exec -it postgres_centrosur psql -U postgres -c "SELECT relname, seq_scan, seq_tup_read, idx_scan, idx_tup_fetch FROM pg_stat_user_tables ORDER BY seq_scan DESC;"
   docker exec -it postgres_centrosur psql -U postgres -c "SELECT pg_size_pretty(pg_database_size('centrosur'));"
   ```

## Problemas con DAGs de Airflow para procesamiento anual

### Problema: El DAG de procesamiento anual se detiene o falla

**Síntomas:** El DAG potencia_integrador_anual falla o no procesa todos los meses.

**Soluciones:**

1. Verificar el estado de cada mes:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d base_prueba -c "SELECT EXTRACT(MONTH FROM fecha) as mes, COUNT(*) FROM potencia_dep_processed WHERE EXTRACT(YEAR FROM fecha) = 2019 GROUP BY mes ORDER BY mes;"
   ```

2. Desbloquear tareas en Airflow:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow dags backfill -s $(date -d '-1 day' +'%Y-%m-%d') -e $(date +'%Y-%m-%d') potencia_integrador_anual --reset_dagruns"
   ```

3. Verificar logs específicos de un mes:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow tasks logs potencia_integrador_anual procesar_enero.camino_procesamiento.procesar_mes_1 $(date -d '-1 day' +'%Y-%m-%d')"
   ```

4. Ejecutar un mes específico manualmente:
   ```bash
   docker exec -it airflow_centrosur python -c "from potencia_integrador_anual import procesar_mes_sustituyendo_tabla; procesar_mes_sustituyendo_tabla(5)"
   ```

### Problema: Errores al restaurar tabla original

**Síntomas:** Falla la tarea de restaurar la tabla original después del procesamiento.

**Soluciones:**

1. Verificar existencia de la tabla de respaldo:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'potencia_dep_original');"
   ```

2. Restaurar manualmente:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "DROP TABLE IF EXISTS potencia_dep; ALTER TABLE potencia_dep_original RENAME TO potencia_dep;"
   ```

3. Recrear la tabla desde cero si no hay respaldo:
   ```bash
   docker exec -it postgres_centrosur psql -U postgres -d centrosur -c "CREATE TABLE potencia_dep AS SELECT * FROM potencia_dep_processed;"
   ```

4. Ejecutar función de restauración manualmente:
   ```bash
   docker exec -it airflow_centrosur python -c "from potencia_integrador_anual import restaurar_tabla_original_v2; restaurar_tabla_original_v2()"
   ```

### Problema: Conflictos al procesar varios meses simultáneamente

**Síntomas:** Errores al ejecutar múltiples instancias del DAG de integración simultáneamente.

**Soluciones:**

1. Configurar concurrency adecuada en Airflow:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow config get-value core dag_concurrency"
   docker exec -it airflow_centrosur bash -c "airflow config set core dag_concurrency 1"
   ```

2. Usar topic de Kafka único para cada mes:
   ```bash
   docker exec -it airflow_centrosur python -c "from potencia_integrador_anual import configurar_topic_procesador; configurar_topic_procesador('bd27ce5a-5b0d-3ab6-b0bc-dfffb23b7671', 'Topic Name', 'potencia-mes5-unico-' + str(int(time.time())))"
   ```

3. Forzar ejecución secuencial de meses:
   - Modificar el DAG para procesar un mes a la vez con dependencias estrictas

4. Pausar todos los meses y ejecutar manualmente:
   ```bash
   docker exec -it airflow_centrosur bash -c "airflow dags pause potencia_integrador_anual"
   # Ejecutar los meses uno a uno manualmente
   ```

## Cómo Solicitar Ayuda

Si has intentado todas las soluciones anteriores y todavía tienes problemas, sigue estos pasos:

1. Recopila todos los logs relevantes de los contenedores afectados
2. Documenta los pasos exactos que has seguido y los errores encontrados
3. Contacta al equipo de soporte técnico con esta información
