# Guía de Instalación - Centro Sur

Esta guía proporciona instrucciones detalladas para instalar y configurar la arquitectura de datos de la Empresa Eléctrica Regional Centro Sur C.A. tanto en entornos Linux como Windows.

## Tabla de Contenidos

1. [Requisitos Previos](#requisitos-previos)
2. [Instalación en Ubuntu Server](#instalación-en-ubuntu-server)
3. [Instalación en Windows](#instalación-en-windows)
4. [Verificación de la Instalación](#verificación-de-la-instalación)
5. [Restauración de la Base de Datos](#restauración-de-la-base-de-datos)
6. [Configuración Post-Instalación](#configuración-post-instalación)
7. [Solución de Problemas](#solución-de-problemas)

## Requisitos Previos

### Para Ubuntu Server

- Ubuntu Server 20.04 LTS o superior
- Docker Engine 20.10.0 o superior
- Docker Compose 2.0.0 o superior
- Git
- 8GB RAM mínimo (recomendado 16GB)
- 50GB espacio en disco mínimo
- Acceso a puertos 5432, 5050, 8080, 8081, 8443, 9092, 5000, 5001

### Para Windows

- Windows 10/11 Professional o Enterprise (64-bit)
- WSL2 habilitado
- Docker Desktop con integración WSL2
- Git para Windows
- 16GB RAM mínimo (recomendado 32GB)
- 50GB espacio en disco mínimo (SSD recomendado)
- Acceso a puertos 5432, 5050, 8080, 8081, 8443, 9092, 5000, 5001

## Instalación en Ubuntu Server

### 1. Preparación del Entorno

Actualiza el sistema e instala los paquetes requeridos:

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install git curl apt-transport-https ca-certificates software-properties-common -y
```

### 2. Instalar Docker

```bash
# Agregar repositorio oficial de Docker
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable"

# Instalar Docker
sudo apt update
sudo apt install docker-ce docker-ce-cli containerd.io -y

# Agregar usuario actual al grupo docker
sudo usermod -aG docker $USER
newgrp docker

# Verificar la instalación
docker --version
```

### 3. Instalar Docker Compose

```bash
# Instalar Docker Compose
sudo curl -L "https://github.com/docker/compose/releases/download/v2.5.0/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose

# Verificar la instalación
docker-compose --version
```

### 4. Clonar el Repositorio

```bash
git clone https://github.com/Miguels5847/DepuracionDatos.git
cd DepuracionDatos
```

### 5. Configurar Variables de Entorno

```bash
cp .env.example .env
# Editar el archivo .env si es necesario
nano .env
```

### 6. Iniciar los Servicios

```bash
docker-compose up -d
```

Este comando descargará todas las imágenes necesarias e iniciará todos los servicios. Este proceso puede tardar varios minutos la primera vez.

### 7. Verificar que los Servicios estén Funcionando

```bash
docker-compose ps
```

Todos los servicios deberían mostrar el estado "Up".

## Instalación en Windows

### 1. Preparación del Entorno

1. **Instalar WSL2 (si no está instalado)**:
   - Abrir PowerShell como administrador
   - Ejecutar: `wsl --install`
   - Reiniciar la computadora

2. **Instalar Docker Desktop**:
   - Descargar e instalar desde [https://www.docker.com/products/docker-desktop](https://www.docker.com/products/docker-desktop)
   - Durante la instalación, asegurarse de marcar la opción "Use WSL 2 instead of Hyper-V"
   - Iniciar Docker Desktop después de la instalación
   - Verificar en la configuración que la integración con WSL2 esté activada

3. **Instalar Git para Windows**:
   - Descargar e instalar desde [https://git-scm.com/download/win](https://git-scm.com/download/win)
   - Usar las opciones predeterminadas durante la instalación

### 2. Clonar el Repositorio

Abre una terminal de PowerShell o cmd:

```powershell
# Navegar a la ubicación donde deseas clonar el repositorio
cd C:\Proyectos\  # o la ubicación que prefieras

# Clonar el repositorio
git clone https://github.com/Miguels5847/DepuracionDatos.git
cd DepuracionDatos
```

### 3. Configurar Variables de Entorno

```powershell
# Copiar el archivo de ejemplo
copy .env.example .env

# Editar el archivo .env con Notepad
notepad .env
```

### 4. Iniciar los Servicios

```powershell
docker-compose up -d
```

Este comando descargará todas las imágenes necesarias e iniciará todos los servicios. Este proceso puede tardar varios minutos la primera vez.

### 5. Verificar que los Servicios estén Funcionando

```powershell
docker-compose ps
```

Todos los servicios deberían mostrar el estado "Up".

## Verificación de la Instalación

Una vez que los servicios estén en ejecución, verifica el acceso a las diferentes interfaces:

1. **PostgreSQL**: Puerto 5432 (accesible mediante pgAdmin o herramientas de cliente SQL)
2. **PgAdmin**: http://localhost:5050 (admin@centrosur.com / 1150040812)
3. **NiFi**: https://localhost:8443/nifi (admin / centrosur123)
4. **Airflow**: http://localhost:8081 (admin / admin)
5. **Kafka UI**: http://localhost:8080 (no requiere autenticación)

### Nota sobre los certificados SSL

Al acceder a NiFi por primera vez, recibirás advertencias del navegador debido al certificado SSL autofirmado. Puedes proceder de manera segura aceptando el riesgo.

## Restauración de la Base de Datos

La base de datos debería restaurarse automáticamente durante la inicialización. Si necesitas restaurarla manualmente:

### En Ubuntu Server

```bash
docker exec -it postgres_centrosur bash -c "psql -U postgres -d centrosur -f /docker-entrypoint-initdb.d/bdcentrosur.sql"
```

### En Windows

```powershell
docker exec -it postgres_centrosur bash -c "psql -U postgres -d centrosur -f /docker-entrypoint-initdb.d/bdcentrosur.sql"
```

## Configuración Post-Instalación

### 1. Verificar la Conexión de NiFi con PostgreSQL

1. Accede a NiFi: https://localhost:8443/nifi
2. Verifica que los Servicios del Controlador (como DBCPConnectionPool) estén habilitados
3. Si algún servicio está deshabilitado, habilítalo seleccionándolo y haciendo clic en el icono de rayo

### 2. Importar los Flujos de NiFi

1. En NiFi, haz clic en el icono de "Upload Template" en la barra de herramientas
2. Selecciona uno de los archivos JSON de flujo del directorio `flows/`
3. Una vez cargado, arrastra el icono de "Template" al lienzo
4. Selecciona el template que acabas de subir
5. Repite para los demás flujos

### 3. Verificar Airflow

1. Accede a Airflow: http://localhost:8081
2. Inicia sesión con admin / admin
3. Verifica que los DAGs estén listados y funcionando correctamente
4. Activa los DAGs que desees ejecutar

### 4. Configurar Kafka

La plataforma viene preconfigurada con Kafka, pero si necesitas crear nuevos temas:

1. Accede a Kafka UI: http://localhost:8080
2. Navega a la sección "Topics"
3. Haz clic en "Add topic" y configura según tus necesidades

## Solución de Problemas

### Problema: NiFi no inicia correctamente

**Síntomas**: El contenedor de NiFi se reinicia continuamente o no se puede acceder a la interfaz de usuario.

**Solución**:
1. Verifica los logs de NiFi:
   ```bash
   docker logs nifi_centrosur
   ```
2. Si hay problemas con permisos o archivos de configuración:
   ```bash
   docker-compose down
   docker volume rm nifi_conf nifi_state
   docker-compose up -d
   ```

### Problema: Base de datos no se restaura correctamente

**Síntomas**: No encuentras las tablas esperadas en PostgreSQL.

**Solución**:
1. Intenta restaurar manualmente:
   ```bash
   docker exec -it postgres_centrosur bash
   psql -U postgres -d centrosur -f /docker-entrypoint-initdb.d/bdcentrosur.sql
   ```
2. Verifica que el archivo SQL sea accesible dentro del contenedor:
   ```bash
   docker exec -it postgres_centrosur ls -la /docker-entrypoint-initdb.d/
   ```

### Problema: Mini Proxy de NiFi no se conecta

**Síntomas**: Los DAGs de Airflow fallan al intentar comunicarse con NiFi a través del proxy.

**Solución**:
1. Reinicia el contenedor del proxy:
   ```bash
   docker-compose restart mini-nifi-proxy
   ```
2. Verifica los logs para diagnosticar:
   ```bash
   docker logs mini_nifi_proxy_centrosur
   ```
3. Comprueba la conectividad entre contenedores:
   ```bash
   docker exec -it airflow_centrosur ping nifi_centrosur
   ```

### Problema: Airflow no puede conectarse a PostgreSQL

**Síntomas**: Los DAGs fallan con errores de conexión a la base de datos.

**Solución**:
1. Verifica que la conexión en Airflow esté configurada correctamente:
   ```bash
   docker exec -it airflow_centrosur airflow connections get postgres_centrosur
   ```
2. Si es necesario, crea la conexión manualmente:
   ```bash
   docker exec -it airflow_centrosur airflow connections add 'postgres_centrosur' \
     --conn-type 'postgres' \
     --conn-host 'postgres_centrosur' \
     --conn-schema 'centrosur' \
     --conn-login 'postgres' \
     --conn-password '1150040812' \
     --conn-port '5432'
   ```

### Problema: No se pueden ver temas de Kafka

**Síntomas**: Kafka UI no muestra temas o muestra errores.

**Solución**:
1. Verifica que Kafka y Zookeeper estén funcionando:
   ```bash
   docker-compose ps kafka_centrosur zookeeper_centrosur
   ```
2. Reinicia los servicios de Kafka:
   ```bash
   docker-compose restart zookeeper_centrosur kafka_centrosur kafka_ui_centrosur
   ```
3. Crea un tema de prueba:
   ```bash
   docker exec -it kafka_centrosur kafka-topics --create --topic test-topic --bootstrap-server kafka_centrosur:9092 --partitions 1 --replication-factor 1
   ```

### Problema: Espacio en disco insuficiente

**Síntomas**: Los contenedores fallan al iniciar con errores de espacio en disco.

**Solución**:
1. Libera espacio eliminando imágenes y volúmenes no utilizados:
   ```bash
   docker system prune -a
   ```
2. Limita el tamaño de los logs de los contenedores en docker-compose.yml:
   ```yaml
   services:
     some_service:
       logging:
         options:
           max-size: "10m"
           max-file: "3"
   ```

Si encuentras algún problema que no se menciona aquí, consulta los logs específicos del servicio afectado y busca mensajes de error relevantes para diagnóstico adicional.