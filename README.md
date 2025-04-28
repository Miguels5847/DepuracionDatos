# DepuracionDatos
# Arquitectura de Datos - Centro Sur

Este repositorio contiene la implementación de una arquitectura de datos para la Empresa Eléctrica Regional Centro Sur C.A., diseñada para la identificación y corrección de datos cuando se presentan cortes de luz o trabajos realizados dentro de la empresa.

## 📋 Descripción General

El sistema implementa una arquitectura basada en eventos con Apache Airflow como orquestador, que permite:

- Gestionar flujos de datos asincrónicos desde múltiples fuentes
- Facilitar la detección y reacción a eventos como cortes de energía
- Programar tareas recurrentes para mantenimiento de datos
- Manejar errores y reintentos automáticos

## 🏗️ Arquitectura

La solución se basa en los siguientes componentes principales:

- **Apache NiFi**: Para la ingesta y transformación de datos desde múltiples fuentes
- **Apache Kafka**: Como sistema de mensajería para procesamiento de eventos
- **PostgreSQL**: Como sistema de almacenamiento principal de datos
- **Apache Airflow**: Como orquestador para la programación y monitoreo de tareas

## 🚀 Requisitos Previos

### Para entorno Linux (Ubuntu Server)

- Docker Engine (versión 20.10.0 o superior)
- Docker Compose (versión 2.0.0 o superior)
- Git
- Mínimo 8GB de RAM
- Al menos 50GB de espacio en disco

### Para entorno Windows

- Docker Desktop (con WSL2 habilitado)
- Git para Windows
- Mínimo 16GB de RAM
- Al menos 50GB de espacio en disco

## 🛠️ Instalación

### Pasos para Ubuntu Server

1. Clonar este repositorio:
   ```bash
   git clone https://github.com/tu-usuario/centrosur-data-architecture.git
   cd centrosur-data-architecture
   ```

2. Crear archivo .env para variables de entorno:
   ```bash
   cp .env.example .env
   # Editar .env con tus credenciales
   ```

3. Iniciar los servicios:
   ```bash
   docker-compose up -d
   ```

4. Restaurar la base de datos:
   ```bash
   # La restauración se ejecutará automáticamente durante el primer inicio
   # Si necesitas forzar la restauración:
   docker exec -it postgres_centrosur bash -c "psql -U postgres -d centrosur -f /docker-entrypoint-initdb.d/bdcentrosur.sql"
   ```

### Pasos para Windows

1. Clonar este repositorio:
   ```powershell
   git clone https://github.com/tu-usuario/centrosur-data-architecture.git
   cd centrosur-data-architecture
   ```

2. Crear archivo .env para variables de entorno:
   ```powershell
   copy .env.example .env
   # Editar .env con tus credenciales usando Notepad o VSCode
   ```

3. Iniciar los servicios:
   ```powershell
   docker-compose up -d
   ```

4. Monitorear el progreso:
   ```powershell
   docker-compose logs -f
   ```

## 🔐 Acceso a las interfaces

Una vez que todos los servicios estén en funcionamiento, podrás acceder a ellos a través de:

| Servicio | URL | Credenciales |
|----------|-----|--------------|
| PostgreSQL | localhost:5432 | postgres / 1150040812 |
| PgAdmin | http://localhost:5050 | admin@centrosur.com / 1150040812 |
| NiFi | https://localhost:8443/nifi | admin / centrosur123 |
| Airflow | http://localhost:8081 | admin / admin |
| Kafka UI | http://localhost:8080 | *(No requiere autenticación)* |

**Nota:** Al acceder a NiFi por primera vez, es posible que recibas advertencias de seguridad del navegador debido al certificado SSL autofirmado. Puedes proceder de manera segura aceptando el riesgo.

## 📊 Estructura de Datos

El sistema maneja las siguientes tablas principales:

- **potencia**: Almacena curvas de potencia originales
- **potencia_dep**: Almacena curvas de potencia depuradas
- **sube_alim**: Relaciona subestaciones con alimentadores
- **subestacion**: Contiene información sobre las subestaciones
- **tension**: Almacena datos de tensión originales
- **tension_dep**: Almacena datos de tensión depurados

## 🔄 Flujos de Integración

Los principales flujos implementados incluyen:

1. **Procesamiento de curvas de potencia**: Extrae, transforma y carga datos de potencia eléctrica
2. **Análisis de subestaciones**: Integra datos de subestaciones con sus alimentadores
3. **Procesamiento de datos de tensión**: Gestiona los datos de tensión por subestación

## 📆 Configuración de tareas programadas

El sistema viene con los siguientes DAGs preconfigurados en Airflow:

- **potencia_integrador_anual**: Procesa datos mensuales de potencia para todo un año
- **sube_alim_dag**: Procesa datos de subestaciones y alimentadores
- **tension_integrador_anual**: Procesa datos de tensión anualmente

## 🐛 Solución de problemas

### Problema: NiFi no inicia correctamente
**Solución**: Verifica los logs con `docker logs nifi_centrosur`. Si hay problemas de permisos, ejecuta:
```bash
docker-compose down
docker volume rm nifi_conf nifi_state
docker-compose up -d
```

### Problema: Base de datos no se restaura correctamente
**Solución**: Intenta restaurar manualmente:
```bash
docker exec -it postgres_centrosur bash
psql -U postgres -d centrosur -f /docker-entrypoint-initdb.d/bdcentrosur.sql
```

### Problema: Mini Proxy de NiFi no se conecta
**Solución**: Reinicia el contenedor:
```bash
docker-compose restart mini-nifi-proxy
```

## 📁 Estructura del Repositorio

```
centrosur-data-architecture/
├── docker/                         # Archivos Docker
│   ├── airflow/                    # Configuración de Airflow
│   │   └── dags/                   # DAGs de Airflow
│   ├── nifi/                       # Configuración de NiFi
│   │   └── drivers/                # Drivers JDBC
│   └── nifi-proxy/                 # Proxy para NiFi
├── scripts/                        # Scripts generales
├── flows/                          # Flujos exportados de NiFi
├── configs/                        # Archivos de configuración
└── docs/                           # Documentación
```

## 📚 Documentación

Para información más detallada sobre los componentes del sistema, consulta:

- [Guía de Apache NiFi](docs/nifi_guide.md)
- [Guía de Apache Kafka](docs/kafka_guide.md)
- [Guía de Apache Airflow](docs/airflow_guide.md)
- [Estructuras de Base de Datos](docs/database_structure.md)
- [Diagramas de Arquitectura](docs/architecture_diagrams.md)

## 📄 Licencia

Este proyecto es propiedad de Empresa Eléctrica Regional Centro Sur C.A. Su uso está restringido a personal autorizado.

---

Desarrollado como parte del proyecto de Arquitectura de Datos para el Departamento de Tecnología de la Información y Comunicación (DITIC).