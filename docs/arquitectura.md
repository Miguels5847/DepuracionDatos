# Arquitectura del Sistema de Depuración de Datos

Este documento describe la arquitectura técnica del sistema de depuración de datos implementado para la Empresa Eléctrica Regional Centro Sur C.A., detallando los componentes, flujos de datos y decisiones de diseño.

## Visión General

El sistema implementa una arquitectura basada en eventos con orquestación de tareas, diseñada específicamente para:

1. Identificar y corregir datos cuando se presentan cortes de luz o trabajos realizados dentro de la empresa
2. Procesar y depurar curvas de potencia eléctrica
3. Manejar datos provenientes de múltiples sistemas y formatos

## Diagrama de Arquitectura

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│                 │     │                 │     │                 │
│  Fuentes de     │────►│    Ingesta      │────►│  Procesamiento  │
│    Datos        │     │    (NiFi)       │     │   (NiFi/Kafka)  │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └────────┬────────┘
                                                         │
┌─────────────────┐     ┌─────────────────┐     ┌────────▼────────┐
│                 │     │                 │     │                 │
│  Visualización  │◄────┤  Análisis       │◄────┤ Almacenamiento  │
│    (Futuro)     │     │  (Airflow)      │     │  (PostgreSQL)   │
│                 │     │                 │     │                 │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

## Componentes Principales

### 1. Fuentes de Datos

**Sistemas Origen:**
- IBM/DB2
- CIS/CRM SAP
- ADMS
- GLPI
- GIS

**Formatos de Archivos:**
- Excel
- CSV
- SQL

La arquitectura está diseñada para ser extensible, permitiendo la incorporación de nuevas fuentes de datos según sea necesario.

### 2. Ingesta de Datos (Apache NiFi)

Apache NiFi gestiona el proceso de ingesta, con las siguientes características:

- **Conectores específicos** para cada fuente de datos
- **Validación inicial** de la estructura y formato de los datos
- **Transformación ligera** para estandarización
- **Buffers de mensajes** para gestionar picos de carga
- **Manejo de errores** con reintentos automáticos

Los flujos de NiFi implementados incluyen:
- Procesamiento de tabla `potencia_dep`
- Procesamiento de tabla `sube_alim`
- Procesamiento de tabla `tension_dep`

### 3. Procesamiento de Datos (Apache NiFi y Apache Kafka)

El procesamiento se distribuye entre NiFi y Kafka:

**En NiFi:**
- Transformación de formatos (Avro, JSON, CSV)
- Enriquecimiento de datos
- Aplicación de reglas de negocio
- Detección de anomalías (valores cero o anómalos)

**En Kafka:**
- Cola de mensajes para desacoplar ingesta y almacenamiento
- Buffer para picos de carga
- Persistencia temporal de eventos
- Soporte para procesamiento paralelo

### 4. Almacenamiento (PostgreSQL)

PostgreSQL se utiliza como sistema de almacenamiento principal con las siguientes características:

- **Modelo relacional** optimizado para consultas analíticas
- **Versionado de datos** (originales vs. corregidos)
- **Esquema optimizado** para series temporales
- **Tablas principales:**
  - `potencia`: Curvas de potencia originales
  - `potencia_dep`: Curvas de potencia depuradas
  - `sube_alim`: Relación entre subestaciones y alimentadores
  - `subestacion`: Información de subestaciones
  - `tension`: Datos de tensión originales
  - `tension_dep`: Datos de tensión depurados

### 5. Análisis y Orquestación (Apache Airflow)

Apache Airflow orquesta todo el proceso con:

- **DAGs** (Directed Acyclic Graphs) para flujos complejos
- **Programación temporal** de tareas recurrentes (mensual)
- **Monitoreo y alertas** para detección de errores
- **Reintentos automáticos** para tareas fallidas
- **Paralelización** de tareas independientes

### 6. Componentes Auxiliares

- **NiFi Proxy**: Facilita la comunicación entre Airflow y NiFi
- **Mini NiFi Proxy**: Versión ligera optimizada para operaciones específicas
- **Kafka UI**: Interfaz de usuario para monitoreo de temas Kafka
- **PgAdmin**: Interfaz para administración de PostgreSQL

## Flujos de Datos Principales

### 1. Depuración de Curvas de Potencia

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│ QueryDataba-│     │ ConvertReco-│     │ PublishKafka│     │ConsumeKafka │
│ seTable     ├────►│ rd          ├────►│             ├────►│             │
└─────────────┘     └─────────────┘     └─────────────┘     └──────┬──────┘
                                                                   │
┌─────────────┐     ┌─────────────┐     ┌─────────────┐     ┌──────▼──────┐
│PutDatabase- │     │ReplaceText  │     │EvaluateJson-│     │SplitJson    │
│Record       │◄────┤             │◄────┤Path         │◄────┤             │
└─────────────┘     └─────────────┘     └─────────────┘     └─────────────┘
```

1. **Extracción**: Se extraen datos de potencia de la base de datos
2. **Transformación**: Se convierten a formato JSON
3. **Carga en Kafka**: Los datos se envían a un tema de Kafka
4. **Consumo desde Kafka**: Airflow orquesta el consumo de mensajes
5. **Procesamiento**: Se aplican reglas de negocio para identificar y corregir anomalías
6. **Carga final**: Los datos depurados se insertan en la tabla correspondiente

### 2. Procesamiento de Subestaciones y Alimentadores

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│QueryDatabase│     │ExecuteScript│     │ConvertRecord│
│Table        ├────►│             ├────►│             │
└─────────────┘     └─────────────┘     └──────┬──────┘
                                               │
┌─────────────┐     ┌─────────────┐     ┌──────▼──────┐
│PutDatabase- │     │EvaluateJson-│     │PublishKafka │
│Record       │◄────┤Path         │◄────┤             │
└─────────────┘     └─────────────┘     └─────────────┘
```

Este flujo maneja la relación entre subestaciones y alimentadores, asegurando la consistencia de los datos.

## Mecanismos de Depuración

### Detección de Anomalías

El sistema implementa varios métodos para identificar valores anómalos:

1. **Detección de valores cero**: Identifica cuándo hay valores cero que podrían ser causados por cortes de energía
2. **Comparación con históricos**: Compara con valores del mes anterior a la misma hora
3. **Análisis de patrones**: Identifica patrones atípicos en series temporales

### Corrección de Datos

Una vez detectadas las anomalías, se aplican estas estrategias de corrección:

1. **Sustitución con valores históricos**: Reemplaza valores anómalos con datos del mes anterior (misma hora y día de la semana)
2. **Generación de curvas tipo**: Crea curvas de referencia para cada alimentador
3. **Interpolación**: Calcula valores intermedios cuando es apropiado

## Consideraciones de Escalabilidad

La arquitectura está diseñada para escalar horizontalmente:

- **NiFi**: Permite agregar nodos adicionales en un clúster
- **Kafka**: Soporte para particiones múltiples y replicación
- **PostgreSQL**: Optimizado con índices apropiados para grandes volúmenes
- **Airflow**: Permite escalar ejecutores según necesidad

## Seguridad

La implementación actual incluye:

- **Autenticación básica** para todos los componentes
- **HTTPS** para NiFi con certificados SSL
- **Control de acceso** basado en roles
- **Aislamiento de red** mediante Docker Compose

## Monitoreo y Logging

Cada componente genera logs detallados:

- **NiFi**: Registra cada operación y transferencia de datos
- **Kafka**: Monitoreo mediante Kafka UI
- **Airflow**: Dashboard para supervisión de tareas
- **PostgreSQL**: Logs de consultas y operaciones

## Futuras Mejoras

La arquitectura está preparada para estas mejoras futuras:

1. **Integración con herramientas de BI** para visualización avanzada
2. **Implementación de machine learning** para predicción de demanda
3. **Alta disponibilidad** mediante clustering de componentes
4. **Expansión a nuevas fuentes de datos**
5. **Mejoras en la seguridad** con autenticación avanzada y cifrado

## Consideraciones para Producción

Para desplegar en un entorno de producción, se recomienda:

1. **Clusterización** de componentes críticos
2. **Respaldos automáticos** de la base de datos
3. **Monitoreo proactivo** con alertas
4. **Gestión de secretos** más robusta
5. **Documentación adicional** para operadores del sistema