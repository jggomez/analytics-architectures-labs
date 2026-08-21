## Guía de Implementación del Laboratorio Práctico

#### Paso 1: Configurar las Fuentes Transaccionales

1. **Cloud SQL (PostgreSQL):** Contiene la tabla transaccional `transacciones_solicitud` (`id`, `cliente_id`, `monto_solicitado`, `estado`, `fecha_creacion`).
2. **Firestore:** Almacena metadatos semiestructurados del cliente en la colección `perfiles_riesgo` (`cliente_id`, `score_externo`, `dispositivo_origen`).

#### Paso 2: Ingesta hacia la Capa Raw (Bronze)

* Configurar **GCP Datastream** para replicar en tiempo real los cambios (CDC) desde Cloud SQL directamente hacia **BigQuery** (Dataset: `raw_landing`).
* Para Firestore, ejecutar una exportación periódica automatizada (vía Cloud Functions o Dataflow) hacia Cloud Storage / BigQuery.

#### Paso 3: Transformación y Enterprise Data Bus con Dataform (o dbt)

Dentro de BigQuery, utilizar **Dataform** para ejecutar transformaciones SQL modulares:

1. **Capa Silver (Staging / Limpieza):** Limpia tipos de datos y desanida el JSON de Firestore.
2. **Capa Bus Dimensional:**
* Crea `dim_cliente` uniendo el perfil de Firestore con el registro de Postgres.
* Aplica lógica de versionamiento (SCD Tipo 2 si cambia la ciudad o score).
3. **Capa Gold (Data Marts):**
* Genera `fact_solicitudes` y la expone en el dataset `dm_creditos`.

#### Paso 4: Exposición y Dashboards

* Conectar **Looker Studio** al dataset `dm_creditos` para crear el dashboard del CEO mostrando:
* Tasa de aprobación por hora.
* Volumen colocado por segmento de cliente (`dim_cliente.segmento`).

---


### Paso 1. Configuración de PostgreSQL en Google Cloud SQL

#### Pasos de Despliegue (Google Cloud CLI / Cloud Shell)

Ejecuta los siguientes comandos para aprovisionar una instancia ligera de PostgreSQL (optimizada para pruebas de laboratorio) y habilitar la replicación de logs transaccionales:

```bash
# 1. Habilitar la API de Cloud SQL
gcloud services enable sqladmin.googleapis.com

# 2. Crear la instancia de Cloud SQL PostgreSQL (Habilitando logical decoding para CDC)
gcloud sql instances create fintech-pg-instance \
    --database-version=POSTGRES_15 \
    --tier=db-f1-micro \
    --region=us-central1 \
    --root-password="TuPasswordSeguro123!" \
    --database-flags=cloudsql.logical_decoding=on

# 3. Crear la base de datos transaccional
gcloud sql databases create fintech_db \
    --instance=fintech-pg-instance

```

#### DDL y Carga Inicial de Datos (SQL)

Conéctate a la base de datos usando `gcloud sql connect fintech-pg-instance --user=postgres --database=fintech_db` y ejecuta el siguiente script:

```sql
-- Creación del esquema transaccional
CREATE SCHEMA IF NOT EXISTS core;

-- Tabla transaccional de solicitudes de crédito
CREATE TABLE core.transacciones_solicitud (
    solicitud_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cliente_id VARCHAR(50) NOT NULL,
    monto_solicitado NUMERIC(12, 2) NOT NULL,
    plazo_meses INT NOT NULL,
    estado_solicitud VARCHAR(20) NOT NULL CHECK (estado_solicitud IN ('APROBADO', 'RECHAZADO', 'PENDIENTE')),
    tasa_interes NUMERIC(5, 2),
    fecha_solicitud TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Inserción de datos iniciales para validación
INSERT INTO core.transacciones_solicitud (cliente_id, monto_solicitado, plazo_meses, estado_solicitud, tasa_interes, fecha_solicitud) VALUES
('CLI-1001', 500000.00, 12, 'APROBADO', 2.15, CURRENT_TIMESTAMP - INTERVAL '2 hours'),
('CLI-1002', 1200000.00, 24, 'RECHAZADO', NULL, CURRENT_TIMESTAMP - INTERVAL '1 hour 45 minutes'),
('CLI-1003', 350000.00, 6, 'APROBADO', 1.95, CURRENT_TIMESTAMP - INTERVAL '1 hour'),
('CLI-1004', 2500000.00, 36, 'PENDIENTE', NULL, CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
('CLI-1005', 800000.00, 12, 'APROBADO', 2.05, CURRENT_TIMESTAMP - INTERVAL '5 minutes');

```

---


### Diagnóstico de Negocio y Técnico

Antes de ejecutar el despliegue de la capa de ingesta hacia la zona de aterrizaje (*Raw/Bronze Layer*) en BigQuery, es necesario validar tres dimensiones críticas:

1. **Objetivo Estratégico y SLA:** ¿El pipeline de replicación CDC de solicitudes requiere sincronización subsegundo para alimentar alertas inmediatas, o un retraso de segundos a minutos es suficiente para la analítica operativa?
2. **Naturaleza de los Orígenes y Gobernanza de Esquemas:** ¿Cómo se gestionará la mutación de esquemas (*Schema Drift*) en la colección NoSQL de Firestore al sincronizarse contra las tablas fuertemente tipadas de BigQuery?
3. **Consumidores Finales:** ¿La capa de aterrizaje (*Raw Dataset*) será consultada directamente por ingenieros de analítica para transformación, o se restringirá su acceso exclusivamente a los procesos automáticos de ETL/ELT por motivos de cumplimiento y auditoría?

---

### Paso 2: Ingesta Continua hacia la Capa Raw (BigQuery)

En esta fase configuramos la ingesta desacoplada y en tiempo real desde nuestras dos fuentes transaccionales (**Cloud SQL PostgreSQL** y **Cloud Firestore**) hacia el dataset de aterrizaje inmutable en **BigQuery**.

```
┌─────────────────────────┐          Datastream (CDC)          ┌──────────────────────────┐
│ Cloud SQL (PostgreSQL)  │───────────────────────────────────>│                          │
└─────────────────────────┘                                    │    BigQuery (Bronze)     │
                                                               │  Dataset: raw_landing    │
┌─────────────────────────┐   Firebase Extension (Stream)      │                          │
│     Cloud Firestore     │───────────────────────────────────>│                          │
└─────────────────────────┘                                    └──────────────────────────┘

```

---

#### 1. Preparación del Dataset de Aterrizaje en BigQuery

Ejecuta en Cloud Shell para crear el dataset de destino que actuará como capa Bronze:

```bash
# Crear dataset para la capa Raw
bq --location=us-central1 mk \
    --dataset \
    --description="Capa Raw / Bronze Landing inmutable de fuentes transaccionales" \
    fintech_analytics_raw

```

---

#### 2. Replicación CDC con GCP Datastream (PostgreSQL $\rightarrow$ BigQuery)

Datastream aprovecha el *logical decoding* habilitado en la instancia de Cloud SQL para capturar cada `INSERT`, `UPDATE` y `DELETE` sin impactar el rendimiento transaccional.

##### A. Configuración de la Conexión en PostgreSQL

Conéctate a la base de datos `fintech_db` como usuario `postgres` y ejecuta los permisos necesarios para la réplica lógica:

```sql
-- ============================================================================
-- 1. ELEVAR PRIVILEGIOS DEL ADMINISTRADOR (Sesión actual)
-- ============================================================================
-- Necesario en Cloud SQL para que 'postgres' pueda ejecutar funciones de replicación
ALTER USER postgres WITH REPLICATION;


-- ============================================================================
-- 2. CREACIÓN Y CONFIGURACIÓN DEL USUARIO DE SERVICIO DEDICADO
-- ============================================================================
-- Crear el usuario que utilizará Datastream para conectarse
CREATE USER datastream_user WITH REPLICATION ENCRYPTED PASSWORD 'StreamSecret2026!';

-- Asignar rol de administración de Cloud SQL
GRANT cloudsqlsuperuser TO datastream_user;

-- Otorgar permisos de lectura estrictamente necesarios en el esquema transaccional
GRANT USAGE ON SCHEMA core TO datastream_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO datastream_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO datastream_user;


-- ============================================================================
-- 3. CREACIÓN DE COMPONENTES DE REPLICACIÓN LÓGICA (CDC)
-- ============================================================================
-- Crear la publicación para todas las tablas del catálogo
CREATE PUBLICATION datastream_pub FOR ALL TABLES;

-- Crear el slot de replicación lógica con el plugin pgoutput
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('datastream_slot', 'pgoutput');


-- ============================================================================
-- 4. VALIDACIÓN DE ARTEFACTOS CREADOS
-- ============================================================================
-- Verificar que el slot esté activo y configurado
SELECT slot_name, plugin, slot_type, active 
FROM pg_replication_slots 
WHERE slot_name = 'datastream_slot';

-- Verificar la publicación
SELECT pubname, puballtables 
FROM pg_publication 
WHERE pubname = 'datastream_pub';

```

##### B. Habilitar APIs y Crear el Stream

```bash
# Habilitar APIs de Datastream
gcloud services enable datastream.googleapis.com

gcloud sql instances patch fintech-pg-instance \
    --authorized-networks=0.0.0.0/0

# 1. Crear Perfil de Conexión Origen (PostgreSQL)
gcloud datastream connection-profiles create pg-source-profile \
    --location=us-central1 \
    --type=POSTGRESQL \
    --display-name="Perfil Origen PostgreSQL Core" \
    --postgresql-hostname="34.63.106.132" \
    --postgresql-port=5432 \
    --postgresql-username=datastream_user \
    --postgresql-password="StreamSecret2026!" \
    --postgresql-database=fintech_db

# 2. Crear Perfil de Conexión Destino (BigQuery)
gcloud datastream connection-profiles create bq-target-profile \
    --display-name="Perfil Destino Bigquery" \
    --location=us-central1 \
    --type=bigquery

# 3. Crear y Activar el Stream hacia BigQuery
cat <<EOF > source.json
{
  "includeObjects": {
    "postgresqlSchemas": [
      {
        "schema": "core",
        "postgresqlTables": [
          {
            "table": "transacciones_solicitud"
          }
        ]
      }
    ]
  },
  "replicationSlot": "datastream_slot",
  "publication": "datastream_pub"
}
EOF

cat <<EOF > dest.json
{
  "singleTargetDataset": {
    "datasetId": "$(gcloud config get-value project):fintech_analytics_raw"
  },
  "dataFreshness": "0s"
}
EOF

gcloud datastream streams create pg-to-bq-stream \
    --location=us-central1 \
    --display-name="Stream PostgreSQL a BigQuery Raw" \
    --source=pg-source-profile \
    --destination=bq-target-profile \
    --backfill-all \
    --postgresql-source-config=source.json \
    --bigquery-destination-config=dest.json

```

*Resultado en BigQuery:* Datastream creará automáticamente la tabla `fintech_analytics_raw.core_transacciones_solicitud` sincronizada en tiempo real.

---

### Verificación de la Capa Raw

Una vez que Datastream y la Extensión están activos, puedes ejecutar una consulta de validación en BigQuery:

```sql
-- Validar replicación CDC de Postgres
SELECT * FROM `fintech_analytics_raw.core_transacciones_solicitud` LIMIT 5;

-- Validar streaming log de Firestore
SELECT 
    document_name, 
    operation, 
    JSON_EXTRACT_SCALAR(data, '$.cliente_id') AS cliente_id,
    JSON_EXTRACT_SCALAR(data, '$.score_crediticio') AS score,
    timestamp
FROM `fintech_analytics_raw.firestore_perfiles_riesgo_raw_raw_latest` 
LIMIT 5;

```

#### Simulamos la tabla de usuarios

OJO EL reto es que hagas una tabla de usuarios en Postgres o en otra BD y hagas el CDC puede ser usando el DataStream

```sql
-- ============================================================================
-- 1. CREACIÓN DE LA TABLA BRONZE (Estructura Nativa de Datastream)
-- ============================================================================
CREATE OR REPLACE TABLE `fintech_analytics_raw.core_usuarios` (
    -- Columnas del modelo transaccional (PostgreSQL)
    usuario_id STRING,
    nombre_completo STRING,
    ciudad_residencia STRING,
    score_crediticio INT64,
    ingreso_mensual BIGNUMERIC,
    fecha_creacion TIMESTAMP,

    -- Columnas de linaje y metadatos inyectadas automáticamente por Datastream
    _metadata_stream STRING,
    _metadata_timestamp TIMESTAMP,
    _metadata_read_timestamp TIMESTAMP,
    _metadata_read_method STRING,
    _metadata_source_type STRING,
    _metadata_deleted BOOLEAN,
    _metadata_schema STRING,
    _metadata_table STRING,
    _metadata_change_type STRING
);

-- ============================================================================
-- 2. INGESTA DE EVENTOS CDC (Simulación del Changelog)
-- ============================================================================
INSERT INTO `fintech_analytics_raw.core_usuarios` 
VALUES 
    -- Evento 1: Carga histórica inicial (Backfill). El sistema lee el registro base.
    ('USR-1001', 'Laura Gómez', 'Bogotá', 750, 4500000, TIMESTAMP('2026-01-15 08:30:00'), 
     'pg-to-bq-stream', TIMESTAMP('2026-08-20 10:00:00'), TIMESTAMP('2026-08-20 10:00:05'), 'oracle-dump', 'POSTGRESQL', FALSE, 'core', 'usuarios', 'INSERT'),

    ('USR-1002', 'Andrés Felipe', 'Medellín', 680, 3200000, TIMESTAMP('2026-02-10 09:15:00'), 
     'pg-to-bq-stream', TIMESTAMP('2026-08-20 10:00:00'), TIMESTAMP('2026-08-20 10:00:05'), 'oracle-dump', 'POSTGRESQL', FALSE, 'core', 'usuarios', 'INSERT'),

    -- Evento 2: Actualización (CDC). Laura cambia de ciudad y mejora su score.
    ('USR-1001', 'Laura Gómez', 'Cali', 790, 4500000, TIMESTAMP('2026-01-15 08:30:00'), 
     'pg-to-bq-stream', TIMESTAMP('2026-08-21 14:30:00'), TIMESTAMP('2026-08-21 14:30:02'), 'cdc', 'POSTGRESQL', FALSE, 'core', 'usuarios', 'UPDATE'),

    -- Evento 3: Eliminación lógica (CDC). Andrés cancela su cuenta en PostgreSQL.
    ('USR-1002', 'Andrés Felipe', 'Medellín', 680, 3200000, TIMESTAMP('2026-02-10 09:15:00'), 
     'pg-to-bq-stream', TIMESTAMP('2026-08-21 16:45:00'), TIMESTAMP('2026-08-21 16:45:01'), 'cdc', 'POSTGRESQL', TRUE, 'core', 'usuarios', 'DELETE');

```
---


### Paso 3: Arquitectura Medallion con Dataform

A continuación, estructuramos el paso 3 aplicando el patrón de Arquitectura Medallion de extremo a extremo usando Dataform.

Dado que eliminamos Firestore, asumiremos que en PostgreSQL también replicamos una tabla `core_clientes` (con datos demográficos básicos) para poder ilustrar el concepto de Dimensión y SCD Tipo 2.

#### 1. Capa Silver: Limpieza y Deduplicación CDC (Staging)

Los datos ingeridos por Datastream contienen metadatos (`_metadata_timestamp`, `_metadata_change_type`). La capa Silver crea vistas inmaterializadas que entregan la versión más reciente de cada registro, ignorando los eliminados lógicos.

**Archivo: `definitions/silver/stg_usuarios.sqlx**`

```sql
config { type: "view", schema: "silver" }

WITH historial_cdc AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY usuario_id ORDER BY _metadata_timestamp DESC) as orden_evento
  FROM ${ref("core_usuarios")} 
)
SELECT usuario_id, nombre_completo, ciudad_residencia, score_crediticio, ingreso_mensual 
FROM historial_cdc 
WHERE orden_evento = 1 AND _metadata_deleted = FALSE

```

**Archivo: `definitions/silver/stg_solicitudes.sqlx**`

```sql
config { type: "view", schema: "silver" }

WITH historial_cdc AS (
  SELECT *, ROW_NUMBER() OVER(PARTITION BY solicitud_id ORDER BY _metadata_timestamp DESC) as orden_evento
  FROM ${ref("core_transacciones_solicitud")} 
)
SELECT solicitud_id, usuario_id, monto_solicitado, plazo_meses, estado_solicitud, fecha_solicitud 
FROM historial_cdc 
WHERE orden_evento = 1 AND _metadata_deleted = FALSE

```


#### 2. Capa GOLD Enterprise Data Bus: Dimensión Cliente con SCD Tipo 2

**Archivo: `definitions/gold/dim_usuarios.sqlx**`

```sql
config { type: "incremental", schema: "edw_core", uniqueKey: ["usuario_id", "fecha_inicio_vigencia"] }

WITH datos_actuales AS (
  SELECT 
    usuario_id, nombre_completo, ciudad_residencia, score_crediticio, ingreso_mensual,
    CURRENT_TIMESTAMP() AS fecha_inicio_vigencia,
    CAST(NULL AS TIMESTAMP) AS fecha_fin_vigencia,
    TRUE AS es_registro_actual
  FROM ${ref("stg_usuarios")}
)
SELECT FARM_FINGERPRINT(CONCAT(usuario_id, CAST(fecha_inicio_vigencia AS STRING))) AS usuario_sk, *
FROM datos_actuales

```

**Archivo: `definitions/gold/fact_solicitudes.sqlx**`

```sql
config { type: "table", schema: "dm_creditos" }

SELECT 
  s.solicitud_id,
  u.usuario_sk, -- Llave subrogada que conecta con la dimensión
  s.fecha_solicitud,
  s.monto_solicitado,
  s.estado_solicitud
FROM ${ref("stg_solicitudes")} s -- Lee de la capa Silver
LEFT JOIN ${ref("dim_usuarios")} u -- Cruza con la dimensión Gold
  ON s.usuario_id = u.usuario_id
  AND u.es_registro_actual = TRUE

```

---
