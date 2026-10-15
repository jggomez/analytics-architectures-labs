# Lab 01 — CDC con Datastream y Arquitectura Medallion (Kimball) con Dataform

> 📖 **Marco Teórico:** Consulta la [Guía de Arquitectura y Patrones](teoria.md) para profundizar en ETL vs. ELT, Inmon vs. Kimball, Medallion y SCD2.

---

## Codelab — "FinTechCo": Solicitudes de Crédito

**FinTechCo** registra sus solicitudes de crédito y sus usuarios en una única base transaccional (**Cloud SQL PostgreSQL**). En este laboratorio vas a replicar esos cambios en tiempo real hacia **BigQuery** vía CDC con **Datastream**, y construir un pipeline completo Bronze → Silver → Gold con **Dataform**, terminando en un dashboard de Looker Studio.

```
ARQUITECTURA DEL LABORATORIO — FINTECHCO

┌──────────────────────────┐
│  Cloud SQL (PostgreSQL)  │
│  core.usuarios           │
│  core.transacciones_     │
│  solicitud                │
└─────────────┬─────────────┘
              │ Datastream (CDC, append-only)
              ▼
┌───────────────────────────────────────┐
│  BIGQUERY — CAPA BRONZE (RAW)         │
│  fintech_analytics_raw.core_usuarios  │
│  fintech_analytics_raw.core_          │
│  transacciones_solicitud               │
└─────────────────┬───────────────────────┘
                  │ Dataform (dedup CDC)
                  ▼
┌───────────────────────────────────────┐
│  BIGQUERY — CAPA SILVER (silver)      │
│  stg_usuarios, stg_solicitudes         │
└─────────────────┬───────────────────────┘
                  │ Dataform (modelado Kimball)
                  ▼
┌───────────────────────────────────────┐
│  BIGQUERY — CAPA GOLD                 │
│  edw_core.dim_usuarios                │
│  dm_creditos.fact_solicitudes          │
└─────────────────┬───────────────────────┘
                  ▼
           Looker Studio
```

> [!NOTE]
> Este lab usa **una sola fuente transaccional** (PostgreSQL) para mantener el flujo de punta a punta coherente. Si quieres agregar una segunda fuente semiestructurada (por ejemplo, Firestore), hazlo como el **Reto 1** al final — no forma parte del camino principal.

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. Configurar un **stream de Datastream** que replica cambios de PostgreSQL (dos tablas relacionadas) hacia BigQuery en modo **append-only** (conservando el historial completo de cambios, no solo el estado actual).
2. Leer correctamente la columna `datastream_metadata` (STRUCT) que Datastream agrega a cada tabla destino en BigQuery.
3. Deduplicar eventos CDC en la capa Silver con `ROW_NUMBER() OVER (... ORDER BY datastream_metadata.source_timestamp DESC)`.
4. Modelar la capa Gold siguiendo Kimball: una dimensión con clave subrogada (`dim_usuarios`) y una tabla de hechos (`fact_solicitudes`) que las conecta.
5. Construir una **capa de control con assertions de Dataform** que valida Silver y Gold y **bloquea** la carga de la tabla de hechos cuando llegan datos malos.
6. Conectar Looker Studio al data mart resultante.

### Prerrequisitos

- Proyecto de Google Cloud con facturación habilitada.
- Cloud Shell + acceso a la consola web de Google Cloud (para Dataform).
- Conocimientos básicos de SQL y del patrón CDC/Medallion (ver [teoría](teoria.md)).

### Costo Estimado (FinOps)

| Concepto | Recurso en el Lab | ¿Cubierto por capa gratuita? | Estimado |
|---|---|---|---|
| **Cloud SQL (PostgreSQL)** | 1 instancia `db-f1-micro`, ~1-2h de uso | ❌ No | **~$0.02–0.05** |
| **Datastream** | 1 stream CDC activo ~1h, volumen mínimo | ❌ No (se cobra por GB procesado) | **~$0.05–0.10** |
| **BigQuery** (Bronze/Silver/Gold) | Storage + queries de Dataform | ✅ Sí (1 TiB consultas / 10 GiB storage gratis al mes) | **~$0.00** |
| **Dataform** | Orquestación de las transformaciones | ✅ Sí — Dataform en sí no tiene cargo aparte; solo pagas los jobs de BigQuery que ejecuta (ya contados arriba) | **$0.00** |
| **Looker Studio** | Dashboard | ✅ Sí — siempre gratuito | **$0.00** |

> [!WARNING]
> Cloud SQL y Datastream **facturan mientras existan**, aunque no los estés usando activamente. El Paso 6 (Limpieza) no es opcional.

---

### Mapa del Laboratorio (~110 minutos)

```
Paso 0  (10 min)  Aprovisionar el entorno (variables, APIs)
Paso 1  (15 min)  Cloud SQL: esquema, datos y replicación lógica
Paso 2  (20 min)  Datastream: stream CDC (append-only) hacia BigQuery
Paso 3  (10 min)  Verificar la capa Raw y simular CDC en vivo
Paso 4  (35 min)  Dataform: Silver (dedup), Gold (Kimball) y capa de control (assertions)
Paso 5  (10 min)  Looker Studio: dashboard
Paso 6  (10 min)  Limpieza + Retos Opcionales
```

---

## Paso 0 — Aprovisionar el Entorno (10 min)

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1

gcloud services enable \
  sqladmin.googleapis.com \
  datastream.googleapis.com \
  bigquery.googleapis.com \
  dataform.googleapis.com

bq --location="$REGION" mk --dataset \
  --description "FinTechCo - Capa Raw/Bronze" \
  "${PROJECT_ID}:fintech_analytics_raw"
```

> [!NOTE]
> Los datasets `silver`, `edw_core` y `dm_creditos` de las capas Silver/Gold **no** hace falta crearlos a mano — Dataform los crea automáticamente la primera vez que ejecuta un modelo con ese `schema` en su `config`.

---

## Paso 1 — Cloud SQL: Esquema, Datos y Replicación Lógica (15 min)

### 1.1. Crear la instancia

```bash
gcloud sql instances create fintech-pg-instance \
    --database-version=POSTGRES_15 \
    --tier=db-f1-micro \
    --region="$REGION" \
    --root-password="TuPasswordSeguro123!" \
    --database-flags=cloudsql.logical_decoding=on \
    --storage-size=10

gcloud sql databases create fintech_db --instance=fintech-pg-instance

# Solo para el laboratorio: abre el acceso para que Datastream pueda alcanzar la instancia
gcloud sql instances patch fintech-pg-instance --authorized-networks=0.0.0.0/0
```

> [!WARNING]
> `--authorized-networks=0.0.0.0/0` deja la instancia abierta a internet con autenticación por contraseña. Es aceptable **solo** en este laboratorio aislado — nunca lo repliques en un proyecto con datos reales; usa Private Service Connect o un rango de IPs restringido.

### 1.2. Esquema y datos iniciales

Conéctate con `gcloud sql connect fintech-pg-instance --user=postgres --database=fintech_db` y ejecuta:

```sql
CREATE SCHEMA IF NOT EXISTS core;

-- Tabla de usuarios
CREATE TABLE core.usuarios (
    usuario_id         VARCHAR(20) PRIMARY KEY,
    nombre_completo    VARCHAR(100) NOT NULL,
    ciudad_residencia  VARCHAR(50) NOT NULL,
    score_crediticio   INT NOT NULL,
    ingreso_mensual    NUMERIC(14, 2) NOT NULL,
    fecha_creacion     TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO core.usuarios (usuario_id, nombre_completo, ciudad_residencia, score_crediticio, ingreso_mensual) VALUES
('USR-1001', 'Laura Gómez', 'Bogotá', 750, 4500000.00),
('USR-1002', 'Andrés Felipe', 'Medellín', 680, 3200000.00),
('USR-1003', 'Marta Díaz', 'Cali', 710, 3900000.00);

-- Tabla transaccional de solicitudes de crédito (referencia a usuarios por convención, sin FK física)
CREATE TABLE core.transacciones_solicitud (
    solicitud_id      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cliente_id        VARCHAR(20) NOT NULL,
    monto_solicitado  NUMERIC(12, 2) NOT NULL,
    plazo_meses       INT NOT NULL,
    estado_solicitud  VARCHAR(20) NOT NULL CHECK (estado_solicitud IN ('APROBADO', 'RECHAZADO', 'PENDIENTE')),
    tasa_interes      NUMERIC(5, 2),
    fecha_solicitud   TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO core.transacciones_solicitud (cliente_id, monto_solicitado, plazo_meses, estado_solicitud, tasa_interes, fecha_solicitud) VALUES
('USR-1001', 500000.00, 12, 'APROBADO', 2.15, CURRENT_TIMESTAMP - INTERVAL '2 hours'),
('USR-1002', 1200000.00, 24, 'RECHAZADO', NULL, CURRENT_TIMESTAMP - INTERVAL '1 hour 45 minutes'),
('USR-1001', 350000.00, 6, 'APROBADO', 1.95, CURRENT_TIMESTAMP - INTERVAL '1 hour'),
('USR-1003', 2500000.00, 36, 'PENDIENTE', NULL, CURRENT_TIMESTAMP - INTERVAL '30 minutes'),
('USR-1002', 800000.00, 12, 'APROBADO', 2.05, CURRENT_TIMESTAMP - INTERVAL '5 minutes');
```

### 1.3. Preparar la replicación lógica para Datastream

```sql
ALTER USER postgres WITH REPLICATION;

CREATE USER datastream_user WITH REPLICATION ENCRYPTED PASSWORD 'StreamSecret2026!';
GRANT cloudsqlsuperuser TO datastream_user;
GRANT USAGE ON SCHEMA core TO datastream_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO datastream_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO datastream_user;

-- Una sola publicación cubre ambas tablas
CREATE PUBLICATION datastream_pub FOR ALL TABLES;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('datastream_slot', 'pgoutput');
```

---

## Paso 2 — Datastream: Stream CDC hacia BigQuery (20 min)

### 2.1. Perfiles de conexión

```bash
gcloud datastream connection-profiles create pg-source-profile \
    --location="$REGION" \
    --type=postgresql \
    --display-name="Perfil Origen PostgreSQL Core" \
    --postgresql-hostname="$(gcloud sql instances describe fintech-pg-instance --format='value(ipAddresses[0].ipAddress)')" \
    --postgresql-port=5432 \
    --postgresql-username=datastream_user \
    --postgresql-password="StreamSecret2026!" \
    --postgresql-database=fintech_db

gcloud datastream connection-profiles create bq-target-profile \
    --location="$REGION" \
    --type=bigquery \
    --display-name="Perfil Destino BigQuery"
```

### 2.2. Configurar y crear el stream (ambas tablas, modo append-only)

```bash
cat <<'EOF' > source.json
{
  "includeObjects": {
    "postgresqlSchemas": [
      {
        "schema": "core",
        "postgresqlTables": [
          { "table": "usuarios" },
          { "table": "transacciones_solicitud" }
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
  "dataFreshness": "0s",
  "appendOnly": {}
}
EOF

gcloud datastream streams create pg-to-bq-stream \
    --location="$REGION" \
    --display-name="Stream PostgreSQL a BigQuery Raw" \
    --source=pg-source-profile \
    --destination=bq-target-profile \
    --postgresql-source-config=source.json \
    --bigquery-destination-config=dest.json \
    --backfill-all
```

> [!IMPORTANT]
> `"appendOnly": {}` es una decisión de diseño deliberada: por **defecto**, un stream de Datastream hacia BigQuery usa modo **merge** (upsert) — la tabla destino queda con **una sola fila por clave primaria**, siempre con el valor más reciente. Eso es cómodo para consumo directo, pero **no deja nada que deduplicar en la capa Silver** (ver [teoría](teoria.md)), que es justo lo que este lab quiere enseñar. Con `appendOnly`, Datastream conserva **cada evento** (`INSERT`/`UPDATE`/`DELETE`) como una fila nueva, y la deduplicación real ocurre en Dataform (Paso 4) — igual que en el [Módulo 05](../05-elt-dataflow-iceberg/lab.md).

### 2.3. Iniciar el stream

Los streams se crean en estado `NOT_STARTED`; hay que arrancarlos explícitamente:

```bash
gcloud datastream streams update pg-to-bq-stream \
    --location="$REGION" \
    --state=RUNNING \
    --update-mask=state

# Verifica el estado (pasa por STARTING -> RUNNING en 1-2 minutos)
gcloud datastream streams describe pg-to-bq-stream \
    --location="$REGION" --format="value(state)"
```

---

## Paso 3 — Verificar la Capa Raw y Simular CDC en Vivo (10 min)

```sql
SELECT * FROM `fintech_analytics_raw.core_usuarios` LIMIT 10;
SELECT * FROM `fintech_analytics_raw.core_transacciones_solicitud` LIMIT 10;
```

> [!NOTE]
> Cada tabla trae una columna **`datastream_metadata`** (tipo `RECORD`/`STRUCT`), no columnas planas `_metadata_*`. En modo append-only expone, entre otros, `datastream_metadata.source_timestamp` (para ordenar eventos) y `datastream_metadata.change_type` (`INSERT`/`UPDATE`/`DELETE` — confírmalo en tu tabla con `SELECT DISTINCT datastream_metadata.change_type FROM ...`, ya que el valor exacto puede variar). Esto es distinto del destino Cloud Storage que usa el Módulo 05, donde esos mismos metadatos llegan anidados dentro de cada evento Avro en vez de en una columna dedicada.

Ahora, para demostrar CDC real (no solo el backfill), vuelve a `gcloud sql connect fintech-pg-instance --user=postgres --database=fintech_db` y ejecuta:

```sql
-- Laura se muda de ciudad y mejora su score
UPDATE core.usuarios SET ciudad_residencia = 'Cali', score_crediticio = 790 WHERE usuario_id = 'USR-1001';
```

En 15-30 segundos aparecerá un nuevo registro en `core_usuarios` para `USR-1001` (con `change_type = 'UPDATE'`), **sin que se borre el anterior** — porque el stream es append-only. Ahora tienes dos versiones de Laura en Bronze: exactamente lo que la capa Silver del Paso 4 va a deduplicar.

---

## Paso 4 — Dataform: Silver (Deduplicación), Gold (Kimball) y Capa de Control (35 min)

### 4.1. Crear el repositorio y workspace de Dataform

1. En la consola de Google Cloud, ve a **BigQuery → Dataform** → **Create Repository**.
2. Dale un nombre (ej. `fintech-medallion`) y la región `us-central1`.
3. Dentro del repositorio, crea un **Development Workspace** (ej. `dev-workspace`) e inicialízalo con la plantilla de proyecto por defecto.

### 4.2. Declarar las fuentes

**Archivo: `definitions/sources/sources.js`** (o dos archivos `.sqlx` de tipo `declaration`, como prefieras):

```sql
config {
    type: "declaration",
    schema: "fintech_analytics_raw",
    name: "core_usuarios",
    description: "Tabla Raw de usuarios replicada vía Datastream (append-only)"
}
```

```sql
config {
    type: "declaration",
    schema: "fintech_analytics_raw",
    name: "core_transacciones_solicitud",
    description: "Tabla Raw de solicitudes replicada vía Datastream (append-only)"
}
```

### 4.3. Capa Silver: deduplicación del CDC

**Archivo: `definitions/silver/stg_usuarios.sqlx`**

```sql
config {
    type: "table",
    schema: "silver",
    description: "Usuarios deduplicados: se queda el evento más reciente por usuario_id"
}

WITH
  historial_cdc AS (
  SELECT
    usuario_id,
    nombre_completo,
    ciudad_residencia,
    score_crediticio,
    ingreso_mensual,
    datastream_metadata.change_type AS change_type,
    ROW_NUMBER() OVER (PARTITION BY usuario_id ORDER BY datastream_metadata.source_timestamp DESC) AS orden_evento
  FROM
    ${ref("core_usuarios")} )
SELECT
  usuario_id,
  nombre_completo,
  ciudad_residencia,
  score_crediticio,
  ingreso_mensual
FROM
  historial_cdc
WHERE
  orden_evento = 1
  AND change_type != 'DELETE'
```

**Archivo: `definitions/silver/stg_solicitudes.sqlx`**

```sql
config {
    type: "table",
    schema: "silver",
    description: "Solicitudes deduplicadas: se queda el evento más reciente por solicitud_id"
}

WITH
  historial_cdc AS (
  SELECT
    solicitud_id,
    cliente_id,
    monto_solicitado,
    plazo_meses,
    estado_solicitud,
    tasa_interes,
    fecha_solicitud,
    datastream_metadata.change_type AS change_type,
    ROW_NUMBER() OVER (PARTITION BY solicitud_id ORDER BY datastream_metadata.source_timestamp DESC) AS orden_evento
  FROM
    ${ref("core_transacciones_solicitud")} )
SELECT
  solicitud_id,
  cliente_id,
  monto_solicitado,
  plazo_meses,
  estado_solicitud,
  tasa_interes,
  fecha_solicitud
FROM
  historial_cdc
WHERE
  orden_evento = 1
  AND change_type != 'DELETE'
```

> [!IMPORTANT]
> Fíjate en el nombre de cada archivo: `stg_usuarios.sqlx` lee de `core_usuarios` y devuelve columnas de usuario; `stg_solicitudes.sqlx` lee de `core_transacciones_solicitud` y devuelve columnas de solicitud. Dataform resuelve `${ref("stg_usuarios")}` **por el nombre del archivo/acción**, así que si el contenido no coincide con el nombre, todo lo que dependa de él (la capa Gold) falla con errores de "columna no encontrada".

### 4.4. Capa Gold: Dimensión y Hecho (Kimball)

**Archivo: `definitions/gold/dim_usuarios.sqlx`**

```sql
config {
    type: "incremental",
    schema: "edw_core",
    uniqueKey: ["usuario_id", "fecha_inicio_vigencia"]
}

WITH
  datos_actuales AS (
  SELECT
    usuario_id,
    nombre_completo,
    ciudad_residencia,
    score_crediticio,
    ingreso_mensual,
    CURRENT_TIMESTAMP() AS fecha_inicio_vigencia,
    CAST(NULL AS TIMESTAMP) AS fecha_fin_vigencia,
    TRUE AS es_registro_actual
  FROM
    ${ref("stg_usuarios")} )
SELECT
  FARM_FINGERPRINT(CONCAT(usuario_id, CAST(fecha_inicio_vigencia AS STRING))) AS usuario_sk,
  *
FROM
  datos_actuales d
${when(incremental(), `
-- En las ejecuciones incrementales solo entra un usuario nuevo o uno cuyos atributos cambiaron
WHERE NOT EXISTS (
  SELECT 1 FROM ${self()} t
  WHERE t.usuario_id = d.usuario_id
    AND t.es_registro_actual
    AND t.nombre_completo = d.nombre_completo
    AND t.ciudad_residencia = d.ciudad_residencia
    AND t.score_crediticio = d.score_crediticio
    AND t.ingreso_mensual = d.ingreso_mensual)`)}
```

> [!NOTE]
> Esta versión es una simplificación didáctica: re-ejecutarla sin cambios no agrega filas (gracias al `WHERE NOT EXISTS` incremental), pero cuando un usuario cambia, inserta la versión nueva **sin cerrar la anterior**. No implementa el `MERGE` completo de SCD Tipo 2 con cierre de vigencias. La assertion `assert_una_version_vigente` del Paso 4.5 detecta justamente ese caso. Verás el patrón `MERGE` real, con `valid_from`/`valid_to`, en [teoría §6.3](teoria.md#63-slowly-changing-dimensions-scd-gestión-de-historial) — implementarlo aquí es el **Reto 2**.

**Archivo: `definitions/gold/fact_solicitudes.sqlx`**

```sql
config {
    type: "incremental",
    schema: "dm_creditos",
    uniqueKey: ["solicitud_id"]
}

SELECT
  s.solicitud_id,
  u.usuario_sk,
  -- Llave subrogada que conecta con la dimensión
  s.fecha_solicitud,
  s.monto_solicitado,
  s.estado_solicitud
FROM
  ${ref("stg_solicitudes")} s
  -- Lee de la capa Silver
LEFT JOIN
  ${ref("dim_usuarios")} u
  -- Cruza con la dimensión Gold
ON
  s.cliente_id = u.usuario_id
  AND u.es_registro_actual = TRUE
```

> [!NOTE]
> `uniqueKey: ["solicitud_id"]` convierte cada ejecución incremental en un `MERGE` por solicitud: re-ejecutar Dataform no duplica filas, y si una solicitud cambia de estado (por ejemplo, de `PENDIENTE` a `APROBADO`) se actualiza en vez de agregarse otra vez.

### 4.5. Capa de control: assertions

Hasta aquí, Dataform construye las tablas **aunque los datos estén mal**: si el origen manda una solicitud con monto `0` o un plazo de `0` meses, llega intacta hasta el tablero del CEO. Una **assertion** es una consulta que devuelve las filas que **violan** una regla. Si devuelve cero filas, pasa; si devuelve alguna, falla. Dataform tiene dos tipos:

| Tipo | Dónde se declara | Para qué |
|---|---|---|
| **Integrada** (`assertions: {…}` en el `config`) | En el mismo archivo del modelo | Reglas comunes sobre una tabla: unicidad (`uniqueKey`), no nulos (`nonNull`), condiciones por fila (`rowConditions`) |
| **Manual** (`type: "assertion"`) | En su propio archivo `.sqlx` | Reglas que cruzan tablas o requieren lógica propia: integridad referencial, una sola versión vigente, conciliaciones |

**1. Assertions integradas en Silver.** Agrega el bloque `assertions` al `config` de los dos modelos de Silver (el SQL de cada archivo no cambia):

**Archivo: `definitions/silver/stg_usuarios.sqlx`** (solo el `config`)

```sql
config {
    type: "table",
    schema: "silver",
    description: "Usuarios deduplicados: se queda el evento más reciente por usuario_id",
    assertions: {
        uniqueKey: ["usuario_id"],
        nonNull: ["usuario_id", "nombre_completo", "score_crediticio", "ingreso_mensual"],
        rowConditions: [
            "score_crediticio BETWEEN 300 AND 850",
            "ingreso_mensual > 0"
        ]
    }
}
```

**Archivo: `definitions/silver/stg_solicitudes.sqlx`** (solo el `config`)

```sql
config {
    type: "table",
    schema: "silver",
    description: "Solicitudes deduplicadas: se queda el evento más reciente por solicitud_id",
    assertions: {
        uniqueKey: ["solicitud_id"],
        nonNull: ["solicitud_id", "cliente_id", "monto_solicitado", "plazo_meses", "estado_solicitud"],
        rowConditions: [
            "monto_solicitado > 0",
            "plazo_meses BETWEEN 1 AND 120",
            "estado_solicitud IN ('APROBADO', 'RECHAZADO', 'PENDIENTE')",
            "estado_solicitud != 'APROBADO' OR tasa_interes IS NOT NULL"
        ]
    }
}
```

La última regla es una **regla de negocio**, no de formato: una solicitud aprobada sin tasa de interés no se puede cobrar. Este tipo de regla es la que más valor aporta, porque ninguna restricción de la base de datos la expresa.

**2. Assertions manuales en Gold.**

**Archivo: `definitions/assertions/assert_una_version_vigente.sqlx`**

```sql
config {
    type: "assertion",
    description: "Cada usuario debe tener exactamente UNA versión vigente en dim_usuarios"
}

SELECT
  usuario_id,
  COUNT(*) AS versiones_vigentes
FROM
  ${ref("dim_usuarios")}
WHERE
  es_registro_actual
GROUP BY
  usuario_id
HAVING
  COUNT(*) > 1
```

**Archivo: `definitions/assertions/assert_solicitudes_con_usuario.sqlx`**

```sql
config {
    type: "assertion",
    description: "Integridad referencial: toda solicitud de la tabla de hechos debe tener un usuario en la dimensión"
}

SELECT
  solicitud_id,
  fecha_solicitud
FROM
  ${ref("fact_solicitudes")}
WHERE
  usuario_sk IS NULL
```

**3. Convertir las assertions en una puerta.** Por defecto, una assertion que falla **avisa pero no detiene nada**: la tabla de hechos se cargaría igual. Para que una tabla solo se construya si las validaciones de lo que lee pasan, sus assertions tienen que ser **dependencias** de esa tabla. Actualiza el `config` de los dos modelos de Gold:

**Archivo: `definitions/gold/dim_usuarios.sqlx`** (solo el `config`)

```sql
config {
    type: "incremental",
    schema: "edw_core",
    uniqueKey: ["usuario_id", "fecha_inicio_vigencia"],
    dependOnDependencyAssertions: true
}
```

**Archivo: `definitions/gold/fact_solicitudes.sqlx`** (solo el `config`)

```sql
config {
    type: "incremental",
    schema: "dm_creditos",
    uniqueKey: ["solicitud_id"],
    dependOnDependencyAssertions: true,
    dependencies: ["assert_una_version_vigente"]
}
```

- **`dependOnDependencyAssertions: true`** convierte en dependencias todas las assertions integradas de las tablas que el modelo lee con `${ref()}`. Así, `dim_usuarios` espera a las de `stg_usuarios`, y `fact_solicitudes` espera a las de `stg_solicitudes`.
- **`dependencies: ["assert_una_version_vigente"]`** agrega una assertion manual. Hace falta nombrarla explícitamente porque no "pertenece" a ninguna tabla que el modelo lea.
- `assert_solicitudes_con_usuario` corre **después** de cargar los hechos: no puede bloquear su propia tabla, pero sí avisar. Es una auditoría posterior a la carga.

```
                  ┌─ assertions stg_usuarios ─┐
stg_usuarios ─────┤                           ├──► dim_usuarios ──► assert_una_version_vigente ─┐
                  └───────────────────────────┘                                                  │
                  ┌─ assertions stg_solicitudes ┐                                                ▼
stg_solicitudes ──┤                             ├──────────────────────────────────────► fact_solicitudes ──► assert_solicitudes_con_usuario
                  └─────────────────────────────┘
```

### 4.6. Ejecutar

En el workspace de Dataform, clic en **Start execution** → selecciona todas las acciones (`stg_usuarios`, `stg_solicitudes`, `dim_usuarios`, `fact_solicitudes` y las assertions) → **Execute**. Dataform calcula el orden de dependencias automáticamente a partir de los `${ref(...)}` y de los `dependencies` — no necesitas correrlas en un orden manual.

En el grafo de la ejecución vas a ver las assertions como acciones propias, con nombres como `silver_stg_solicitudes_assertions_rowConditions` y `silver_stg_solicitudes_assertions_uniqueKey_0` (el patrón es `<dataset>_<tabla>_assertions_<tipo>`). Todas deben terminar en verde. Dataform guarda cada assertion como una **vista** en el dataset `dataform_assertions` (el dataset de assertions que define `workflow_settings.yaml`): puedes consultarla en cualquier momento para ver qué filas violan la regla.

Valida el resultado:

```sql
SELECT * FROM `edw_core.dim_usuarios` ORDER BY usuario_id, fecha_inicio_vigencia;
SELECT * FROM `dm_creditos.fact_solicitudes` ORDER BY fecha_solicitud;
```

Deberías ver una fila vigente por usuario en `dim_usuarios` (la de `USR-1001`, Laura, ya con Cali y score 790 si hiciste el `UPDATE` del Paso 3), y sus solicitudes en `fact_solicitudes` correctamente enlazadas por `usuario_sk`.

### 4.7. Probar la capa de control: llega un dato malo

Un error en la app de originación registra una solicitud **aprobada con monto 0, plazo 0 y sin tasa**. Vuelve a `gcloud sql connect fintech-pg-instance --user=postgres --database=fintech_db` y ejecuta:

```sql
INSERT INTO core.transacciones_solicitud (cliente_id, monto_solicitado, plazo_meses, estado_solicitud, tasa_interes)
VALUES ('USR-1003', 0, 0, 'APROBADO', NULL);
```

Espera 30 segundos a que Datastream lo replique y vuelve a ejecutar **todas las acciones** en Dataform. Esta vez:

- `stg_solicitudes` se reconstruye ✅ (la tabla Silver sí se escribe).
- `silver_stg_solicitudes_assertions_rowConditions` **falla** ❌.
- `fact_solicitudes` queda **omitida** (no se ejecuta), porque depende de esa assertion.
- `dim_usuarios` y `assert_una_version_vigente` pasan: el problema está en las solicitudes, no en los usuarios.

Diagnostica qué filas rompieron qué regla consultando la vista de la assertion:

```sql
SELECT * FROM `dataform_assertions.silver_stg_solicitudes_assertions_rowConditions`;

-- La tabla de hechos NO tiene la solicitud mala: el tablero sigue mostrando datos consistentes
SELECT COUNT(*) AS solicitudes, COUNTIF(monto_solicitado = 0) AS con_monto_cero
FROM `dm_creditos.fact_solicitudes`;
```

> [!IMPORTANT]
> Esta es la diferencia entre una assertion que **avisa** y una capa de control que **protege**. Sin las `dependencies`, Dataform habría marcado la assertion en rojo y aun así habría cargado la solicitud mala en `fact_solicitudes`. Con ellas, Silver se actualiza (y se puede inspeccionar), pero **Gold no recibe datos que no pasaron las reglas**.

Corrige el dato en el origen. El borrado viaja por CDC como un evento `DELETE`, y `stg_solicitudes` lo excluye:

```sql
DELETE FROM core.transacciones_solicitud WHERE monto_solicitado = 0;
```

Espera 30 segundos y ejecuta otra vez todas las acciones: ahora todo queda en verde y `fact_solicitudes` se vuelve a cargar.

> [!TIP]
> **¿Qué pasa si un usuario cambia?** Ejecuta `UPDATE core.usuarios SET score_crediticio = 800 WHERE usuario_id = 'USR-1002';` y vuelve a correr Dataform. `dim_usuarios` inserta la versión nueva **sin cerrar la anterior** (la simplificación del Paso 4.4), `assert_una_version_vigente` falla y `fact_solicitudes` no se carga. La assertion encontró un defecto real del modelo. El Reto 2 lo corrige con un SCD2 de verdad.

---

## Paso 5 — Looker Studio: Dashboard (10 min)

1. Ve a [lookerstudio.google.com](https://lookerstudio.google.com) → **Crear → Fuente de datos → BigQuery**.
2. Selecciona tu proyecto → dataset `dm_creditos` → tabla `fact_solicitudes` (agrega `edw_core.dim_usuarios` como fuente adicional si quieres desglosar por ciudad o segmento).
3. Crea una tabla o gráfico de barras: dimensión `estado_solicitud`, métrica `COUNT(solicitud_id)` — la "tasa de aprobación" que pediría el CEO.

---

## Paso 6 — Limpieza (10 min)

> [!IMPORTANT]
> Cloud SQL y Datastream facturan mientras existan. Ejecuta esto antes de cerrar la sesión.

```bash
# 1. Pausar y eliminar el stream de Datastream (pausar no es instantáneo: espera a PAUSED)
gcloud datastream streams update pg-to-bq-stream \
    --location="$REGION" --state=PAUSED --update-mask=state

until [[ "$(gcloud datastream streams describe pg-to-bq-stream --location="$REGION" --format='value(state)')" == "PAUSED" ]]; do
  echo "Esperando a que el stream termine de pausarse..."
  sleep 10
done

gcloud datastream streams delete pg-to-bq-stream --location="$REGION" --quiet
gcloud datastream connection-profiles delete pg-source-profile --location="$REGION" --quiet
gcloud datastream connection-profiles delete bq-target-profile --location="$REGION" --quiet

# 2. Eliminar Cloud SQL
gcloud sql instances delete fintech-pg-instance --quiet

# 3. Eliminar los datasets de BigQuery
bq rm --recursive --force "${PROJECT_ID}:fintech_analytics_raw"
bq rm --recursive --force "${PROJECT_ID}:silver"
bq rm --recursive --force "${PROJECT_ID}:edw_core"
bq rm --recursive --force "${PROJECT_ID}:dm_creditos"
```

Por último, en la consola: **BigQuery → Dataform** → elimina el repositorio `fintech-medallion` si ya no lo necesitas.

---

## Retos Opcionales (Extensión)

### Reto 1: Agrega una fuente semiestructurada con Firestore

Crea una colección `perfiles_riesgo` en Firestore (`cliente_id`, `score_externo`, `dispositivo_origen`) e instala la [Firebase Extension "Export Collections to BigQuery"](https://extensions.dev/extensions/firebase/firestore-bigquery-export) para exportarla en tiempo real hacia `fintech_analytics_raw`. Desanida el JSON resultante en un nuevo modelo Silver y crúzalo con `stg_usuarios` por `cliente_id`.

<details>
<summary>👀 Ver Pista de Solución Reto 1</summary>

La extensión crea una tabla de *changelog* (`<coleccion>_raw_changelog`) y una vista con el estado más reciente (`<coleccion>_raw_latest`) — usa la vista `_raw_latest` como fuente en tu declaración de Dataform, y `JSON_EXTRACT_SCALAR(data, '$.score_externo')` para desanidar cada campo del documento.

</details>

### Reto 2: Implementa el SCD Tipo 2 real con `MERGE`

Reemplaza el `dim_usuarios.sqlx` simplificado del Paso 4.4 por el patrón `MERGE` completo de [teoría §6.3](teoria.md#63-slowly-changing-dimensions-scd-gestión-de-historial), cerrando la vigencia (`valid_to`, `is_current = FALSE`) de la versión anterior cuando cambie `ciudad_residencia` o `score_crediticio`. Sabrás que funciona cuando `assert_una_version_vigente` (Paso 4.5) pase después de actualizar un usuario.

<details>
<summary>👀 Ver Pista de Solución Reto 2</summary>

En Dataform, un modelo `MERGE` se declara con `config { type: "operations" }` en vez de `"table"` o `"incremental"` — dentro escribes el `MERGE` tal cual, usando `${ref("stg_usuarios")}` como fuente y el nombre completo de la tabla destino (`${self()}` para referirte a la propia tabla del modelo).

</details>

### Reto 3: Detecta un borrado real

Ejecuta `DELETE FROM core.transacciones_solicitud WHERE estado_solicitud = 'RECHAZADO';` en PostgreSQL, y confirma en `stg_solicitudes` que esa fila desaparece (por el filtro `change_type != 'DELETE'`) mientras que en `core_transacciones_solicitud` (Bronze) sigue existiendo el evento de borrado — la diferencia entre "lo que pasó" (Bronze, inmutable) y "el estado actual" (Silver).

---

## Referencias

- [Datastream — Destino BigQuery](https://docs.cloud.google.com/datastream/docs/destination-bigquery)
- [Datastream — Create connection profiles](https://docs.cloud.google.com/datastream/docs/create-connection-profiles)
- [Dataform — Incremental tables](https://cloud.google.com/dataform/docs/incremental-tables)
- [Dataform — Test data quality with assertions](https://docs.cloud.google.com/dataform/docs/assertions)
- [Dataform — Declare dependencies (assertions como dependencias)](https://docs.cloud.google.com/dataform/docs/dependencies)
- [Dataform — Operations (MERGE, DDL/DML custom)](https://cloud.google.com/dataform/docs/define-operations)
- [Firebase Extension — Export Collections to BigQuery](https://extensions.dev/extensions/firebase/firestore-bigquery-export)
