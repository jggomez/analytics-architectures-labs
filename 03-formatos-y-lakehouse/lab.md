# Lab 03 — Formatos de archivo y formatos de tabla abiertos

> 📖 **Marco Teórico:** Consulta la [Guía de Formatos de Archivo y Tablas Abiertas](teoria.md) para profundizar en Avro, Parquet, Iceberg, Delta Lake y Hudi.

## Codelab paso a paso

Laboratorio corto para **ver por dentro** los formatos de datos que se usan en un lakehouse
y entender cuándo elegir cada uno:

- **Formatos de archivo:** Parquet (columnar) y Avro (fila).
- **Formatos de tabla abiertos:** Delta Lake, Apache Iceberg y Apache Hudi (una capa de
  metadatos con ACID sobre archivos Parquet).

El *hands-on* de escritura se concentra en **Parquet/Avro** (Parte A) e **Iceberg
gestionado en BigQuery** (Parte C). **Delta** se ve en modo inspección + lectura sin Spark
(Parte D1) y **Hudi** de forma conceptual (Parte D2).

> ✅ Los comandos de este lab se ejecutaron end-to-end contra un proyecto real
> (2026-09-03). Donde el resultado real difería de lo esperado, el texto lo refleja.

---

### Objetivos

Al terminar podrás:

1. Exportar datos a CSV, Avro y Parquet y **comparar tamaño y comportamiento de lectura**.
2. Leer el **esquema y las estadísticas internas** de un archivo Parquet y el
   **esquema embebido** de un Avro.
3. Explicar qué problema resuelven Delta / Iceberg / Hudi frente a “Parquet suelto”.
4. Crear una **tabla Iceberg gestionada en BigQuery**, hacer `INSERT/UPDATE/DELETE/MERGE`,
   usar **time travel** y materializar/inspeccionar sus **metadatos** en Cloud Storage.
5. Inspeccionar el *transaction log* de **Delta Lake** y leerlo desde BigQuery.
6. Elegir el formato adecuado con un **árbol de decisión**.

### Prerrequisitos

- Proyecto de Google Cloud con facturación.
- APIs habilitadas: `bigquery.googleapis.com`, `bigqueryconnection.googleapis.com`,
  `storage.googleapis.com`.
- **Cloud Shell** (trae `gcloud`, `bq`, `python3`, `pip`, `jq`).
- En los bloques `sql`, reemplaza `TU_PROYECTO` por el ID de tu proyecto y `TU_BUCKET`
  por el **nombre del bucket sin `gs://`** (= `${PROJECT_ID}-lab03`). En los bloques
  `bash` se usan las variables de entorno del Paso 0.

### Costo estimado

| Concepto | En este lab | Costo |
|---|---|---|
| Consultas BigQuery | ≈ 25 GiB escaneados (5 `EXPORT DATA` de ~4 GiB + consultas) | Primer 1 TiB/mes gratis → **~$0** |
| Almacenamiento GCS | ≈ 3 GB (sobre todo el CSV sin comprimir), borrado al final | Capa gratuita → **~$0** |
| Cómputo | Ninguno (sin Dataproc/Spark) | **$0** |

Incluye paso de limpieza al final.

---

### Paso 0 — Preparar el entorno

```bash
export PROJECT_ID=$(gcloud config get-value project)
export LOCATION=US                       # multi-región, igual que bigquery-public-data
export BUCKET="gs://${PROJECT_ID}-lab03"
export DATASET="formatos_lab"

gcloud storage buckets create "$BUCKET" --location="$LOCATION"
bq --location="$LOCATION" mk --dataset \
  --description "Lab 03 - Formatos de archivo y de tabla" \
  "${PROJECT_ID}:${DATASET}"
```

> ⚠️ El dataset y el bucket deben estar en **`US`** para poder leer
> `bigquery-public-data` en la Parte A.

### Mapa del laboratorio

```
Parte A  Parquet vs Avro         EXPORT DATA → GCS → inspección → tabla externa
Parte B  ¿Por qué tablas abiertas?  Parquet suelto NO tiene ACID / UPDATE / time travel
Parte C  Iceberg gestionado       CREATE TABLE table_format='ICEBERG' → DML → time travel → EXPORT TABLE METADATA
Parte D  Delta (lectura) + Hudi (concepto)
Parte E  Decisión + limpieza + retos
```

---

## Parte A — Parquet vs Avro

Fuente: `bigquery-public-data.london_bicycles.cycle_hire` — **83,4 M filas, ~10 GB**.
Para que los `EXPORT DATA` sean rápidos y baratos trabajamos con la **rebanada de 2016**
(~13 M filas) y con un juego de **9 columnas**, incluidas dos de texto repetitivo
(`start_station_name`, `end_station_name`) que es donde el formato columnar más se nota.

### A1. Exportar la misma consulta a varios formatos y compresiones

Ejecuta en el editor de BigQuery (uno por uno). El `uri` **debe** contener un `*`.

```sql
-- 1) CSV SIN comprimir (referencia)
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/csv-raw/h-*.csv',
  format = 'CSV', header = true, overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, end_date,
       start_station_id, start_station_name, end_station_id, end_station_name
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_date >= '2016-01-01' AND start_date < '2017-01-01';
```

```sql
-- 2) CSV + GZIP
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/csv-gzip/h-*.csv.gz',
  format = 'CSV', compression = 'GZIP', header = true, overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, end_date,
       start_station_id, start_station_name, end_station_id, end_station_name
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_date >= '2016-01-01' AND start_date < '2017-01-01';
```

```sql
-- 3) Avro + DEFLATE (fila, binario, esquema embebido)
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/avro/h-*.avro',
  format = 'AVRO', compression = 'DEFLATE', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, end_date,
       start_station_id, start_station_name, end_station_id, end_station_name
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_date >= '2016-01-01' AND start_date < '2017-01-01';
```

```sql
-- 4) Parquet + SNAPPY (columnar, compresión rápida)
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/parquet-snappy/h-*.parquet',
  format = 'PARQUET', compression = 'SNAPPY', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, end_date,
       start_station_id, start_station_name, end_station_id, end_station_name
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_date >= '2016-01-01' AND start_date < '2017-01-01';
```

```sql
-- 5) Parquet + ZSTD (columnar, mejor ratio)
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/parquet-zstd/h-*.parquet',
  format = 'PARQUET', compression = 'ZSTD', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, end_date,
       start_station_id, start_station_name, end_station_id, end_station_name
FROM `bigquery-public-data.london_bicycles.cycle_hire`
WHERE start_date >= '2016-01-01' AND start_date < '2017-01-01';
```

Compara el tamaño total en disco de cada carpeta:

```bash
for f in csv-raw csv-gzip avro parquet-snappy parquet-zstd; do
  printf "%-16s " "$f"; gcloud storage du -s "$BUCKET/expA/$f"
done
```

**Resultado real (rebanada 2016, 9 columnas):**

| Formato | Tamaño aprox. |
|---|---:|
| CSV sin comprimir | **~1,37 GB** |
| Avro + DEFLATE | ~373 MB |
| CSV + GZIP | ~298 MB |
| Parquet + SNAPPY | ~298 MB |
| Parquet + ZSTD | **~257 MB** |

**Lo que enseña este resultado:**

1. **La compresión domina.** El salto grande es comprimir vs no comprimir (~1,37 GB → ~300 MB),
   no el contenedor.
2. Entre formatos comprimidos, las diferencias aquí son **modestas**: el **códec**
   (ZSTD/GZIP frente a SNAPPY) pesa tanto como elegir Parquet o Avro.
3. Parquet **no siempre es el archivo más pequeño**; con pocas columnas numéricas incluso
   puede empatar o quedar por encima de CSV+GZIP. **Su ventaja real es la lectura
   selectiva**, que se mide en A3.

### A2. Mirar los archivos por dentro

`EXPORT DATA` reparte la salida en muchos *shards* y **algunos salen vacíos** (0 filas).
Elige uno con datos (p. ej. el `-000000000001`, o el más grande):

```bash
pip install --quiet parquet-tools fastavro

BIG=$(gcloud storage ls -l "$BUCKET/expA/parquet-snappy/h-*.parquet" \
      | sort -n | tail -2 | head -1 | awk '{print $NF}')
gcloud storage cp "$BIG" /tmp/s.parquet

parquet-tools inspect /tmp/s.parquet     # esquema, num_row_groups, codec, min/max por columna
parquet-tools show --head 5 /tmp/s.parquet
```

Verás algo como: `num_rows: ~98000`, `num_row_groups: 1`, y por columna el códec y las
estadísticas, p. ej. `duration min=-3180 max=532920` (¡duraciones negativas! dato sucio
de la fuente) o `start_date min=2016-01-01 max=2016-12-31`.

```bash
A=$(gcloud storage ls -l "$BUCKET/expA/avro/h-*.avro" | sort -n | tail -2 | head -1 | awk '{print $NF}')
gcloud storage cp "$A" /tmp/a.avro
python3 - <<'PY'
import json, fastavro
with open('/tmp/a.avro', 'rb') as f:
    r = fastavro.reader(f)
    print("Esquema embebido en el header del archivo Avro:")
    print(json.dumps(r.writer_schema, indent=2))
    for i, rec in zip(range(3), r):
        print(rec)
PY
```

**Observa:**
- Parquet trae **row groups** y **estadísticas min/max por columna** → el motor puede
  saltarse bloques enteros (*predicate pushdown*) y leer **solo las columnas pedidas**.
- Avro trae el **esquema completo en el header** (autodescriptivo, pensado para
  evolución de esquema), pero es **por fila**: para leer una columna hay que leer la fila
  entera.

### A3. Leer cada formato desde BigQuery — la lectura selectiva

```sql
CREATE OR REPLACE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.hire_parquet`
OPTIONS (format = 'PARQUET', uris = ['gs://TU_BUCKET/expA/parquet-snappy/h-*.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.hire_avro`
OPTIONS (format = 'AVRO', uris = ['gs://TU_BUCKET/expA/avro/h-*.avro']);
```

Ejecuta una consulta que use **una sola columna** contra cada tabla:

```sql
SELECT AVG(duration) FROM `TU_PROYECTO.formatos_lab.hire_parquet`;   -- luego hire_avro
```

Para tablas externas, el validador muestra *“lower bound of 0 bytes”* (no puede estimar
antes de ejecutar). Lee el valor real en **Job information → Bytes processed** o con:

```sql
SELECT referenced_tables[SAFE_OFFSET(0)].table_id AS tabla,
       ROUND(total_bytes_processed / 1048576, 1)  AS mb
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 10 MINUTE)
  AND statement_type = 'SELECT' AND state = 'DONE'
ORDER BY creation_time DESC LIMIT 5;
```

**Resultado real:** `AVG(duration)` procesa **~78 MB sobre Parquet** y **~356 MB sobre
Avro** (~4,5×). Parquet lee solo la columna `duration`; Avro lee las 9 columnas de cada
fila. *Esta* es la ventaja de Parquet, no el tamaño en disco.

### A4. Resumen — fila vs columna

| | CSV | Avro | Parquet |
|---|---|---|---|
| Orientación | fila, texto | fila, binario | **columna**, binario |
| Esquema | externo | **embebido**, evoluciona | embebido |
| Compresión | depende 100 % del códec | media | alta con buen códec |
| Poda de columnas al leer | ❌ | ❌ | ✅ (**~4,5× menos bytes** en A3) |
| Pushdown por estadísticas | ❌ | ❌ | ✅ (row groups + min/max) |
| *Splittable* | ❌ (gzip) | ✅ | ✅ |
| Ideal para | intercambio simple | **ingesta / streaming / evolución de esquema / escritura registro a registro** | **analítica / lectura / escaneos selectivos** |

---

## Parte B — ¿Por qué existen los formatos de tabla abiertos?

Un directorio de archivos Parquet en un data lake **no** tiene:

- **commits atómicos** (un job que falla a medias deja archivos “sueltos”),
- **ACID** con varios escritores a la vez,
- `UPDATE` / `DELETE` / `MERGE` a nivel de registro,
- **time travel** (leer la tabla como estaba ayer),
- forma fiable de saber **qué archivos** componen la tabla *ahora* (listar carpetas es
  frágil y lento).

Delta Lake, Iceberg y Hudi resuelven exactamente eso: añaden una **capa de metadatos**
que registra, versión a versión, qué archivos Parquet forman la tabla.

| | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Metadatos | `_delta_log/` (JSON por línea + checkpoints Parquet) | árbol: *metadata.json → manifest list → manifests* | *timeline* en `.hoodie/` + índice de registros |
| Origen | Databricks | Netflix | Uber |
| Punto fuerte | lakehouse sobre Spark, `MERGE` batch, *time travel* | neutralidad entre motores, escala, **evolución de partición**, *hidden partitioning* | **upserts/deletes muy frecuentes**, ingesta near-real-time, consultas **incrementales / CDC** |
| Motores | Spark (nativo); lectura amplia | Spark, Flink, Trino, BigQuery, Snowflake… | Spark, Flink; lectura amplia |
| **En GCP** | **lectura** desde BigQuery (tabla externa BigLake `format='DELTA_LAKE'`, reader v3 con *deletion vectors* y *column mapping*; sin CDC). Escritura: Spark/Dataproc o `delta-rs` | **escritura nativa gestionada en BigQuery** (`table_format='ICEBERG'`): DML completo, transacciones ACID, time travel. Metadatos Iceberg estándar vía `EXPORT TABLE METADATA` o BigLake metastore | **lectura** desde BigQuery como tabla externa vía **archivo manifest** (`file_set_spec_type='NEW_LINE_DELIMITED_MANIFEST'`), COW y MOR *read-optimized* con partición hive-style. Escritura: Spark/Flink en Dataproc |

Los tres almacenan los **datos** como Parquet: se diferencian en **cómo gestionan los
metadatos y las mutaciones**.

---

## Parte C — Apache Iceberg gestionado en BigQuery (hands-on)

BigQuery puede **crear y escribir** tablas Iceberg: los datos y los metadatos Iceberg
viven en **tu bucket de GCS**, pero usas SQL de BigQuery como con cualquier tabla.

### C1. Conexión y permisos

```bash
bq mk --connection --location="$LOCATION" --connection_type=CLOUD_RESOURCE lab03_conn

# El service account se lee con el id de 3 partes y SIN la bandera --location
# (pasar --location con un id de 2 partes dispara un bug del CLI).
SA=$(bq show --format=prettyjson --connection "${PROJECT_ID}.us.lab03_conn" \
      | jq -r .cloudResource.serviceAccountId)
echo "SA de la conexión: $SA"

gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.objectUser"
gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.legacyBucketReader"

sleep 20   # dar tiempo a que propaguen los permisos IAM
```

> La conexión se crea con `--location=US` pero se referencia en SQL como
> `` `TU_PROYECTO.us.lab03_conn` `` (token de región en minúsculas).

### C2. Crear la tabla Iceberg

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO.formatos_lab.estaciones_ice` (
  station_id        INT64,
  nombre            STRING,
  bicis_disponibles INT64,
  actualizado       TIMESTAMP
)
WITH CONNECTION `TU_PROYECTO.us.lab03_conn`
OPTIONS (
  file_format  = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri  = 'gs://TU_BUCKET/iceberg/estaciones'
);
```

Si falla con *“…make sure gs://… is accessible via appropriate IAM roles”*, espera unos
segundos más (propagación de IAM) y reintenta.

### C3. DML: `INSERT` / `UPDATE` / `DELETE` / `MERGE`

```sql
INSERT INTO `TU_PROYECTO.formatos_lab.estaciones_ice` VALUES
  (1, 'Hyde Park',   15, CURRENT_TIMESTAMP()),
  (2, 'Waterloo',     3, CURRENT_TIMESTAMP()),
  (3, 'Kings Cross', 22, CURRENT_TIMESTAMP());

-- Ejecuta esto justo aquí y anota el valor (es el instante "3 filas"):
SELECT CURRENT_TIMESTAMP() AS t_tres_filas;

UPDATE `TU_PROYECTO.formatos_lab.estaciones_ice`
SET bicis_disponibles = 0, actualizado = CURRENT_TIMESTAMP()
WHERE station_id = 2;

DELETE FROM `TU_PROYECTO.formatos_lab.estaciones_ice`
WHERE station_id = 3;

MERGE `TU_PROYECTO.formatos_lab.estaciones_ice` T
USING UNNEST([STRUCT(1 AS station_id, 9 AS bicis),
              STRUCT(4 AS station_id, 12 AS bicis)]) S
ON T.station_id = S.station_id
WHEN MATCHED THEN
  UPDATE SET bicis_disponibles = S.bicis, actualizado = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
  INSERT (station_id, nombre, bicis_disponibles, actualizado)
  VALUES (S.station_id, 'Nueva', S.bicis, CURRENT_TIMESTAMP());

SELECT * FROM `TU_PROYECTO.formatos_lab.estaciones_ice` ORDER BY station_id;
```

Estado final: `1 → 9`, `2 → 0`, `4 → Nueva/12` (la fila 3 fue borrada). Cada sentencia
creó un **snapshot**.

### C4. Time travel

**Opción simple** (relativa) — funciona si han pasado >2 min desde `CREATE TABLE`:

```sql
SELECT * FROM `TU_PROYECTO.formatos_lab.estaciones_ice`
  FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 MINUTE)
ORDER BY station_id;
```

**Opción precisa** — pega el valor `t_tres_filas` que anotaste en C3 (la consola lo
muestra en **UTC**); debe ser posterior a `CREATE TABLE` y estar dentro de la ventana de
time travel (7 días):

```sql
SELECT * FROM `TU_PROYECTO.formatos_lab.estaciones_ice`
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-03 22:05:35+00'   -- ← reemplaza
ORDER BY station_id;
```

Con el instante entre el `INSERT` y el `UPDATE` verás las **3 filas originales**
(`1→15, 2→3, 3→22`): prueba de que cada DML dejó un snapshot consultable.

### C5. Materializar e inspeccionar los metadatos Iceberg

Tras el DML, el bucket solo tiene los **datos** y un `metadata/v0.metadata.json` inicial
(con `current-snapshot-id: -1`). BigQuery gestiona sus metadatos internamente; para
generar el **árbol Iceberg estándar** (que otros motores pueden leer) se ejecuta:

```sql
EXPORT TABLE METADATA FROM `TU_PROYECTO.formatos_lab.estaciones_ice`;
```

```bash
gcloud storage ls -r "$BUCKET/iceberg/estaciones/**"
```

Ahora aparece:

```
iceberg/estaciones/
├── data/
│   └── *.parquet                              ← los datos (un archivo por operación DML)
└── metadata/
    ├── v0.metadata.json                       ← inicial (snapshot -1)
    ├── v<timestamp>.metadata.json             ← estado actual: esquema + snapshots
    ├── <uuid>-f-manifest-list-00000-of-00001.avro   ← "manifest list" del snapshot
    ├── <uuid>-f-00000-of-00001.avro           ← "manifest": qué data files hay en el snapshot
    └── version-hint.text                      ← apunta a la versión de metadata vigente
```

```bash
gcloud storage cat "$BUCKET"/iceberg/estaciones/metadata/v1*.metadata.json \
  | python3 -m json.tool | head -n 60
```

Modelo mental: **metadata.json → snapshot → manifest list → manifests → data files**.
`FOR SYSTEM_TIME AS OF` = “usa el snapshot vigente en ese instante”.

### C6. (Opcional) leer la misma tabla desde otro motor

Ese árbol de `metadata/` es **Iceberg estándar**: un Spark/Trino/Flink con soporte
Iceberg (p. ej. en Dataproc), apuntando al `version-hint.text` / `metadata.json` del
bucket o al **catálogo BigLake metastore (REST)**, lee los mismos snapshots. Repite
`EXPORT TABLE METADATA` tras nuevos DML para refrescar. Es la prueba de que el formato es
abierto y no queda atado a BigQuery.

---

## Parte D — Delta Lake y Hudi

### D1. Delta Lake sin Spark (`delta-rs`) + lectura desde BigQuery

`deltalake` (delta-rs) escribe tablas Delta **sin Spark ni JVM**:

```bash
pip install --quiet "deltalake>=0.20" pandas pyarrow

python3 - <<'PY'
import pandas as pd, os, shutil
from deltalake import write_deltalake, DeltaTable

path = "/tmp/delta_estaciones"; shutil.rmtree(path, ignore_errors=True)

write_deltalake(path,
    pd.DataFrame({"station_id":[1,2,3],
                  "nombre":["Hyde Park","Waterloo","Kings Cross"],
                  "bicis":[15,3,22]}),
    mode="overwrite")                                  # commit 0

write_deltalake(path,
    pd.DataFrame({"station_id":[4], "nombre":["Angel"], "bicis":[7]}),
    mode="append")                                     # commit 1

dt = DeltaTable(path)
dt.delete("station_id = 3")                            # commit 2
dt = DeltaTable(path)
print("versión actual:", dt.version())
print(dt.to_pandas().sort_values("station_id").to_string(index=False))
print("--- _delta_log ---")
for f in sorted(os.listdir(path + "/_delta_log")): print(" ", f)
PY

# El log es JSON por línea (una acción por línea): usar cat, NO un pretty-printer
cat /tmp/delta_estaciones/_delta_log/00000000000000000002.json
```

Verás 3 commits (`00000000000000000000.json` … `..02.json`). El último contiene una
acción `remove` (el archivo con la fila 3) y `add` (archivo reescrito sin ella): *leer la
tabla* = reproducir el log hasta la última versión.

Súbela a GCS y créala como tabla externa BigLake:

```bash
gcloud storage cp --recursive /tmp/delta_estaciones "$BUCKET/delta/estaciones"
```

```sql
CREATE OR REPLACE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.estaciones_delta`
WITH CONNECTION `TU_PROYECTO.us.lab03_conn`
OPTIONS (format = 'DELTA_LAKE', uris = ['gs://TU_BUCKET/delta/estaciones']);

SELECT * FROM `TU_PROYECTO.formatos_lab.estaciones_delta` ORDER BY station_id;
```

**Resultado real:** BigQuery devuelve las estaciones `1, 2, 4` (la 3, borrada, no
aparece) → confirma que BigQuery lee la **última versión** reproduciendo el
`_delta_log`. Para *time travel* usas `delta-rs` (`DeltaTable(path, version=0)`) o Spark
(`VERSION AS OF`); la tabla externa de BigQuery siempre ve la versión más reciente.

### D2. Apache Hudi (conceptual — no se ejecuta en este lab)

Escribir una tabla Hudi necesita **Spark o Flink**, así que aquí solo se describe su forma
y cómo se leería desde BigQuery. Para practicarlo, usa el *quickstart* de Hudi en
**Dataproc Serverless**.

**Timeline (`.hoodie/`):** cada operación deja un *instant*: `<ts>.commit` (COW),
`<ts>.deltacommit` (MOR), `<ts>.clean`, `<ts>.rollback`, más `hoodie.properties`. Los
datos van en `<particion>/<fileId>_<token>_<ts>.parquet` (archivos base) y, en MOR,
ficheros `.log` con los cambios incrementales.

- **Copy-on-Write (COW):** cada `upsert` reescribe los archivos base afectados →
  **lectura rápida**, escritura más cara. Es lo más parecido a Delta/Iceberg.
- **Merge-on-Read (MOR):** los `upsert` se anexan a `.log` → **escritura barata**;
  la lectura *read-optimized* ve solo la base, la *real-time* fusiona base + log.
- **Consulta incremental:** `hoodie.datasource.query.type = incremental` +
  `begin.instanttime` → devuelve **solo lo que cambió** desde un commit (CDC nativo).

**Leer desde BigQuery:** el `BigQuerySyncTool` de Hudi
(`hoodie.gcp.bigquery.sync.use_bq_manifest_file = true`) genera un **archivo manifest**
con la lista de archivos base vigentes y crea una tabla externa así:

```sql
-- La produce BigQuerySyncTool; se muestra para entender qué genera
CREATE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.tabla_hudi`
WITH PARTITION COLUMNS
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://TU_BUCKET/hudi/tabla/.hoodie/manifest/*'],
  file_set_spec_type = 'NEW_LINE_DELIMITED_MANIFEST',
  hive_partition_uri_prefix = 'gs://TU_BUCKET/hudi/tabla/'
);
```

Así BigQuery escanea **solo** los archivos del manifest (no todo el directorio). Soportado:
**COW** y **MOR read-optimized** con partición *hive-style*.

---

## Parte E — Cuándo usar cada uno

```
¿Necesitas UPDATE/DELETE/MERGE, ACID o time travel sobre el lake?
├─ NO  → ¿escritura por eventos / evolución de esquema fuerte / registro a registro?
│        ├─ SÍ → Avro
│        └─ NO → Parquet
└─ SÍ  → ¿upserts/deletes muy frecuentes o ingesta near-real-time + CDC incremental?
         ├─ SÍ → Hudi
         └─ NO → ¿ecosistema Spark / Databricks?
                  ├─ SÍ → Delta Lake
                  └─ NO → Iceberg
                          (en GCP: gestionado por BigQuery — DML y time travel nativos,
                           neutral entre motores, evolución de partición sin reescribir)
```

Resumen:

- **Parquet** — dataset analítico estático o *append-only*, intercambio entre sistemas.
  Elige bien el **códec** (ZSTD/GZIP para archivo, SNAPPY para lecturas calientes).
- **Avro** — pipelines de ingesta, colas de eventos (Pub/Sub, Kafka), evolución de
  esquema intensa. *(La salida de Datastream a GCS es Avro.)*
- **Delta Lake** — lakehouse centrado en Spark/Databricks; ACID + `MERGE` + time travel
  mayormente batch. En GCP: escritura con Spark/`delta-rs`, lectura desde BigQuery.
- **Iceberg** — lakehouse abierto multi-motor, tablas grandes, evolución de
  partición/esquema sin reescribir, neutralidad de proveedor. **Opción de primera clase
  en GCP** (tablas gestionadas en BigQuery).
- **Hudi** — upserts/borrados a nivel de registro muy frecuentes, ingesta near-real-time,
  procesamiento incremental / CDC. En GCP: escritura con Spark/Flink en Dataproc, lectura
  desde BigQuery vía manifest.

### Limpieza

```sql
DROP SCHEMA IF EXISTS `TU_PROYECTO.formatos_lab` CASCADE;
```

```bash
bq rm --force --connection "${PROJECT_ID}.us.lab03_conn"
gcloud storage rm --recursive "$BUCKET"
```

### Retos

1. **Evolución de esquema en Iceberg:** `ALTER TABLE ... ADD COLUMN nueva STRING`,
   inserta filas, `EXPORT TABLE METADATA`, y consulta un snapshot anterior — ¿cómo se ve
   la columna nueva en los datos viejos?
2. **Códec vs contenedor:** repite A1 exportando Parquet con `SNAPPY`, `GZIP` y `ZSTD`;
   ¿cuánto cambia el tamaño solo por el códec?
3. **Time travel local en Delta:** `DeltaTable('/tmp/delta_estaciones', version=0).to_pandas()`.
   ¿Por qué la tabla externa de BigQuery solo ve la última versión?
4. **Interoperabilidad:** con Apache XTable, expón una misma tabla como Iceberg y Delta a
   la vez y léela desde dos motores.

---

## Referencias

- [BigQuery — Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery)
- [BigQuery — Export table metadata (Iceberg)](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery#export-metadata)
- [BigQuery — Create BigLake external tables for Delta Lake](https://docs.cloud.google.com/bigquery/docs/create-delta-lake-table)
- [BigQuery — Query open table formats with manifests (Hudi)](https://docs.cloud.google.com/bigquery/docs/query-open-table-format-using-manifest-files)
- [Apache Hudi — Google BigQuery integration](https://hudi.apache.org/docs/gcp_bigquery/)
- [BigQuery — EXPORT DATA statement](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/export-statements)
- [delta-rs (`deltalake` para Python)](https://delta-io.github.io/delta-rs/)
