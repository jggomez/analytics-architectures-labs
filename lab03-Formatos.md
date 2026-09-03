# Lab 03 — Formatos de archivo y formatos de tabla abiertos

## Codelab paso a paso

Laboratorio corto para **ver por dentro** los formatos de datos que se usan en un lakehouse
y entender cuándo elegir cada uno:

- **Formatos de archivo:** Parquet (columnar) y Avro (fila).
- **Formatos de tabla abiertos:** Delta Lake, Apache Iceberg y Apache Hudi (una capa de
  metadatos con ACID sobre archivos Parquet).

El *hands-on* de escritura se concentra en **Parquet/Avro** (Parte A) e **Iceberg
gestionado en BigQuery** (Parte C). **Delta** y **Hudi** se ven en modo
**inspección + lectura desde BigQuery** (Parte D), sin necesidad de Spark.

---

### Objetivos

Al terminar podrás:

1. Exportar datos a CSV, Avro y Parquet y **comparar tamaño y comportamiento de lectura**.
2. Leer el **esquema y las estadísticas internas** de un archivo Parquet y el
   **esquema embebido** de un Avro.
3. Explicar qué problema resuelven Delta / Iceberg / Hudi frente a “Parquet suelto”.
4. Crear una **tabla Iceberg gestionada en BigQuery**, hacer `INSERT/UPDATE/DELETE/MERGE`,
   usar **time travel** y leer sus **metadatos** en Cloud Storage.
5. Inspeccionar el *transaction log* de **Delta Lake** y la *timeline* de **Hudi**, y
   leer ambas desde BigQuery.
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
| Consultas BigQuery | < 10 GiB escaneados | Primer 1 TiB/mes gratis → **~$0** |
| Almacenamiento BigQuery + GCS | < 2 GB, borrado al final | Capa gratuita → **~$0** |
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
Parte C  Iceberg gestionado       CREATE TABLE ... table_format='ICEBERG' → DML → time travel → metadatos
Parte D  Delta + Hudi (lectura)   inspeccionar _delta_log / .hoodie  +  tabla externa BigLake
Parte E  Decisión + limpieza + retos
```

---

## Parte A — Parquet vs Avro

Usamos una tabla pública mediana: `bigquery-public-data.london_bicycles.cycle_hire`
(~24M filas de alquileres de bicis en Londres).

### A1. Exportar la misma consulta a tres formatos

Ejecuta en el editor de BigQuery (uno por uno). El `uri` **debe** contener un `*`.

```sql
-- CSV comprimido
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/csv/hire-*.csv.gz',
  format = 'CSV', compression = 'GZIP', header = true, overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, start_station_id, end_station_id
FROM `bigquery-public-data.london_bicycles.cycle_hire`;
```

```sql
-- Avro (fila, binario) con compresión DEFLATE
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/avro/hire-*.avro',
  format = 'AVRO', compression = 'DEFLATE', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, start_station_id, end_station_id
FROM `bigquery-public-data.london_bicycles.cycle_hire`;
```

```sql
-- Parquet (columnar) con SNAPPY
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/parquet-snappy/hire-*.parquet',
  format = 'PARQUET', compression = 'SNAPPY', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, start_station_id, end_station_id
FROM `bigquery-public-data.london_bicycles.cycle_hire`;
```

```sql
-- Parquet con GZIP (mejor ratio, más CPU)
EXPORT DATA OPTIONS(
  uri = 'gs://TU_BUCKET/expA/parquet-gzip/hire-*.parquet',
  format = 'PARQUET', compression = 'GZIP', overwrite = true
) AS
SELECT rental_id, duration, bike_id, start_date, start_station_id, end_station_id
FROM `bigquery-public-data.london_bicycles.cycle_hire`;
```

Compara el tamaño total en disco de cada carpeta:

```bash
for f in csv avro parquet-snappy parquet-gzip; do
  printf "%-16s " "$f"; gcloud storage du -s "$BUCKET/expA/$f"
done
```

**Qué esperar:** `CSV.gz` es el más grande; Avro/DEFLATE queda en medio; **Parquet es el
más pequeño** (codificación por columna + diccionarios), y GZIP < SNAPPY.

### A2. Mirar los archivos por dentro

```bash
pip install --quiet parquet-tools fastavro

gcloud storage cp "$BUCKET/expA/parquet-snappy/hire-000000000000.parquet" /tmp/s.parquet
parquet-tools inspect /tmp/s.parquet     # esquema, num_row_groups, codec, min/max por columna
parquet-tools show --head 5 /tmp/s.parquet
```

```bash
gcloud storage cp "$BUCKET/expA/avro/hire-000000000000.avro" /tmp/a.avro
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
  saltarse bloques enteros (*predicate pushdown*) y leer solo las columnas pedidas.
- Avro trae el **esquema completo en el header** (autodescriptivo, pensado para
  evolución de esquema), pero es **por fila**: para leer una columna hay que leer la fila
  entera.

### A3. Leer cada formato desde BigQuery

```sql
CREATE OR REPLACE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.hire_parquet`
OPTIONS (format = 'PARQUET', uris = ['gs://TU_BUCKET/expA/parquet-snappy/hire-*.parquet']);

CREATE OR REPLACE EXTERNAL TABLE `TU_PROYECTO.formatos_lab.hire_avro`
OPTIONS (format = 'AVRO', uris = ['gs://TU_BUCKET/expA/avro/hire-*.avro']);
```

Ejecuta la **misma** consulta contra cada una y compara **“Bytes processed”** en
*Job information*:

```sql
SELECT start_station_id, COUNT(*) AS viajes, AVG(duration) AS duracion_media
FROM `TU_PROYECTO.formatos_lab.hire_parquet`     -- luego cambia a hire_avro
WHERE start_station_id = 300
GROUP BY start_station_id;
```

**Resultado:** la consulta sobre Parquet lee muchos menos bytes (solo 2 columnas de los
*row groups* relevantes); sobre Avro lee prácticamente todos los datos.

### A4. Resumen — fila vs columna

| | CSV | Avro | Parquet |
|---|---|---|---|
| Orientación | fila, texto | fila, binario | **columna**, binario |
| Esquema | externo | **embebido**, evoluciona | embebido |
| Compresión típica | baja | media | **alta** |
| Poda de columnas | ❌ | ❌ | ✅ |
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
| Metadatos | `_delta_log/` (JSON + checkpoints Parquet) | árbol: *metadata.json → manifest list → manifests* | *timeline* en `.hoodie/` + índice de registros |
| Origen | Databricks | Netflix | Uber |
| Punto fuerte | lakehouse sobre Spark, `MERGE` batch, *time travel* | neutralidad entre motores, escala, **evolución de partición**, *hidden partitioning* | **upserts/deletes muy frecuentes**, ingesta near-real-time, consultas **incrementales / CDC** |
| Motores | Spark (nativo); lectura amplia | Spark, Flink, Trino, BigQuery, Snowflake… | Spark, Flink; lectura amplia |
| **En GCP** | **lectura** desde BigQuery (tabla externa BigLake `format='DELTA_LAKE'`, reader v3 con *deletion vectors* y *column mapping*; sin CDC). Escritura: Spark/Dataproc o `delta-rs` | **escritura nativa gestionada en BigQuery** (`table_format='ICEBERG'`): DML completo, transacciones ACID, time travel. También tablas externas + catálogo BigLake | **lectura** desde BigQuery como tabla externa vía **archivo manifest** (`file_set_spec_type='NEW_LINE_DELIMITED_MANIFEST'`), COW y MOR *read-optimized* con partición hive-style. Escritura: Spark/Flink en Dataproc |

Los tres almacenan los **datos** como Parquet: se diferencian en **cómo gestionan los
metadatos y las mutaciones**.

---

## Parte C — Apache Iceberg gestionado en BigQuery (hands-on)

BigQuery puede **crear y escribir** tablas Iceberg: los datos y los metadatos Iceberg
viven en **tu bucket de GCS**, pero usas SQL de BigQuery como con cualquier tabla.

### C1. Conexión y permisos

```bash
bq mk --connection --location="$LOCATION" --connection_type=CLOUD_RESOURCE lab03_conn

# Service account que BigQuery usa para tocar el bucket
SA=$(bq show --format=prettyjson --connection --location="$LOCATION" \
      "${PROJECT_ID}.lab03_conn" | jq -r .cloudResource.serviceAccountId)
echo "SA de la conexión: $SA"

gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.objectUser"
gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.legacyBucketReader"
```

> En SQL, la conexión se referencia como `` `TU_PROYECTO.REGION.lab03_conn` `` donde
> `REGION` es `$LOCATION` en minúsculas → aquí **`us`**.

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

### C3. DML: `INSERT` / `UPDATE` / `DELETE` / `MERGE`

```sql
INSERT INTO `TU_PROYECTO.formatos_lab.estaciones_ice` VALUES
  (1, 'Hyde Park',   15, CURRENT_TIMESTAMP()),
  (2, 'Waterloo',     3, CURRENT_TIMESTAMP()),
  (3, 'Kings Cross', 22, CURRENT_TIMESTAMP());

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

Cada sentencia crea un **snapshot** nuevo de la tabla.

### C4. Time travel

Antes de empezar C3, anota la hora. Luego, tras el `INSERT` pero antes del `DELETE`,
ejecuta un `SELECT CURRENT_TIMESTAMP()` y guarda ese valor: es el instante “con 3 filas”.

```sql
-- Reemplaza el literal por el timestamp que anotaste entre el INSERT y el DELETE.
-- Debe estar dentro de la ventana de time travel (7 días) y después de crear la tabla.
SELECT *
FROM `TU_PROYECTO.formatos_lab.estaciones_ice`
  FOR SYSTEM_TIME AS OF TIMESTAMP '2026-09-03 15:00:00+00'
ORDER BY station_id;
```

Deberías ver las 3 filas originales (con `station_id = 3` aún presente y `station_id = 2`
todavía en 3 bicis), demostrando que cada DML dejó un snapshot consultable.

### C5. Ver los metadatos Iceberg en GCS

```bash
gcloud storage ls -r "$BUCKET/iceberg/estaciones/**"
```

Estructura típica (los nombres exactos pueden variar):

```
iceberg/estaciones/
├── data/                 *.parquet   ← los datos
└── metadata/
    ├── v1.metadata.json  ...         ← estado de la tabla: esquema + lista de snapshots
    ├── snap-*.avro                   ← "manifest list": los manifests de un snapshot
    └── *.avro                        ← "manifests": qué archivos de data pertenecen al snapshot
```

```bash
gcloud storage cat "$BUCKET"/iceberg/estaciones/metadata/v*.metadata.json \
  | python3 -m json.tool | head -n 60
```

Modelo mental: **metadata.json → snapshot → manifest list → manifests → data files**.
`FOR SYSTEM_TIME AS OF` = “usa el snapshot vigente en ese instante”.

### C6. (Opcional) leer la misma tabla desde otro motor

El `storage_uri` contiene metadatos Iceberg **estándar**: un Spark/Trino/Flink con
soporte Iceberg (p. ej. en Dataproc), apuntando al catálogo BigLake REST o al mismo
prefijo de GCS, lee los **mismos snapshots**. Es la prueba de que el formato es abierto y
no queda atado a BigQuery.

---

## Parte D — Delta Lake y Hudi en GCP (lectura e inspección)

### D1. Delta Lake sin Spark (`delta-rs`) + lectura desde BigQuery

```bash
pip install --quiet "deltalake>=0.17" pandas pyarrow

python3 - <<'PY'
import pandas as pd
from deltalake import write_deltalake, DeltaTable

path = "/tmp/delta_estaciones"
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
print("versión actual:", dt.version())
print(dt.to_pandas().sort_values("station_id"))
PY

ls -1 /tmp/delta_estaciones/_delta_log/
cat /tmp/delta_estaciones/_delta_log/00000000000000000002.json   # acciones add / remove de archivos
```

**Observa:** `_delta_log/N.json` es **un commit**. Cada línea es una acción (`add`,
`remove`, `metaData`, `protocol`). “Leer la tabla” = reproducir el log hasta la última
versión.

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

BigQuery lee **la última versión** de la tabla Delta. Para *time travel* usas
`delta-rs` (`DeltaTable(path, version=0)`) o Spark (`VERSION AS OF`).

### D2. Apache Hudi: estructura e integración con BigQuery

Escribir una tabla Hudi necesita **Spark o Flink** (fuera del alcance de este lab; ver el
*quickstart* de Hudi en Dataproc Serverless). Lo importante aquí es entender su forma y
cómo se lee desde BigQuery.

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
con la lista de archivos base vigentes y crea una tabla externa como esta:

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

Así BigQuery escanea **solo** los archivos del manifest (no todo el directorio) y sigue
la evolución de esquema desde los metadatos de commit. Soportado: **COW** y **MOR
read-optimized** con partición *hive-style*.

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
bq rm --force --connection --location="$LOCATION" "${PROJECT_ID}.lab03_conn"
gcloud storage rm --recursive "$BUCKET"
```

### Retos

1. **Evolución de esquema en Iceberg:** `ALTER TABLE ... ADD COLUMN nueva STRING`,
   inserta filas, y consulta un snapshot anterior — ¿cómo se ve la columna nueva en los
   datos viejos?
2. **Iceberg vs Parquet plano:** exporta los mismos datos con `EXPORT DATA` a Parquet y
   compara el tamaño en disco frente a la tabla Iceberg.
3. **Time travel local en Delta:** `DeltaTable('/tmp/delta_estaciones', version=0).to_pandas()`.
   ¿Por qué la tabla externa de BigQuery solo ve la última versión?
4. **Interoperabilidad:** con Apache XTable, expón una misma tabla como Iceberg y Delta a
   la vez y léela desde dos motores.

---

## Referencias

- [BigQuery — Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery)
- [BigQuery — Query Apache Iceberg external tables](https://docs.cloud.google.com/bigquery/docs/query-iceberg-data)
- [BigQuery — Create BigLake external tables for Delta Lake](https://docs.cloud.google.com/bigquery/docs/create-delta-lake-table)
- [BigQuery — Query open table formats with manifests (Hudi/Iceberg)](https://docs.cloud.google.com/bigquery/docs/query-open-table-format-using-manifest-files)
- [Apache Hudi — Google BigQuery integration](https://hudi.apache.org/docs/gcp_bigquery/)
- [BigQuery — EXPORT DATA statement](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/export-statements)
- [Google Cloud Blog — Announcing BigQuery tables for Apache Iceberg](https://cloud.google.com/blog/products/data-analytics/announcing-bigquery-tables-for-apache-iceberg)
- [delta-rs (`deltalake` para Python)](https://delta-io.github.io/delta-rs/)
