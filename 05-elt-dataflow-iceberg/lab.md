# Lab 05 — ELT Batch con Dataflow: CDC (Datastream) + Bronze/Silver Iceberg + Datamart Gold

> 📖 **Marco Teórico:** Consulta la [Guía de ELT Programático con Apache Beam/Dataflow](teoria.md) para entender el modelo de programación de Beam, cómo Datastream entrega CDC como archivos y por qué este lab escribe Iceberg vía BigQuery en vez del conector nativo `IcebergIO`.
>
> Este lab da por conocidos el patrón CDC + Medallion del [Módulo 01](../01-patrones-y-modelado/lab.md) y las tablas Iceberg gestionadas en BigQuery del [Módulo 03](../03-formatos-y-lakehouse/lab.md).

---

## Codelab — Taller de 2 horas: "RetailFlow"

**RetailFlow** es un comercio minorista ficticio. Su sistema transaccional (PostgreSQL) tiene una tabla `orders` que cambia constantemente (nuevos pedidos, cambios de estado). Sus catálogos de `customers` y `products` llegan en cambio como **exports batch diarios en CSV**. El objetivo del taller es construir, con **Apache Beam corriendo en Dataflow**, un pipeline Bronze → Silver → Gold que termine en un **datamart de ventas en BigQuery** (`fact_sales`, `dim_customer`, `dim_product`).

```
ARQUITECTURA DEL LABORATORIO — RETAILFLOW

┌─────────────────────────┐        ┌──────────────────────────────┐
│ Cloud SQL (PostgreSQL)  │        │  GCS landing/ (CSV batch)     │
│ core.orders             │        │  customers.csv, products.csv │
└────────────┬─────────────┘        └───────────────┬────────────────┘
             │ Datastream (CDC, --backfill-all)      │
             ▼ Avro en GCS (bronze-cdc/orders/)       │
┌─────────────────────────────────────────────────────┴────────────┐
│         DATAFLOW · JOB BRONZE (bronze_pipeline.py)                 │
└───────────────────────────────┬────────────────────────────────────┘
                                 ▼
          BigQuery · dataset `bronze` (tablas Iceberg gestionadas)
                                 │
                                 ▼ DATAFLOW · JOB SILVER (silver_pipeline.py)
          BigQuery · dataset `silver` (tablas Iceberg gestionadas)
                                 │
                                 ▼ DATAFLOW · JOB GOLD (gold_pipeline.py)
          BigQuery · dataset `gold` (tablas nativas: fact_sales, dim_*)
                                 │
                                 ▼
                         Looker Studio / SQL
```

> [!NOTE]
> Los tres jobs de Dataflow usan **pipelines de Apache Beam en Python ya escritos** (los generas en el Paso 0 con `cat <<EOF`). El foco del taller es **correrlos, entender cada paso del código y ajustar 1-2 parámetros** — escribir un pipeline Beam completo desde cero no cabe en 2 horas.

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. Configurar un **stream de Datastream** que replica cambios de PostgreSQL hacia **Cloud Storage** en formato Avro (CDC como archivos, no como tabla).
2. Ejecutar un **pipeline de Apache Beam en Dataflow** que unifica un flujo CDC (Avro) con archivos batch (CSV) en la capa Bronze.
3. Deduplicar eventos CDC con `GroupByKey` y tipar datos en la capa Silver, escribiendo en **tablas Iceberg gestionadas por BigQuery**.
4. Construir un **datamart dimensional** (`fact_sales`, `dim_customer`, `dim_product`) en la capa Gold con un tercer job de Dataflow.
5. Explicar **por qué** las tablas Iceberg deben pre-crearse con SQL antes de que Dataflow escriba en ellas.
6. Identificar el **costo real** (no-$0) de Cloud SQL, Datastream, Dataflow y las cargas hacia tablas Iceberg gestionadas, y limpiar todos los recursos al finalizar.

### Prerrequisitos

- Proyecto de Google Cloud con facturación habilitada.
- **Todo este lab se ejecuta en Google Cloud Shell** (icono `>_` en la consola de GCP) — no necesitas instalar nada localmente. Cloud Shell ya trae `gcloud`, `bq`, `python3` y `pip`, y persiste tu `$HOME` entre sesiones (útil si el taller se interrumpe y retomas más tarde).
- Conocimientos básicos de SQL, Python y del patrón CDC/Medallion (Módulo 01).
- Todos los bloques `bash` de este lab están escritos para pegarse directamente en Cloud Shell y reutilizan las variables de entorno definidas en el Paso 0 (deben seguir exportadas en la misma sesión). En los bloques `sql` reemplaza `TU_PROYECTO_ID` y `TU_BUCKET` por tus valores reales.

### Costo Estimado (FinOps)

| Concepto | Recurso en el Lab | ¿Cubierto por capa gratuita? | Estimado |
|---|---|---|---|
| **Cloud SQL (PostgreSQL)** | 1 instancia `db-f1-micro`, ~1-2h de uso | ❌ No | **~$0.02–0.05** |
| **Datastream** | 1 stream CDC activo ~1h | ❌ No (se cobra por GB procesado) | **~$0.05–0.10** (volumen mínimo) |
| **Dataflow (3 jobs batch)** | Workers `n1-standard-1`, ~10-15 min cada job | ❌ No | **~$0.15–0.30** total |
| **Cargas hacia tablas Iceberg (Bronze/Silver)** | `LOAD JOB` con slots Enterprise pay-as-you-go (ver [teoría §6](teoria.md#6-costo-real-de-escribir-en-tablas-iceberg-gestionadas)) | ❌ No (a diferencia de tablas BigQuery normales) | **~$0.05–0.10** |
| **BigQuery Gold (tablas nativas) + consultas** | Storage + queries del datamart | ✅ Sí (1 TiB consultas / 10 GiB storage gratis al mes) | **~$0.00** |
| **Cloud Storage** | Buckets `landing/`, `bronze-cdc/`, metadata Iceberg | ✅ Sí (5 GiB/mes gratis) para este volumen | **~$0.00** |

> [!WARNING]
> A diferencia de los Módulos 02 y 03 (donde casi todo cabía en la capa gratuita), **este lab tiene un costo real de aproximadamente $0.30–0.60 USD** si lo completas en 1-2 horas y limpias los recursos al final. Cloud SQL y Datastream **siguen facturando mientras existan**, aunque no los estés usando activamente — por eso el Paso 6 (Limpieza) no es opcional.

---

### Mapa del Laboratorio (Taller de 120 minutos)

```
Bloque previo (10 min)  Contexto: arquitectura, por qué Beam/Dataflow, por qué Iceberg vía BigQuery
Paso 0  (15 min)  Aprovisionar: buckets, Cloud SQL + tabla orders, datasets BigQuery, conexión BigLake
Paso 1  (15 min)  Datastream: Cloud SQL (orders) -> GCS en Avro (CDC)
Paso 2  (25 min)  Dataflow BRONZE: CDC Avro + CSV landing -> tablas Iceberg Bronze
Paso 3  (25 min)  Dataflow SILVER: dedup + tipado + calidad -> tablas Iceberg Silver
Paso 4  (25 min)  Dataflow GOLD: fact_sales + dim_customer + dim_product -> BigQuery nativo
Paso 5  (10 min)  Consumo: SQL de negocio + gráfico en Looker Studio
Paso 6  (10 min)  Limpieza + Retos Opcionales
```

---

## Paso 0 — Aprovisionar el Entorno (15 min)

Crea un nuevo proyecto en GCP y usa el Cloud Shell 

### 0.1. Variables de entorno y recursos base

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1
export BUCKET_NAME="${PROJECT_ID}-lab05"
export BUCKET="gs://${BUCKET_NAME}"

gcloud services enable \
  sqladmin.googleapis.com \
  datastream.googleapis.com \
  dataflow.googleapis.com \
  bigqueryconnection.googleapis.com \
  compute.googleapis.com

gcloud storage buckets create "$BUCKET" --location="$REGION"

# Datasets de las 3 capas
bq --location="$REGION" mk --dataset --description "RetailFlow - Capa Bronze" "${PROJECT_ID}:bronze"
bq --location="$REGION" mk --dataset --description "RetailFlow - Capa Silver" "${PROJECT_ID}:silver"
bq --location="$REGION" mk --dataset --description "RetailFlow - Capa Gold"   "${PROJECT_ID}:gold"
```

### 0.2. Conexión BigLake (necesaria para las tablas Iceberg de Bronze/Silver)

Igual que en el [Módulo 03](../03-formatos-y-lakehouse/lab.md):

```bash
bq mk --connection --location="$REGION" --connection_type=CLOUD_RESOURCE lab05_conn

SA=$(bq show --format=prettyjson --connection "${PROJECT_ID}.${REGION}.lab05_conn" \
      | jq -r .cloudResource.serviceAccountId)
echo "Service account de la conexión: $SA"

sleep 30

gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.objectUser"
gcloud storage buckets add-iam-policy-binding "$BUCKET" \
  --member="serviceAccount:${SA}" --role="roles/storage.legacyBucketReader"

# El SA de la conexion (arriba) es quien lee/escribe en GCS EN NOMBRE de BigQuery.
# Aparte de eso, quien SUBMITEA el LOAD JOB (el worker de Dataflow, que corre como
# el SA de Compute Engine por defecto) necesita permiso para USAR la conexion:
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.connectionUser"

sleep 30
```

> [!WARNING]
> Sin el último `add-iam-policy-binding` de arriba, los Pasos 2 y 3 (Dataflow escribiendo hacia `bronze.*`/`silver.*`) fallan con `Access Denied: ... User does not have bigquery.connections.delegate permission for connection ...`. Es un permiso distinto y fácil de olvidar: el SA de la conexión necesita acceso a **GCS** (lo de arriba); el SA que **ejecuta el job de Dataflow** necesita permiso para **usar la conexión misma** (`roles/bigquery.connectionUser`, que incluye `bigquery.connections.delegate`). Si usaste una cuenta de servicio custom para los workers de Dataflow (`--service_account_email`), otorga este rol a esa cuenta en vez de al SA de Compute Engine por defecto.

### 0.3. Cloud SQL (PostgreSQL) con logical decoding habilitado

```bash
gcloud sql instances create retailflow-pg \
    --database-version=POSTGRES_15 \
    --tier=db-f1-micro \
    --region="$REGION" \
    --root-password="TuPasswordSeguro123!" \
    --database-flags=cloudsql.logical_decoding=on \
    --storage-size=10

gcloud sql databases create retailflow_db --instance=retailflow-pg

# Solo para el laboratorio: abre el acceso para que Datastream pueda alcanzar la instancia
gcloud sql instances patch retailflow-pg --authorized-networks=0.0.0.0/0
```

> [!WARNING]
> `--authorized-networks=0.0.0.0/0` deja la instancia abierta a internet con autenticación por contraseña. Es aceptable **solo** en este laboratorio aislado — nunca lo repliques en un proyecto con datos reales; usa Private Service Connect o un rango de IPs restringido.

### 0.4. Esquema, tabla y datos iniciales de `orders`

Conéctate con `gcloud sql connect retailflow-pg --user=postgres --database=retailflow_db` y ejecuta:

```sql
CREATE SCHEMA IF NOT EXISTS core;

CREATE TABLE core.orders (
    order_id     VARCHAR(20) PRIMARY KEY,
    customer_id  VARCHAR(20) NOT NULL,
    product_id   VARCHAR(20) NOT NULL,
    quantity     INT NOT NULL,
    unit_price   NUMERIC(10,2) NOT NULL,
    status       VARCHAR(20) NOT NULL,
    order_ts     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO core.orders (order_id, customer_id, product_id, quantity, unit_price, status, order_ts) VALUES
('ORD-0001','CUST-01','PROD-01', 2,  45.00, 'COMPLETED', CURRENT_TIMESTAMP - INTERVAL '3 hours'),
('ORD-0002','CUST-02','PROD-02', 1, 899.00, 'COMPLETED', CURRENT_TIMESTAMP - INTERVAL '2 hours 30 minutes'),
('ORD-0003','CUST-01','PROD-03', 3,  25.50, 'COMPLETED', CURRENT_TIMESTAMP - INTERVAL '2 hours'),
('ORD-0004','CUST-03','PROD-01', 1,  45.00, 'PENDING',   CURRENT_TIMESTAMP - INTERVAL '1 hour'),
('ORD-0005','CUST-04','PROD-04', 5,  12.00, 'COMPLETED', CURRENT_TIMESTAMP - INTERVAL '30 minutes');

-- Preparar la replicación lógica para Datastream
ALTER USER postgres WITH REPLICATION;
CREATE USER datastream_user WITH REPLICATION ENCRYPTED PASSWORD 'StreamSecret2026!';
GRANT cloudsqlsuperuser TO datastream_user;
GRANT USAGE ON SCHEMA core TO datastream_user;
GRANT SELECT ON ALL TABLES IN SCHEMA core TO datastream_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA core GRANT SELECT ON TABLES TO datastream_user;

CREATE PUBLICATION datastream_pub FOR ALL TABLES;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('datastream_slot', 'pgoutput');
```

### 0.5. Archivos CSV de `landing/` (customers y products)

```bash
cat <<'EOF' > customers.csv
customer_id,name,city,segment
CUST-01,Laura Gómez,Bogotá,Gold
CUST-02,Andrés Ruiz,Medellín,Silver
CUST-03,Marta Díaz,Cali,Bronze
CUST-04,Jorge Salazar,Barranquilla,Silver
CUST-05,Camila Ortiz,Bogotá,Gold
EOF

cat <<'EOF' > products.csv
product_id,name,category,unit_cost
PROD-01,Auriculares Bluetooth,Electronics,45.00
PROD-02,Laptop 14 pulgadas,Electronics,899.00
PROD-03,Mouse Ergonómico,Accessories,25.50
PROD-04,Cable USB-C 2m,Accessories,12.00
PROD-05,Teclado Mecánico,Electronics,120.00
EOF

gcloud storage cp customers.csv products.csv "$BUCKET/landing/"
```

### 0.6. Entorno Python para Beam/Dataflow

> [!NOTE]
> Ejecuta esto en **Cloud Shell** (no necesitas nada instalado localmente). Cloud Shell ya trae `python3`, `pip`, `gcloud` y `bq` preinstalados.

¿Para qué sirve este paso? Los tres jobs de Dataflow (`bronze_pipeline.py`, `silver_pipeline.py`, `gold_pipeline.py`) son **scripts de Apache Beam**: cuando los corres con `python3 ..._pipeline.py --runner=DataflowRunner ...` (Pasos 2, 3 y 4), el proceso que arranca en Cloud Shell **no es solo un cliente de línea de comandos** — usa el SDK de Beam instalado localmente para construir el grafo del pipeline, empaquetarlo (*staging*) y enviarlo a la API de Dataflow, que luego lo ejecuta en workers remotos. Por eso Cloud Shell necesita el paquete `apache-beam[gcp]` instalado.

> [!WARNING]
> El `$HOME` de Cloud Shell tiene un disco persistente de **solo 5 GB, y no se puede ampliar**. `apache-beam[gcp]` arrastra un árbol de dependencias pesado (`grpcio`, `pyarrow`, `protobuf`, `numpy`...) que puede agotarlo fácilmente, sobre todo si ya tienes otros proyectos ocupando espacio ahí — y un `cat <<EOF > archivo.json` que se ejecuta con el disco lleno **escribe un archivo vacío o truncado sin avisar claramente**, lo que luego provoca errores confusos en pasos posteriores (por ejemplo, `gcloud datastream streams create` fallando con un críptico `None is not of type 'object'` porque el JSON que le pasaste quedó corrupto). Por eso este lab crea el entorno virtual en **`/tmp`** en vez de `$HOME`: `/tmp` vive en el disco efímero de la VM de Cloud Shell (mucho más grande) y no cuenta contra esa cuota de 5 GB. El costo es que `/tmp` no sobrevive un reinicio completo de la VM de Cloud Shell (si eso pasa, solo repites este paso — no pierdes nada del resto del lab, que sí vive en GCS/BigQuery).

```bash
df -h "$HOME"   # solo para que veas cuánto te queda del disco de 5GB

python3 -m venv /tmp/lab05-venv
source /tmp/lab05-venv/bin/activate
pip install --quiet --no-cache-dir "apache-beam[gcp]==2.76.0"
```

> Si en algún momento del taller `$HOME` se queda sin espacio (por los CSV, los JSON de configuración, etc.), libera con `rm -rf ~/.cache/pip` — pero el venv pesado ya no vive ahí, así que no debería volver a pasar.

> [!IMPORTANT]
> Los Pasos 2.3, 3.3 y 4.2 (`python3 bronze_pipeline.py ...`, `python3 silver_pipeline.py ...`, `python3 gold_pipeline.py ...`) asumen que este entorno virtual sigue **activo en la misma sesión de Cloud Shell**. Si cierras la pestaña o se desconecta pero es la misma VM, vuelve a correr `source /tmp/lab05-venv/bin/activate`. Si Cloud Shell te asignó una VM nueva (por ejemplo, tras mucho tiempo de inactividad), `/tmp/lab05-venv` ya no existe — repite este Paso 0.6 completo.

---

## Paso 1 — Datastream: CDC de `orders` hacia Cloud Storage (15 min)

### 1.1. Perfiles de conexión (origen PostgreSQL, destino GCS)

```bash
gcloud datastream connection-profiles create pg-source-profile \
    --location="$REGION" \
    --type=postgresql \
    --display-name="RetailFlow PostgreSQL Origen" \
    --postgresql-hostname="$(gcloud sql instances describe retailflow-pg --format='value(ipAddresses[0].ipAddress)')" \
    --postgresql-port=5432 \
    --postgresql-username=datastream_user \
    --postgresql-password="StreamSecret2026!" \
    --postgresql-database=retailflow_db

gcloud datastream connection-profiles create gcs-dest-profile \
    --location="$REGION" \
    --type=google-cloud-storage \
    --display-name="RetailFlow GCS Destino Bronze CDC" \
    --bucket="$BUCKET_NAME" \
    --root-path="/bronze-cdc/orders"
```

### 1.2. Configuración y creación del stream

```bash
cat <<'EOF' > pg_source_config.json
{
  "includeObjects": {
    "postgresqlSchemas": [
      {
        "schema": "core",
        "postgresqlTables": [ { "table": "orders" } ]
      }
    ]
  },
  "replicationSlot": "datastream_slot",
  "publication": "datastream_pub"
}
EOF

cat <<'EOF' > gcs_dest_config.json
{
  "fileRotationMb": 5,
  "fileRotationInterval": "15s",
  "avroFileFormat": {}
}
EOF

gcloud datastream streams create orders-to-gcs-stream \
    --location="$REGION" \
    --display-name="RetailFlow orders -> GCS (CDC)" \
    --source=pg-source-profile \
    --destination=gcs-dest-profile \
    --postgresql-source-config=pg_source_config.json \
    --gcs-destination-config=gcs_dest_config.json \
    --backfill-all
```

### 1.3. Iniciar el stream

Los streams se crean en estado `NOT_STARTED`; hay que arrancarlos explícitamente:

```bash
gcloud datastream streams update orders-to-gcs-stream \
    --location="$REGION" \
    --state=RUNNING \
    --update-mask=state

# Verifica el estado (pasa por STARTING -> RUNNING en 1-2 minutos)
gcloud datastream streams describe orders-to-gcs-stream \
    --location="$REGION" --format="value(state)"
```

> [!NOTE]
> Mientras el stream pasa a `RUNNING` y hace el backfill inicial (1-3 min), sigue leyendo el Paso 2 — no necesitas esperar con los brazos cruzados.

### 1.4. Verificar el backfill y simular CDC en vivo

```bash
gcloud storage ls -r "$BUCKET/bronze-cdc/orders/**"
```

Deberías ver algo como `bronze-cdc/orders/core_orders/2026/09/19/15/33/<archivo>.avro`. Esa estructura de carpetas (`[esquema]_[tabla]/yyyy/mm/dd/hh/mm/`) es **automática y obligatoria** en Datastream cuando el destino es Cloud Storage — no la configuraste tú y no se puede desactivar. `core_orders` es el esquema `core` + la tabla `orders` (separados por `_`); el `yyyy/mm/dd/hh/mm` es la hora del **evento en el origen** (para el backfill, cuándo se leyó de Postgres; para CDC, cuándo cambió la fila) — no la hora en que Datastream escribió el archivo. Esto es importante para el Paso 2: el job Bronze **debe** leer con un patrón recursivo (`**/*.avro`), no `*.avro`, porque un solo `*` en Beam no cruza niveles de carpeta y nunca encontraría estos archivos.

Ahora, para demostrar CDC real (no solo el backfill), vuelve a `gcloud sql connect retailflow-pg --user=postgres --database=retailflow_db` y ejecuta:

```sql
-- Un pedido cambia de estado (UPDATE) y llega uno nuevo (INSERT)
UPDATE core.orders SET status = 'SHIPPED', order_ts = CURRENT_TIMESTAMP WHERE order_id = 'ORD-0004';

INSERT INTO core.orders (order_id, customer_id, product_id, quantity, unit_price, status, order_ts)
VALUES ('ORD-0006','CUST-05','PROD-05', 1, 120.00, 'PENDING', CURRENT_TIMESTAMP);
```

En 15-30 segundos aparecerá un nuevo archivo Avro en `bronze-cdc/orders/` con estos dos eventos. **`ORD-0004` ahora tiene dos eventos en Bronze** (el backfill inicial y el `UPDATE`): esto es exactamente lo que el job Silver del Paso 3 deduplicará.

---

## Paso 2 — Dataflow: Job BRONZE (25 min)

### 2.1. Crear las tablas Iceberg gestionadas de Bronze

Ejecuta en el editor SQL de BigQuery (reemplaza `TU_PROYECTO_ID` y `TU_BUCKET`):

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO_ID.bronze.orders` (
  order_id          STRING,
  customer_id       STRING,
  product_id        STRING,
  quantity          STRING,
  unit_price        STRING,
  status            STRING,
  order_ts          STRING,
  change_type       STRING,
  is_deleted        BOOL,
  source_timestamp  STRING
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/bronze/orders');

CREATE OR REPLACE TABLE `TU_PROYECTO_ID.bronze.customers` (
  customer_id STRING, name STRING, city STRING, segment STRING
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/bronze/customers');

CREATE OR REPLACE TABLE `TU_PROYECTO_ID.bronze.products` (
  product_id STRING, name STRING, category STRING, unit_cost STRING
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/bronze/products');
```

> [!NOTE]
> Bronze guarda **todo como `STRING`** salvo los metadatos de CDC. Es la filosofía "raw": no se pierde información por una conversión de tipo fallida en el peor momento. El tipado ocurre recién en Silver (Paso 3) — igual que hacía el Módulo 01 al separar `core_usuarios` (raw) de `stg_usuarios` (tipada).

> [!WARNING]
> Este job **no es idempotente**: cada vez que lo corres, `ReadFromAvro`/`ReadFromText` vuelven a leer **todos** los archivos que existan en ese momento (Datastream nunca borra los `.avro` ya leídos), y como escribe con `WRITE_APPEND`, una segunda corrida duplica cada fila de la primera. Para el flujo normal del taller (correr Bronze una sola vez, después de que ya está todo el CDC/CSV en su lugar) esto no es un problema. Si necesitas **reintentar** este job (por ejemplo, después de un error a mitad de corrida), vuelve a ejecutar el `CREATE OR REPLACE TABLE` del Paso 2.1 para vaciar las tablas Bronze antes de correr `bronze_pipeline.py` de nuevo — así evitas duplicados en cascada hacia Silver y Gold.

### 2.2. El pipeline de Beam

```bash
cat <<'EOF' > bronze_pipeline.py
"""Job BRONZE: unifica el CDC de orders (Avro) con customers/products (CSV)."""
import argparse
import csv
import io

import apache_beam as beam
from apache_beam.io.avroio import ReadFromAvro
from apache_beam.io.textio import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions


def parse_order_event(event):
    payload = event["payload"]
    meta = event.get("source_metadata") or {}
    return {
        "order_id": str(payload["order_id"]),
        "customer_id": str(payload["customer_id"]),
        "product_id": str(payload["product_id"]),
        "quantity": str(payload["quantity"]),
        "unit_price": str(payload["unit_price"]),
        "status": str(payload["status"]),
        "order_ts": str(payload["order_ts"]),
        "change_type": str(meta.get("change_type", "BACKFILL")),
        "is_deleted": bool(meta.get("is_deleted", False)),
        "source_timestamp": str(event["source_timestamp"]),
    }


def parse_csv_line(line, fieldnames):
    reader = csv.DictReader(io.StringIO(line), fieldnames=fieldnames)
    return next(reader)


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--orders_avro", required=True)
    parser.add_argument("--customers_csv", required=True)
    parser.add_argument("--products_csv", required=True)
    parser.add_argument("--bronze_orders_table", required=True)
    parser.add_argument("--bronze_customers_table", required=True)
    parser.add_argument("--bronze_products_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "LeerCDCOrders" >> ReadFromAvro(known_args.orders_avro)
            | "ParsearEventoOrder" >> beam.Map(parse_order_event)
            | "EscribirBronzeOrders" >> WriteToBigQuery(
                known_args.bronze_orders_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        customer_fields = ["customer_id", "name", "city", "segment"]
        (
            p
            | "LeerCustomersCSV" >> ReadFromText(known_args.customers_csv, skip_header_lines=1)
            | "ParsearCustomers" >> beam.Map(parse_csv_line, fieldnames=customer_fields)
            | "EscribirBronzeCustomers" >> WriteToBigQuery(
                known_args.bronze_customers_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        product_fields = ["product_id", "name", "category", "unit_cost"]
        (
            p
            | "LeerProductsCSV" >> ReadFromText(known_args.products_csv, skip_header_lines=1)
            | "ParsearProducts" >> beam.Map(parse_csv_line, fieldnames=product_fields)
            | "EscribirBronzeProducts" >> WriteToBigQuery(
                known_args.bronze_products_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
EOF
```

### 2.3. Ejecutar el job en Dataflow

```bash
python3 bronze_pipeline.py \
  --orders_avro="$BUCKET/bronze-cdc/orders/**/*.avro" \
  --customers_csv="$BUCKET/landing/customers.csv" \
  --products_csv="$BUCKET/landing/products.csv" \
  --bronze_orders_table="${PROJECT_ID}:bronze.orders" \
  --bronze_customers_table="${PROJECT_ID}:bronze.customers" \
  --bronze_products_table="${PROJECT_ID}:bronze.products" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --temp_location="$BUCKET/tmp" \
  --staging_location="$BUCKET/staging" \
  --runner=DataflowRunner \
  --job_name="retailflow-bronze-$(date +%s)"
```

Sigue el progreso en **Cloud Console → Dataflow → Jobs**. Con este volumen de datos debería tardar 5-8 minutos en aprovisionar workers y terminar.

### 2.4. Validar

```sql
SELECT change_type, is_deleted, count(*) FROM `TU_PROYECTO_ID.bronze.orders` GROUP BY 1, 2;
SELECT count(*) FROM `TU_PROYECTO_ID.bronze.customers`;
SELECT count(*) FROM `TU_PROYECTO_ID.bronze.products`;
```

Deberías ver **7 eventos** en `bronze.orders` (5 del backfill inicial + el `UPDATE` de `ORD-0004` + el `INSERT` de `ORD-0006`), 5 clientes y 5 productos.

### 2.5. En producción, esto no se haría así

El job que acabas de correr **relee todos los archivos cada vez** y **no es idempotente** (Paso 2.2, nota de advertencia). Es una simplificación deliberada para caber en 2 horas — en un pipeline real de producción, este mismo patrón (Datastream → GCS → Dataflow batch) se resolvería con alguna de estas técnicas:

1. **Mover o archivar lo ya procesado:** al terminar de leer un archivo, moverlo de `bronze-cdc/orders/...` a un prefijo como `bronze-cdc/orders-processed/...` (o borrarlo). El siguiente `glob` solo ve archivos nuevos.
2. **Watermark / tabla de control:** guardar en algún lado (una tabla chica en BigQuery, un archivo de estado) el último `source_timestamp` procesado, y filtrar cada corrida con `WHERE source_timestamp > ultimo_watermark`. Es el equivalente a un modelo incremental de Dataform/dbt.
3. **Convertir Bronze en streaming real (unbounded), no batch:** configurar una notificación de Cloud Storage vía Pub/Sub que se dispare cada vez que Datastream escribe un archivo nuevo, y correr Dataflow como pipeline *unbounded* consumiendo esa notificación — cada archivo se procesa exactamente una vez, apenas llega. Es la distinción *bounded vs. unbounded* de [teoría §2.1](teoria.md#21-conceptos-clave): el mismo código Beam puede adaptarse a este modo.
4. **Escrituras idempotentes con `MERGE` en vez de `WRITE_APPEND` ciego:** escribir por clave natural (`order_id`) con un `MERGE` (DML, sí soportado en tablas Iceberg gestionadas — a diferencia del `LOAD JOB` de `WriteToBigQuery`, que solo permite `WRITE_APPEND`). Si el mismo evento se reprocesa por error, el `MERGE` solo actualiza la fila; no la duplica.
5. **Orquestación con estado:** un DAG de Cloud Composer/Airflow que sabe qué rango de tiempo ya procesó, en vez de que una persona corra el script a mano.

Cualquiera de estas cinco cosas es tema de un módulo de "pipelines de producción", no de este taller introductorio — pero vale la pena saber que existen antes de llevar este patrón a un entorno real.

---

## Paso 3 — Dataflow: Job SILVER (25 min)

### 3.1. Crear las tablas Iceberg de Silver (ya tipadas)

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO_ID.silver.orders` (
  order_id STRING, customer_id STRING, product_id STRING,
  quantity INT64, unit_price FLOAT64, status STRING, order_ts STRING
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/silver/orders');

CREATE OR REPLACE TABLE `TU_PROYECTO_ID.silver.customers` (
  customer_id STRING, name STRING, city STRING, segment STRING
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/silver/customers');

CREATE OR REPLACE TABLE `TU_PROYECTO_ID.silver.products` (
  product_id STRING, name STRING, category STRING, unit_cost FLOAT64
)
WITH CONNECTION `TU_PROYECTO_ID.us-central1.lab05_conn`
OPTIONS (file_format = 'PARQUET', table_format = 'ICEBERG', storage_uri = 'gs://TU_BUCKET/iceberg/silver/products');
```

> [!WARNING]
> Las cargas batch (`LOAD JOB`, el método que usa `WriteToBigQuery` por defecto) hacia una tabla Iceberg gestionada **solo soportan `WRITE_APPEND`** — BigQuery rechaza `WRITE_TRUNCATE` en este tipo de tabla vía carga batch. Por eso el pipeline de abajo usa `WRITE_APPEND` y confía en que el Paso 3.1 ya recreó las tablas Silver vacías con `CREATE OR REPLACE TABLE`: en una sola corrida, "append a una tabla vacía" logra el mismo resultado que un truncate+load. Si necesitas volver a correr el job sin repetir el 3.1, ejecuta antes `DELETE FROM silver.orders WHERE TRUE` (DML sí soportado) para vaciarla.

### 3.2. El pipeline de deduplicación

```bash
cat <<'EOF' > silver_pipeline.py
"""Job SILVER: deduplica el CDC de orders (queda el evento mas reciente por order_id),
descarta borrados y tipa los tres datasets."""
import argparse

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions


def keyed_by_order_id(row):
    return (row["order_id"], row)


def ultimo_evento_por_orden(kv):
    # kv = (order_id, [lista de eventos de ESA orden]); el agrupado ya fue por order_id.
    # Aqui elegimos, DENTRO del grupo, cual evento es el vigente: el de source_timestamp mas alto.
    _, eventos = kv
    return max(eventos, key=lambda e: e["source_timestamp"])


def tipar_orden(row):
    return {
        "order_id": row["order_id"],
        "customer_id": row["customer_id"],
        "product_id": row["product_id"],
        "quantity": int(row["quantity"]),
        "unit_price": float(row["unit_price"]),
        "status": row["status"],
        "order_ts": row["order_ts"],
    }


def tipar_customer(row):
    return {
        "customer_id": row["customer_id"],
        "name": row["name"],
        "city": row["city"],
        "segment": row["segment"],
    }


def tipar_product(row):
    return {
        "product_id": row["product_id"],
        "name": row["name"],
        "category": row["category"],
        "unit_cost": float(row["unit_cost"]),
    }


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--bronze_orders_table", required=True)
    parser.add_argument("--bronze_customers_table", required=True)
    parser.add_argument("--bronze_products_table", required=True)
    parser.add_argument("--silver_orders_table", required=True)
    parser.add_argument("--silver_customers_table", required=True)
    parser.add_argument("--silver_products_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "LeerBronzeOrders" >> ReadFromBigQuery(table=known_args.bronze_orders_table)
            | "ClavePorOrderId" >> beam.Map(keyed_by_order_id)
            | "AgruparPorOrderId" >> beam.GroupByKey()
            | "QuedarseConElUltimoEvento" >> beam.Map(ultimo_evento_por_orden)
            | "DescartarBorrados" >> beam.Filter(lambda e: not e["is_deleted"])
            | "TiparOrdenes" >> beam.Map(tipar_orden)
            | "EscribirSilverOrders" >> WriteToBigQuery(
                known_args.silver_orders_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        (
            p
            | "LeerBronzeCustomers" >> ReadFromBigQuery(table=known_args.bronze_customers_table)
            | "FiltrarCustomersValidos" >> beam.Filter(lambda c: c.get("customer_id"))
            | "TiparCustomers" >> beam.Map(tipar_customer)
            | "EscribirSilverCustomers" >> WriteToBigQuery(
                known_args.silver_customers_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )

        (
            p
            | "LeerBronzeProducts" >> ReadFromBigQuery(table=known_args.bronze_products_table)
            | "FiltrarProductsValidos" >> beam.Filter(lambda pr: pr.get("product_id"))
            | "TiparProducts" >> beam.Map(tipar_product)
            | "EscribirSilverProducts" >> WriteToBigQuery(
                known_args.silver_products_table,
                create_disposition=BigQueryDisposition.CREATE_NEVER,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
            )
        )


if __name__ == "__main__":
    run()
EOF
```

### 3.3. Ejecutar

```bash
python3 silver_pipeline.py \
  --bronze_orders_table="${PROJECT_ID}:bronze.orders" \
  --bronze_customers_table="${PROJECT_ID}:bronze.customers" \
  --bronze_products_table="${PROJECT_ID}:bronze.products" \
  --silver_orders_table="${PROJECT_ID}:silver.orders" \
  --silver_customers_table="${PROJECT_ID}:silver.customers" \
  --silver_products_table="${PROJECT_ID}:silver.products" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --temp_location="$BUCKET/tmp" \
  --staging_location="$BUCKET/staging" \
  --runner=DataflowRunner \
  --job_name="retailflow-silver-$(date +%s)"
```

### 3.4. Validar la deduplicación

```sql
SELECT * FROM `TU_PROYECTO_ID.silver.orders` ORDER BY order_id;
```

Debes ver **exactamente 6 filas** (`ORD-0001` a `ORD-0006`), y `ORD-0004` con `status = 'SHIPPED'` (el evento más reciente ganó, no el backfill original con `PENDING`).

---

## Paso 4 — Dataflow: Job GOLD (25 min)

Gold usa **tablas BigQuery nativas** (no Iceberg): es la capa de consumo para BI, no necesita la evolución de esquema ni el time travel de Iceberg — solo lectura rápida y barata.

### 4.1. El pipeline dimensional

```bash
cat <<'EOF' > gold_pipeline.py
"""Job GOLD: calcula fact_sales y publica dim_customer / dim_product."""
import argparse

import apache_beam as beam
from apache_beam.io.gcp.bigquery import ReadFromBigQuery, WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import PipelineOptions

FACT_SALES_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "quantity", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "unit_price", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "revenue", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "status", "type": "STRING", "mode": "NULLABLE"},
        {"name": "order_ts", "type": "STRING", "mode": "NULLABLE"},
    ]
}

DIM_CUSTOMER_SCHEMA = {
    "fields": [
        {"name": "customer_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "segment", "type": "STRING", "mode": "NULLABLE"},
    ]
}

DIM_PRODUCT_SCHEMA = {
    "fields": [
        {"name": "product_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "name", "type": "STRING", "mode": "NULLABLE"},
        {"name": "category", "type": "STRING", "mode": "NULLABLE"},
        {"name": "unit_cost", "type": "FLOAT", "mode": "NULLABLE"},
    ]
}


def calcular_revenue(order):
    order = dict(order)
    order["revenue"] = float(order["quantity"]) * float(order["unit_price"])
    return order


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--silver_orders_table", required=True)
    parser.add_argument("--silver_customers_table", required=True)
    parser.add_argument("--silver_products_table", required=True)
    parser.add_argument("--gold_fact_sales_table", required=True)
    parser.add_argument("--gold_dim_customer_table", required=True)
    parser.add_argument("--gold_dim_product_table", required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)
    options = PipelineOptions(pipeline_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "LeerSilverOrders" >> ReadFromBigQuery(table=known_args.silver_orders_table)
            | "CalcularRevenue" >> beam.Map(calcular_revenue)
            | "EscribirFactSales" >> WriteToBigQuery(
                known_args.gold_fact_sales_table,
                schema=FACT_SALES_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        (
            p
            | "LeerSilverCustomers" >> ReadFromBigQuery(table=known_args.silver_customers_table)
            | "EscribirDimCustomer" >> WriteToBigQuery(
                known_args.gold_dim_customer_table,
                schema=DIM_CUSTOMER_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            )
        )

        (
            p
            | "LeerSilverProducts" >> ReadFromBigQuery(table=known_args.silver_products_table)
            | "EscribirDimProduct" >> WriteToBigQuery(
                known_args.gold_dim_product_table,
                schema=DIM_PRODUCT_SCHEMA,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
            )
        )


if __name__ == "__main__":
    run()
EOF
```

> [!NOTE]
> Aquí `create_disposition=CREATE_IF_NEEDED` sí funciona sin pre-crear la tabla con SQL: `fact_sales`, `dim_customer` y `dim_product` son tablas BigQuery **nativas**, y `WriteToBigQuery` sabe crear ese tipo de tabla a partir del `schema`. Esa es la diferencia práctica con Bronze/Silver que viste en los Pasos 2 y 3 (ver [teoría §5](teoria.md#5-escribir-iceberg-desde-dataflow-la-decisión-de-diseño-de-este-módulo)).

### 4.2. Ejecutar

```bash
python3 gold_pipeline.py \
  --silver_orders_table="${PROJECT_ID}:silver.orders" \
  --silver_customers_table="${PROJECT_ID}:silver.customers" \
  --silver_products_table="${PROJECT_ID}:silver.products" \
  --gold_fact_sales_table="${PROJECT_ID}:gold.fact_sales" \
  --gold_dim_customer_table="${PROJECT_ID}:gold.dim_customer" \
  --gold_dim_product_table="${PROJECT_ID}:gold.dim_product" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --temp_location="$BUCKET/tmp" \
  --staging_location="$BUCKET/staging" \
  --runner=DataflowRunner \
  --job_name="retailflow-gold-$(date +%s)"
```

---

## Paso 5 — Consumo: SQL de Negocio + Looker Studio (10 min)

### 5.1. Preguntas de negocio sobre el datamart

```sql
-- Ingresos por categoría de producto
SELECT p.category, ROUND(SUM(f.revenue), 2) AS revenue
FROM `TU_PROYECTO_ID.gold.fact_sales` f
JOIN `TU_PROYECTO_ID.gold.dim_product` p USING (product_id)
GROUP BY p.category
ORDER BY revenue DESC;

-- Ingresos por segmento de cliente
SELECT c.segment, ROUND(SUM(f.revenue), 2) AS revenue, COUNT(*) AS pedidos
FROM `TU_PROYECTO_ID.gold.fact_sales` f
JOIN `TU_PROYECTO_ID.gold.dim_customer` c USING (customer_id)
GROUP BY c.segment
ORDER BY revenue DESC;
```

### 5.2. Gráfico rápido en Looker Studio

1. Ve a [lookerstudio.google.com](https://lookerstudio.google.com) → **Crear → Fuente de datos → BigQuery**.
2. Selecciona tu proyecto → dataset `gold` → tabla `fact_sales` (agrega `dim_product` como una segunda fuente si quieres el nombre de categoría).
3. Crea un **gráfico de barras**: dimensión `product_id` (o `category` si uniste `dim_product`), métrica `SUM(revenue)`.

Esto cierra visualmente el recorrido Bronze → Silver → Gold → BI.

---

## Paso 6 — Limpieza (10 min)

> [!IMPORTANT]
> Cloud SQL y Datastream **facturan mientras existan**, hayas terminado o no el taller. Ejecuta esto antes de cerrar la sesión.

```bash
# 1. Pausar y eliminar el stream de Datastream
gcloud datastream streams update orders-to-gcs-stream \
    --location="$REGION" --state=PAUSED --update-mask=state

# Pausar NO es instantáneo: el stream pasa por RUNNING -> DRAINING -> PAUSED.
# Si intentas 'delete' mientras sigue en DRAINING, falla. Espera a que llegue a PAUSED:
until [[ "$(gcloud datastream streams describe orders-to-gcs-stream --location="$REGION" --format='value(state)')" == "PAUSED" ]]; do
  echo "Esperando a que el stream termine de pausarse (drenando datos en tránsito)..."
  sleep 10
done

gcloud datastream streams delete orders-to-gcs-stream --location="$REGION" --quiet

gcloud datastream connection-profiles delete pg-source-profile --location="$REGION" --quiet
gcloud datastream connection-profiles delete gcs-dest-profile --location="$REGION" --quiet

# 2. Eliminar Cloud SQL
gcloud sql instances delete retailflow-pg --quiet

# 3. Eliminar datasets de BigQuery (Bronze/Silver/Gold) y la conexión BigLake
bq rm --recursive --force "${PROJECT_ID}:bronze"
bq rm --recursive --force "${PROJECT_ID}:silver"
bq rm --recursive --force "${PROJECT_ID}:gold"
bq rm --force --connection "${PROJECT_ID}.${REGION}.lab05_conn"

# 4. Eliminar el bucket
gcloud storage rm --recursive "$BUCKET"

# 5. Salir del entorno virtual de Python
deactivate
```

> [!NOTE]
> Los jobs de Dataflow de este lab son **batch**: cada uno termina y libera sus workers automáticamente al finalizar. Solo necesitas cancelar manualmente un job desde **Dataflow → Jobs → Cancelar** si lo dejaste corriendo por error.

---

## Retos Opcionales (Extensión)

### Reto 1: Data quality gate en Silver

Modifica `silver_pipeline.py` para que las órdenes con `quantity <= 0` o con un `customer_id` que no exista en `bronze.customers` se envíen a una salida lateral (*side output*) en vez de a `silver.orders`, y cuenta cuántas filas fueron rechazadas.

<details>
<summary>👀 Ver Pista de Solución Reto 1</summary>

Usa `beam.pvalue.TaggedOutput` dentro de un `DoFn` (en vez de `beam.Map`) para producir dos ramas: la principal (`orders_ok`) y una etiquetada (`rechazados`). Necesitarás convertir `customers` en una `PCollection` de diccionario en memoria (`beam.pvalue.AsDict` o un `side input`) para poder validar `customer_id` contra ella dentro del `DoFn` que procesa `orders`.

</details>

### Reto 2: SCD Tipo 2 mínimo para `dim_customer`

En el Módulo 01 viste SCD2 implementado en SQL con `MERGE`. Extiende `gold_pipeline.py` para que, si `dim_customer` ya existe, compare el `segment` actual contra el `silver.customers` de esta corrida y solo agregue una fila nueva (con `valid_from`/`valid_to`) cuando el segmento cambió, en vez de sobrescribir toda la tabla con `WRITE_TRUNCATE`.

<details>
<summary>👀 Ver Pista de Solución Reto 2</summary>

Necesitarás leer la `dim_customer` actual con `ReadFromBigQuery` **antes** de escribir, cruzarla con `silver.customers` usando `CoGroupByKey` por `customer_id`, y cambiar `write_disposition` a `WRITE_APPEND`. Es sustancialmente más código que el `MERGE` de una línea en SQL — exactamente el trade-off que discute la [teoría §1](teoria.md#1-resumen-ejecutivo-sql-declarativo-vs-programación-de-datos): SQL declarativo gana en concisión cuando la lógica cabe en un `MERGE`.

</details>

### Reto 3: Compara Beam contra SQL puro para el mismo Gold

Reemplaza el Paso 4 completo por una única consulta `CREATE OR REPLACE TABLE ... AS SELECT` sobre `silver.orders`/`silver.customers`/`silver.products` directamente en BigQuery (sin Dataflow). Compara el tiempo total y la simplicidad del código contra el pipeline de Beam.

<details>
<summary>👀 Ver Pista de Solución Reto 3</summary>

Para este volumen de datos (unas pocas miles de filas, sin necesidad de parsear Avro/CSV heterogéneos), la consulta SQL directa será más rápida y bastante más corta que `gold_pipeline.py`. Es la evidencia empírica del punto central de la teoría de este módulo: usa Beam/Dataflow donde aporta algo que SQL no puede (parsear formatos, unificar fuentes heterogéneas), no como reemplazo genérico de SQL.

</details>

---

## Resumen de lo Aprendido

- **Beam/Dataflow como motor de transformación programático:** útil cuando la "T" del ELT necesita lógica imperativa (parsear Avro/CSV heterogéneos, deduplicar CDC con `GroupByKey`) que un `SELECT` declarativo no puede expresar cómodamente.
- **Datastream hacia Cloud Storage entrega CDC como archivos anidados** (`payload` + `source_metadata`), a diferencia del destino BigQuery del Módulo 01, que aplana esos mismos metadatos como columnas `_metadata_*`.
- **Iceberg gestionado en BigQuery (Módulo 03) es el puente pragmático** entre Dataflow y un lakehouse abierto: evita depender del conector `IcebergIO` nativo de Beam (hoy Java-only y con un *setup* de catálogo REST que no cabe en un taller corto), a costa de tener que pre-crear las tablas con SQL antes de escribir desde Dataflow.
- **El costo no es cero:** Cloud SQL, Datastream, los workers de Dataflow y los `LOAD JOB`s hacia tablas Iceberg gestionadas facturan por separado de la capa gratuita de BigQuery — la limpieza al final no es opcional.

---

## Referencias

- [Datastream — Create connection profiles](https://docs.cloud.google.com/datastream/docs/create-connection-profiles)
- [Datastream — Events and streams](https://docs.cloud.google.com/datastream/docs/events-and-streams)
- [Datastream — Stream states and actions](https://docs.cloud.google.com/datastream/docs/stream-states-and-actions)
- [Apache Beam — Python SDK BigQuery I/O](https://beam.apache.org/documentation/io/built-in/google-bigquery/)
- [Apache Beam — Programming Guide](https://beam.apache.org/documentation/programming-guide/)
- [BigQuery — Apache Iceberg managed tables](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery)
- [Dataflow — Deploy a pipeline](https://docs.cloud.google.com/dataflow/docs/guides/deploying-a-pipeline)
