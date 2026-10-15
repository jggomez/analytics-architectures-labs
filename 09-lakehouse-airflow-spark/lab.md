# Lab 09 — Lakehouse en Producción: Datastream → Iceberg → Spark → BigQuery, orquestado con Airflow

> 📖 **Marco Teórico:** Lee primero la [Guía de Lakehouse en Producción](teoria.md). Explica por qué cada capa usa el formato y el catálogo que usa, cómo se vuelve idempotente un pipeline CDC, qué es Write-Audit-Publish y cómo se reparte la calidad de datos entre Spark y Knowledge Catalog.
>
> Este es el lab **más avanzado del repositorio** y da por conocidos el patrón CDC + Medallion ([Módulo 01](../01-patrones-y-modelado/lab.md)), Iceberg ([Módulo 03](../03-formatos-y-lakehouse/lab.md)) y el recorrido Datastream → Iceberg → datamart del [Módulo 05](../05-elt-dataflow-iceberg/lab.md). Lo nuevo aquí es lo que separa un taller de un sistema real: **varias tablas, CDC con borrados, cargas incrementales e idempotentes, cuarentena, SCD2, los tres tipos de hechos de Kimball, una puerta de calidad, orquestación con Airflow y un incidente que hay que resolver.**

---

## Codelab — Taller de ~3 horas: "ShopLatam"

**ShopLatam** es un e-commerce ficticio que vende en Colombia, México, Perú y Chile. Su sistema transaccional es un PostgreSQL con **9 tablas** (`categories`, `products`, `inventory`, `customers`, `addresses`, `orders`, `order_items`, `payments`, `shipments`) que cambian todo el tiempo: entran pedidos, se pagan, se despachan, se entregan; los clientes cambian de segmento y de correo; los precios se ajustan; se borran direcciones.

El área de datos necesita un **datamart de ventas y operación logística** actualizado cada 30 minutos, que no se rompa cuando el origen manda datos malos y que nunca le muestre a negocio un número sin validar.

```
ARQUITECTURA DEL LABORATORIO — SHOPLATAM

 Cloud SQL (PostgreSQL 15)  ◄── generator.py (pedidos, pagos, envíos, cambios, borrados)
 esquema shop · 9 tablas
        │ Datastream (CDC, append-only)
        ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ BRONZE · BigQuery dataset `bronze` · tablas Apache Iceberg (Parquet en       │
 │ gs://…-shop-bronze) administradas por BigQuery · 1 fila por evento CDC        │
 └──────────────────────────────────┬───────────────────────────────────────────┘
                                    │ Spark serverless · silver_job.py  ◄── Airflow DAG shop_silver (cada 30 min)
                                    ▼
 ┌──────────────────────────────────────────────────────────────────────────────┐
 │ SILVER · Lakehouse runtime catalog `shop_lake` (Iceberg REST) en             │
 │ gs://…-shop-lake · estado actual limpio + historia + cuarentena + ops         │
 └──────────────────────────────────┬───────────────────────────────────────────┘
                                    │ Spark serverless · gold_job.py   ◄── Airflow DAG shop_gold (Asset)
                                    ▼
          BigQuery `gold_staging` ──► Knowledge Catalog (puerta de calidad) ──► BigQuery `gold`
          (Write)                      (Audit)                                  (Publish)
                                                                                    │
                                                                         BI / SQL de negocio
```

> [!NOTE]
> **Nombres de productos en 2026.** Google renombró varios servicios este año y en la consola vas a ver los dos nombres. Los comandos `gcloud`, las APIs y los roles de IAM **no cambiaron**:
>
> | Nombre anterior | Nombre actual | Comando que sigue funcionando |
> |---|---|---|
> | Cloud Composer | **Managed Service for Apache Airflow** | `gcloud composer …` |
> | Dataproc / Serverless for Apache Spark | **Managed Service for Apache Spark** | `gcloud dataproc batches …` |
> | BigLake | **Lakehouse** | `gcloud biglake …` |
> | BigLake metastore | **Lakehouse runtime catalog** | `gcloud biglake iceberg catalogs …` |
> | Dataplex Universal Catalog | **Knowledge Catalog** | `gcloud dataplex datascans …` |

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. Replicar **9 tablas** de PostgreSQL con **Datastream directamente a tablas Apache Iceberg** (sin pasar por archivos Avro intermedios) y explicar qué es cada columna de `datastream_metadata`.
2. Construir con **Spark serverless** una capa Silver en un **catálogo Iceberg REST** (Lakehouse runtime catalog): carga incremental con *watermark* + *lookback*, deduplicación de CDC, borrados lógicos, `MERGE` idempotente, limpieza, enriquecimiento y **cuarentena**.
3. Modelar en Gold una **dimensión SCD2**, una SCD1 y los **tres tipos de tabla de hechos** de Kimball (transaccional, *accumulating snapshot* y *periodic snapshot*), con claves sustitutas deterministas y un **miembro desconocido**.
4. Orquestar todo con **Airflow 3** en Managed Service for Apache Airflow: un DAG dispara al otro por **Assets**, los jobs corren como batches de Spark, y Gold sigue el patrón **Write-Audit-Publish** con una puerta de calidad de **Knowledge Catalog**.
5. Resolver un **incidente real**: datos malos del origen, puerta de calidad que bloquea la publicación, corrección y re-ejecución.
6. Usar **time travel y rollback de Iceberg** para deshacer un despliegue con un bug, y correr el **mantenimiento** de las tablas (compactación y expiración de snapshots).

### Prerrequisitos

- Proyecto de Google Cloud con facturación habilitada y rol **Owner** (el lab crea cuentas de servicio y les otorga roles). **Recomendado: un proyecto nuevo solo para este lab**; así la limpieza final es un solo comando.
- Todo se ejecuta en **Cloud Shell**. Ya trae `gcloud`, `bq`, `psql` y `python3`.
- Cuota regional de CPU en `us-central1` de al menos **24 vCPU** (cada batch de Spark usa 12). Revísala en **IAM y administración → Cuotas** si tu proyecto es nuevo.
- Haber hecho (o al menos leído) los Módulos 01, 03 y 05.

> [!IMPORTANT]
> Los bloques `bash` reutilizan variables de entorno del Paso 0.1. Cloud Shell se desconecta tras un rato de inactividad: el Paso 0.1 las guarda en `~/lab09.env`, así que **si se reinicia la sesión, ejecuta `source ~/lab09.env`** antes de seguir. En los bloques `sql`, reemplaza `TU_PROYECTO_ID` por el tuyo.

### Costo Estimado (FinOps)

| Concepto | Recurso en el lab | ¿Capa gratuita? | Estimado (~3-4 h) |
|---|---|---|---|
| **Managed Service for Apache Airflow** | 1 entorno Gen 3 en **modo desarrollo** (preset Small) | ❌ No | **~$1.50–3.00** (se cobra por DCU-hora mientras existe; Google no publica un precio cerrado por entorno, usa la [calculadora](https://cloud.google.com/products/calculator)) |
| **Cloud SQL (PostgreSQL)** | `db-g1-small`, edición Enterprise | ❌ No | **~$0.10–0.20** |
| **Datastream** | 1 stream, 9 tablas, volumen de MB | ❌ No (por GB procesado) | **< $0.10** |
| **Managed Service for Apache Spark** | ~15 batches serverless de 2-4 min, 12 vCPU c/u | ❌ No | **~$0.50–1.50** |
| **Knowledge Catalog (calidad de datos)** | ~6 ejecuciones de 2 escaneos | ❌ No (por DCU) | **< $0.10** |
| **BigQuery** | Ingesta de Datastream, lecturas de Spark, consultas | ✅ Casi todo dentro de la capa gratuita | **~$0.00** |
| **Cloud Storage** | 3 buckets con MB de datos | ✅ Sí | **~$0.00** |

> [!WARNING]
> **Costo total aproximado: $3–6 USD**, siempre que limpies al final. **El entorno de Airflow y Cloud SQL facturan por hora aunque no los uses.** Si vas a pausar el lab varias horas, el Paso 7 explica cómo **hibernar** Airflow (GA desde el 13-oct-2026), que deja casi en cero su costo.

---

### Mapa del Laboratorio (~180 minutos)

```
Bloque previo (10 min)  Arquitectura, renombres 2026 y qué hace "real" a este pipeline
Paso 0  (20 min)  Variables, cuentas de servicio, Airflow (en segundo plano), Cloud SQL, buckets, catálogo
Paso 1  (25 min)  Origen con 9 tablas + Datastream → Bronze Iceberg + generador de tráfico
Paso 2  (25 min)  Silver con Spark: incremental, MERGE idempotente, limpieza, cuarentena
Paso 3  (25 min)  Gold con Spark: SCD2, 3 tipos de hechos, conciliación + reglas de calidad
Paso 4  (25 min)  Airflow 3: DAGs con Assets, Write-Audit-Publish, consultas de negocio
Paso 5  (25 min)  🚨 Incidente: datos malos, la puerta de calidad bloquea, corrección
Paso 6  (15 min)  Iceberg: despliegue con bug, time travel, rollback y mantenimiento
Paso 7  (10 min)  Hibernar o limpiar
```

---

## Paso 0 — Aprovisionar el Entorno (20 min)

### 0.1. Variables de entorno y APIs

```bash
cat > ~/lab09.env <<EOF
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects describe "$(gcloud config get-value project)" --format="value(projectNumber)")
export REGION=us-central1
export BRONZE_BUCKET="\${PROJECT_ID}-shop-bronze"
export LAKE_BUCKET="\${PROJECT_ID}-shop-lake"
export OPS_BUCKET="\${PROJECT_ID}-shop-ops"
export CATALOG=shop_lake
export SPARK_SA="sa-spark-lakehouse@\${PROJECT_ID}.iam.gserviceaccount.com"
export AIRFLOW_SA="sa-airflow@\${PROJECT_ID}.iam.gserviceaccount.com"
export AIRFLOW_ENV=shop-airflow
export PG_INSTANCE=shoplatam-pg
export PG_PASSWORD='TuPasswordSeguro123!'
EOF
source ~/lab09.env && echo "Proyecto: $PROJECT_ID ($PROJECT_NUMBER)"

gcloud services enable \
  sqladmin.googleapis.com datastream.googleapis.com \
  bigquery.googleapis.com bigqueryconnection.googleapis.com bigquerystorage.googleapis.com \
  biglake.googleapis.com dataproc.googleapis.com composer.googleapis.com \
  dataplex.googleapis.com compute.googleapis.com storage.googleapis.com
```

> [!WARNING]
> La contraseña de Postgres va en texto plano solo porque es un lab. En producción vive en **Secret Manager** y ni Datastream ni Airflow la ven en claro.

### 0.2. Cuentas de servicio con mínimo privilegio

Cada componente corre con **su propia identidad**: Spark solo puede leer y escribir datos; Airflow solo puede lanzar trabajos y validar. Si una se ve comprometida, el daño queda acotado.

```bash
gcloud iam service-accounts create sa-spark-lakehouse --display-name="ShopLatam - Spark (Bronze/Silver/Gold)"
gcloud iam service-accounts create sa-airflow --display-name="ShopLatam - Airflow (orquestador)"
sleep 20

# Spark: leer Bronze (Storage Read API), escribir Silver (catálogo + GCS) y Gold (BigQuery)
for ROLE in roles/dataproc.worker roles/bigquery.dataEditor roles/bigquery.jobUser \
            roles/bigquery.readSessionUser roles/biglake.editor roles/storage.objectUser; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${SPARK_SA}" \
    --role="$ROLE" --condition=None --quiet > /dev/null
done

# Airflow: lanzar batches de Spark, correr escaneos de calidad y copiar tablas en BigQuery
for ROLE in roles/composer.worker roles/dataproc.editor roles/bigquery.jobUser roles/bigquery.dataEditor \
            roles/dataplex.dataScanEditor roles/dataplex.dataScanDataViewer; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" --member="serviceAccount:${AIRFLOW_SA}" \
    --role="$ROLE" --condition=None --quiet > /dev/null
done

# Airflow necesita "actuar como" la cuenta de Spark para lanzar batches con esa identidad
gcloud iam service-accounts add-iam-policy-binding "$SPARK_SA" \
  --member="serviceAccount:${AIRFLOW_SA}" --role="roles/iam.serviceAccountUser"

# Agente de servicio de Knowledge Catalog: lee las tablas que valida y escribe los resultados
gcloud beta services identity create --service=dataplex.googleapis.com
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:service-${PROJECT_NUMBER}@gcp-sa-dataplex.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor" --condition=None --quiet > /dev/null
```

> [!NOTE]
> Para que quepa en el tiempo del lab, los roles se otorgan **a nivel de proyecto**. En producción irían a nivel de bucket y de dataset: por ejemplo, el agente de Knowledge Catalog solo necesita *leer* `gold_staging` y *escribir* en `quality`.

### 0.3. Crear el entorno de Airflow **ya** (tarda ~25 min)

Crear el entorno es lo más lento del lab, así que lo lanzas primero con `--async` y sigues trabajando mientras se aprovisiona.

```bash
gcloud beta composer environments create "$AIRFLOW_ENV" \
  --location="$REGION" \
  --image-version=composer-3-airflow-3 \
  --enable-development-mode \
  --service-account="$AIRFLOW_SA" \
  --env-variables="SHOP_PROJECT=${PROJECT_ID},SHOP_REGION=${REGION},SHOP_OPS_BUCKET=${OPS_BUCKET},SHOP_SPARK_SA=${SPARK_SA},SHOP_CATALOG=${CATALOG}" \
  --async
```

- **`composer-3-airflow-3`**: Airflow 3 es GA en la generación 3 del servicio desde abril de 2026. La imagen *por defecto* sigue siendo Airflow 2.11, por eso se pide explícitamente.
- **`--enable-development-mode`**: entorno de un solo nodo, preset Small y la posibilidad de **hibernar**. Es para desarrollo, no para producción, y es justo lo que necesita un lab.
- **`--env-variables`**: los DAGs leen la configuración de estas variables en vez de tener el proyecto escrito en el código. Van con el prefijo `SHOP_` porque Airflow reserva algunos nombres (como `GCS_BUCKET`).

### 0.4. Cloud SQL con decodificación lógica (también en segundo plano)

```bash
gcloud sql instances create "$PG_INSTANCE" \
  --database-version=POSTGRES_15 \
  --edition=ENTERPRISE \
  --tier=db-g1-small \
  --region="$REGION" \
  --root-password="$PG_PASSWORD" \
  --database-flags=cloudsql.logical_decoding=on \
  --storage-size=10 \
  --authorized-networks=0.0.0.0/0 \
  --async
```

> [!WARNING]
> `--authorized-networks=0.0.0.0/0` abre la instancia a internet (protegida solo por contraseña) para que Datastream y Cloud Shell la alcancen sin configurar redes privadas. **Solo en este lab.** En producción se usa conectividad privada (Private Service Connect) y la instancia no tiene IP pública.

### 0.5. Buckets, datasets, conexión y catálogo Iceberg

```bash
# Un bucket por responsabilidad: Bronze (lo escribe Datastream), Lake (Silver), Ops (código, referencias, temporales)
for B in "$BRONZE_BUCKET" "$LAKE_BUCKET" "$OPS_BUCKET"; do
  gcloud storage buckets create "gs://${B}" --location="$REGION" --uniform-bucket-level-access
done

bq --location="$REGION" mk --dataset --description "ShopLatam - Bronze (Iceberg, CDC de Datastream)" "${PROJECT_ID}:bronze"
bq --location="$REGION" mk --dataset --description "ShopLatam - Gold sin validar (Write)"            "${PROJECT_ID}:gold_staging"
bq --location="$REGION" mk --dataset --description "ShopLatam - Gold publicado (lo que ve negocio)"  "${PROJECT_ID}:gold"
bq --location="$REGION" mk --dataset --description "ShopLatam - Resultados de calidad de datos"      "${PROJECT_ID}:quality"

# Conexión de BigQuery que usa Datastream para escribir el Parquet de Bronze en TU bucket
bq mk --connection --location="$REGION" --connection_type=CLOUD_RESOURCE shop_bronze_conn
CONN_SA=$(bq show --format=prettyjson --connection "${PROJECT_ID}.${REGION}.shop_bronze_conn" \
          | jq -r .cloudResource.serviceAccountId)
sleep 20
gcloud storage buckets add-iam-policy-binding "gs://${BRONZE_BUCKET}" \
  --member="serviceAccount:${CONN_SA}" --role="roles/storage.admin"

# Catálogo Iceberg REST de Silver (Lakehouse runtime catalog)
gcloud biglake iceberg catalogs create "$CATALOG" \
  --project="$PROJECT_ID" \
  --catalog-type=biglake \
  --default-location="gs://${LAKE_BUCKET}" \
  --credential-mode=end-user

# Spark serverless corre sin IP pública: necesita Private Google Access para hablar con las APIs
gcloud compute networks subnets update default --region="$REGION" --enable-private-ip-google-access
```

**Dos tipos de tabla Iceberg en el mismo lab, a propósito:**

| | Bronze | Silver |
|---|---|---|
| **Quién escribe** | Datastream (vía BigQuery) | Spark |
| **Catálogo** | BigQuery (tabla Iceberg administrada por BigQuery) | Lakehouse runtime catalog (Iceberg REST) |
| **Quién lee** | BigQuery y Spark (conector de BigQuery) | Spark, BigQuery y cualquier motor Iceberg (Trino, Flink…) |
| **Por qué** | Es la única forma en que Datastream escribe Iceberg | Es la opción que Google recomienda para un lakehouse abierto: el dato no queda atado a un motor |

- `--credential-mode=end-user`: cada motor accede a GCS con **su propia identidad**, por eso Spark tiene `roles/storage.objectUser`. La alternativa, *credential vending*, hace que el catálogo entregue credenciales temporales por tabla. Se ve en la [teoría §5](teoria.md#5-catálogos-iceberg-en-google-cloud).
- `--catalog-type=biglake`: catálogo de varios buckets, el tipo recomendado. El tipo de un solo bucket es *legacy*.

---

## Paso 1 — Origen, Datastream y Bronze (25 min)

### 1.1. Esquema transaccional de 9 tablas

Espera a que Cloud SQL esté lista (5-8 min desde el Paso 0.4) y crea la base:

```bash
until [[ "$(gcloud sql instances describe "$PG_INSTANCE" --format='value(state)')" == "RUNNABLE" ]]; do
  echo "Esperando a Cloud SQL..."; sleep 20
done
gcloud sql databases create shoplatam_db --instance="$PG_INSTANCE"
export PG_IP=$(gcloud sql instances describe "$PG_INSTANCE" --format='value(ipAddresses[0].ipAddress)')
echo "export PG_IP=${PG_IP}" >> ~/lab09.env
```

El esquema refleja un e-commerce real: claves foráneas, una clave compuesta (`order_items`), una jerarquía (`categories.parent_id`) y un `ON DELETE SET NULL`. Además tiene, **a propósito, la deuda técnica típica** de un sistema heredado: `order_items.quantity` no tiene `CHECK (quantity > 0)`.

```bash
mkdir -p ~/lab09 && cd ~/lab09
cat <<'EOF' > schema.sql
CREATE SCHEMA IF NOT EXISTS shop;

CREATE TABLE shop.categories (
    category_id   INT PRIMARY KEY,
    name          VARCHAR(80) NOT NULL,
    parent_id     INT REFERENCES shop.categories(category_id)
);

CREATE TABLE shop.products (
    product_id      SERIAL PRIMARY KEY,
    sku             VARCHAR(20) UNIQUE NOT NULL,
    name            VARCHAR(120) NOT NULL,
    category_id     INT NOT NULL REFERENCES shop.categories(category_id),
    list_price_usd  NUMERIC(12,2) NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    updated_at      TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shop.inventory (
    product_id     INT PRIMARY KEY REFERENCES shop.products(product_id),
    stock_on_hand  INT NOT NULL,
    updated_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shop.customers (
    customer_id  SERIAL PRIMARY KEY,
    email        VARCHAR(120),
    full_name    VARCHAR(120) NOT NULL,
    phone        VARCHAR(40),
    segment      VARCHAR(20) NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT now(),
    updated_at   TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shop.addresses (
    address_id    SERIAL PRIMARY KEY,
    customer_id   INT NOT NULL REFERENCES shop.customers(customer_id),
    city          VARCHAR(80) NOT NULL,
    country_code  CHAR(2) NOT NULL,
    postal_code   VARCHAR(12),
    is_default    BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at    TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shop.orders (
    order_id             BIGSERIAL PRIMARY KEY,
    customer_id          INT NOT NULL REFERENCES shop.customers(customer_id),
    shipping_address_id  INT REFERENCES shop.addresses(address_id) ON DELETE SET NULL,
    status               VARCHAR(20) NOT NULL,
    currency             CHAR(3) NOT NULL,
    order_ts             TIMESTAMP NOT NULL DEFAULT now(),
    updated_at           TIMESTAMP NOT NULL DEFAULT now()
);

-- Sin CHECK (quantity > 0): es un sistema "legacy" y eso es justo lo que Silver debe atrapar.
CREATE TABLE shop.order_items (
    order_id      BIGINT NOT NULL REFERENCES shop.orders(order_id),
    line_number   INT NOT NULL,
    product_id    INT NOT NULL REFERENCES shop.products(product_id),
    quantity      INT NOT NULL,
    unit_price    NUMERIC(14,2) NOT NULL,
    discount_pct  NUMERIC(5,2) NOT NULL DEFAULT 0,
    PRIMARY KEY (order_id, line_number)
);

CREATE TABLE shop.payments (
    payment_id  BIGSERIAL PRIMARY KEY,
    order_id    BIGINT NOT NULL REFERENCES shop.orders(order_id),
    method      VARCHAR(20) NOT NULL,
    amount      NUMERIC(14,2) NOT NULL,
    currency    CHAR(3) NOT NULL,
    status      VARCHAR(20) NOT NULL,
    paid_at     TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE shop.shipments (
    shipment_id   BIGSERIAL PRIMARY KEY,
    order_id      BIGINT NOT NULL REFERENCES shop.orders(order_id),
    carrier       VARCHAR(30) NOT NULL,
    status        VARCHAR(20) NOT NULL,
    shipped_at    TIMESTAMP NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMP
);

-- Replicación lógica para Datastream
ALTER USER postgres WITH REPLICATION;
CREATE USER datastream_user WITH REPLICATION ENCRYPTED PASSWORD 'StreamSecret2026!';
GRANT cloudsqlsuperuser TO datastream_user;
GRANT USAGE ON SCHEMA shop TO datastream_user;
GRANT SELECT ON ALL TABLES IN SCHEMA shop TO datastream_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA shop GRANT SELECT ON TABLES TO datastream_user;

CREATE PUBLICATION datastream_pub FOR TABLES IN SCHEMA shop;
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('datastream_slot', 'pgoutput');
EOF

PGPASSWORD="$PG_PASSWORD" psql "host=${PG_IP} dbname=shoplatam_db user=postgres sslmode=require" \
  -v ON_ERROR_STOP=1 -f schema.sql
```

> [!NOTE]
> `CREATE PUBLICATION … FOR TABLES IN SCHEMA shop` (PostgreSQL 15+) publica **todas las tablas del esquema, incluidas las que se creen después**. Por eso, en el Reto 1, una tabla nueva entra al CDC sin tocar la publicación.

### 1.2. Generador de tráfico y datos iniciales

`generator.py` simula la operación del negocio. En modo `seed` crea el catálogo y 150 clientes. En modo `traffic` hace avanzar pedidos por el embudo (`CREATED → PAID → SHIPPED → DELIVERED`, o `CANCELLED`), registra pagos rechazados, cambia segmentos (lo que alimenta el SCD2), ajusta precios y borra direcciones. Y, como en la vida real, **escribe datos sucios**: emails en mayúsculas con espacios, ciudades como `bogota` o `BOGOTÁ D.C.`, teléfonos con guiones y algún email inválido.

```bash
cat <<'EOF' > generator.py
"""Generador de ShopLatam: siembra el catálogo y simula la operación del e-commerce.

Modos:
  seed     crea categorías, productos, inventario, clientes y direcciones
  traffic  simula pedidos, pagos, envíos, cambios de clientes y precios durante N minutos
  chaos    simula el lanzamiento en Argentina (moneda ARS) con un bug en devoluciones
"""
import argparse
import random
import time

import pg8000.native
from faker import Faker

fake = Faker("es_CO")

# país -> (moneda, unidades por USD, variantes "sucias" de ciudades tal como las tipean los usuarios)
COUNTRIES = {
    "CO": ("COP", 4000.0, ["Bogotá", "bogota", "BOGOTÁ D.C.", "Medellín", "medellin", "Cali"]),
    "MX": ("MXN", 18.5, ["Ciudad de México", "CDMX", "Guadalajara", "monterrey"]),
    "PE": ("PEN", 3.7, ["Lima", "LIMA", "Arequipa"]),
    "CL": ("CLP", 950.0, ["Santiago", "santiago de chile", "Valparaíso"]),
}
PHONE_PREFIX = {"CO": "+57", "MX": "+52", "PE": "+51", "CL": "+56", "AR": "+54"}

CATEGORIES = [
    (1, "Electrónica", None), (2, "Hogar", None), (3, "Moda", None), (4, "Deportes", None),
    (11, "Celulares", 1), (12, "Computadores", 1), (13, "Audio", 1),
    (21, "Cocina", 2), (22, "Muebles", 2),
    (31, "Calzado", 3), (32, "Ropa", 3),
    (41, "Ciclismo", 4), (42, "Fitness", 4),
]
PRICE_RANGE_USD = {11: (150, 900), 12: (400, 1800), 13: (20, 300), 21: (15, 250), 22: (80, 900),
                   31: (30, 160), 32: (10, 90), 41: (60, 1200), 42: (10, 200)}
CARRIERS = ["Servientrega", "DHL", "Estafeta", "Olva", "Chilexpress"]
PAYMENT_METHODS = ["CARD", "PSE", "WALLET", "CASH_ON_DELIVERY"]


def connect(args):
    return pg8000.native.Connection(user="postgres", password=args.password, host=args.host,
                                    database="shoplatam_db")


def dirty_email():
    user = fake.user_name()
    domain = random.choice(["gmail.com", "hotmail.com", "outlook.com", "empresa.com.co", "correo.mx"])
    r = random.random()
    if r < 0.01:
        return "sin-correo"                       # inválido: va a cuarentena en Silver
    if r < 0.15:
        return f"  {user.upper()}@{domain.upper()} "  # válido pero sucio: Silver lo normaliza
    return f"{user}@{domain}"


def dirty_phone(country):
    digits = "".join(random.choice("0123456789") for _ in range(10))
    if random.random() < 0.5:
        return f"{PHONE_PREFIX[country]} {digits[:3]} {digits[3:6]} {digits[6:]}"
    return f"{digits[:3]}-{digits[3:6]}-{digits[6:]}"


def create_customer(con, country=None):
    country = country or random.choice(list(COUNTRIES))
    name = fake.name()
    row = con.run(
        "INSERT INTO shop.customers (email, full_name, phone, segment) "
        "VALUES (:e, :n, :p, 'NEW') RETURNING customer_id",
        e=dirty_email(), n=name, p=dirty_phone(country))
    customer_id = row[0][0]
    cities = COUNTRIES[country][2] if country in COUNTRIES else ["Buenos Aires", "Córdoba"]
    for i in range(random.choice([1, 1, 2])):
        con.run(
            "INSERT INTO shop.addresses (customer_id, city, country_code, postal_code, is_default) "
            "VALUES (:c, :city, :cc, :pc, :d)",
            c=customer_id, city=random.choice(cities), cc=country, pc=fake.postcode(), d=(i == 0))
    return customer_id


def seed(con, n_customers):
    for cid, name, parent in CATEGORIES:
        con.run("INSERT INTO shop.categories VALUES (:i, :n, :p)", i=cid, n=name, p=parent)
    sku = 1
    for cid, (low, high) in PRICE_RANGE_USD.items():
        for _ in range(5):
            row = con.run(
                "INSERT INTO shop.products (sku, name, category_id, list_price_usd) "
                "VALUES (:s, :n, :c, :p) RETURNING product_id",
                s=f"SKU-{sku:05d}", n=f"{fake.word().capitalize()} {fake.word()}",
                c=cid, p=round(random.uniform(low, high), 2))
            con.run("INSERT INTO shop.inventory (product_id, stock_on_hand) VALUES (:p, :s)",
                    p=row[0][0], s=random.randint(50, 500))
            sku += 1
    for _ in range(n_customers):
        create_customer(con)
    print(f"Semilla lista: {len(CATEGORIES)} categorías, {sku - 1} productos, {n_customers} clientes")


def create_order(con, customer_id=None, currency_override=None, bad_quantity=False):
    if customer_id is None:
        customer_id = con.run("SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1")[0][0]
    addr = con.run("SELECT address_id, country_code FROM shop.addresses "
                   "WHERE customer_id = :c ORDER BY is_default DESC LIMIT 1", c=customer_id)
    if not addr:
        return None
    address_id, country = addr[0]
    currency, rate = COUNTRIES[country][:2] if country in COUNTRIES else ("ARS", 1000.0)
    currency = currency_override or currency
    order_id = con.run(
        "INSERT INTO shop.orders (customer_id, shipping_address_id, status, currency) "
        "VALUES (:c, :a, 'CREATED', :cur) RETURNING order_id",
        c=customer_id, a=address_id, cur=currency)[0][0]
    products = con.run("SELECT product_id, list_price_usd FROM shop.products "
                       "WHERE is_active ORDER BY random() LIMIT :n", n=random.randint(1, 4))
    for line, (product_id, price_usd) in enumerate(products, start=1):
        quantity = random.choice([1, 1, 1, 2, 3])
        if bad_quantity and line == 1:
            quantity = -quantity                  # bug del flujo de devoluciones
        con.run(
            "INSERT INTO shop.order_items (order_id, line_number, product_id, quantity, unit_price, discount_pct) "
            "VALUES (:o, :l, :p, :q, :u, :d)",
            o=order_id, l=line, p=product_id, q=quantity,
            u=round(float(price_usd) * rate * random.uniform(0.95, 1.05), 2),
            d=random.choice([0, 0, 0, 5, 10]))
    return order_id


def order_total(con, order_id):
    return con.run("SELECT COALESCE(SUM(quantity * unit_price * (1 - discount_pct / 100)), 0) "
                   "FROM shop.order_items WHERE order_id = :o", o=order_id)[0][0]


def advance_orders(con):
    """Mueve pedidos por el embudo CREATED -> PAID -> SHIPPED -> DELIVERED (o CANCELLED)."""
    for order_id, status, currency in con.run(
            "SELECT order_id, status, currency FROM shop.orders "
            "WHERE status IN ('CREATED', 'PAID', 'SHIPPED') ORDER BY random() LIMIT 8"):
        r = random.random()
        if status == "CREATED" and r < 0.06:
            con.run("UPDATE shop.orders SET status = 'CANCELLED', updated_at = now() WHERE order_id = :o", o=order_id)
        elif status == "CREATED":
            method = random.choice(PAYMENT_METHODS)
            if r < 0.15:
                con.run("INSERT INTO shop.payments (order_id, method, amount, currency, status) "
                        "VALUES (:o, :m, :a, :c, 'REJECTED')",
                        o=order_id, m=method, a=order_total(con, order_id), c=currency)
                continue
            con.run("INSERT INTO shop.payments (order_id, method, amount, currency, status) "
                    "VALUES (:o, :m, :a, :c, 'APPROVED')",
                    o=order_id, m=method, a=order_total(con, order_id), c=currency)
            con.run("UPDATE shop.orders SET status = 'PAID', updated_at = now() WHERE order_id = :o", o=order_id)
            con.run("UPDATE shop.inventory i SET stock_on_hand = GREATEST(i.stock_on_hand - oi.quantity, 0), "
                    "updated_at = now() FROM shop.order_items oi "
                    "WHERE oi.order_id = :o AND oi.product_id = i.product_id", o=order_id)
        elif status == "PAID":
            con.run("INSERT INTO shop.shipments (order_id, carrier, status) VALUES (:o, :c, 'IN_TRANSIT')",
                    o=order_id, c=random.choice(CARRIERS))
            con.run("UPDATE shop.orders SET status = 'SHIPPED', updated_at = now() WHERE order_id = :o", o=order_id)
        elif status == "SHIPPED":
            con.run("UPDATE shop.shipments SET status = 'DELIVERED', delivered_at = now() WHERE order_id = :o",
                    o=order_id)
            con.run("UPDATE shop.orders SET status = 'DELIVERED', updated_at = now() WHERE order_id = :o", o=order_id)


def mutate_master_data(con):
    """Cambios que alimentan SCD2 (segmento), SCD1 (precio) y borrados (direcciones)."""
    if random.random() < 0.4:
        con.run("UPDATE shop.customers SET segment = CASE segment WHEN 'NEW' THEN 'REGULAR' ELSE 'VIP' END, "
                "updated_at = now() WHERE customer_id = (SELECT customer_id FROM shop.customers "
                "WHERE segment <> 'VIP' ORDER BY random() LIMIT 1)")
    if random.random() < 0.15:
        con.run("UPDATE shop.customers SET email = :e, updated_at = now() WHERE customer_id = "
                "(SELECT customer_id FROM shop.customers ORDER BY random() LIMIT 1)", e=dirty_email())
    if random.random() < 0.3:
        create_customer(con)
    if random.random() < 0.2:
        con.run("UPDATE shop.products SET list_price_usd = ROUND(list_price_usd * :f, 2), updated_at = now() "
                "WHERE product_id = (SELECT product_id FROM shop.products ORDER BY random() LIMIT 1)",
                f=random.choice([0.9, 1.05, 1.1]))
    if random.random() < 0.1:
        con.run("DELETE FROM shop.addresses WHERE address_id = (SELECT a.address_id FROM shop.addresses a "
                "WHERE NOT a.is_default AND NOT EXISTS (SELECT 1 FROM shop.orders o "
                "WHERE o.shipping_address_id = a.address_id) LIMIT 1)")


def traffic(con, minutes, interval):
    end = time.time() + minutes * 60
    tick = 0
    while time.time() < end:
        tick += 1
        for _ in range(random.randint(2, 5)):
            create_order(con)
        advance_orders(con)
        mutate_master_data(con)
        stats = con.run("SELECT status, count(*) FROM shop.orders GROUP BY 1 ORDER BY 1")
        print(f"[tick {tick}] pedidos por estado: {dict(stats)}", flush=True)
        time.sleep(interval)


def chaos(con):
    """Argentina entra en operación (moneda ARS sin tasa de cambio) y un bug genera cantidades negativas."""
    for _ in range(6):
        customer_id = create_customer(con, country="AR")
        create_order(con, customer_id=customer_id, currency_override="ARS")
    for _ in range(3):
        create_order(con, bad_quantity=True)
    print("Caos inyectado: 6 pedidos en ARS y 3 pedidos con una línea de cantidad negativa")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["seed", "traffic", "chaos"])
    parser.add_argument("--host", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--customers", type=int, default=150)
    parser.add_argument("--minutes", type=int, default=120)
    parser.add_argument("--interval", type=int, default=20)
    args = parser.parse_args()

    con = connect(args)
    if args.mode == "seed":
        seed(con, args.customers)
    elif args.mode == "traffic":
        traffic(con, args.minutes, args.interval)
    else:
        chaos(con)


if __name__ == "__main__":
    main()
EOF

python3 -m venv /tmp/lab09-venv && source /tmp/lab09-venv/bin/activate
pip install --quiet pg8000==1.31.2 faker==37.1.0

python generator.py seed --host "$PG_IP" --password "$PG_PASSWORD"
```

### 1.3. Datastream: CDC directo a tablas Apache Iceberg

En el Módulo 05, Datastream escribía **archivos Avro** en Cloud Storage y Dataflow los convertía en tablas. Aquí se usa la otra opción del destino BigQuery: **"Apache Iceberg tables"**. Datastream escribe Parquet en **tu** bucket y BigQuery administra la tabla Iceberg. No hay archivos Avro intermedios ni un job que los convierta.

```bash
gcloud datastream connection-profiles create pg-source-profile \
    --location="$REGION" --type=postgresql \
    --display-name="ShopLatam PostgreSQL" \
    --postgresql-hostname="$PG_IP" --postgresql-port=5432 \
    --postgresql-username=datastream_user --postgresql-password="StreamSecret2026!" \
    --postgresql-database=shoplatam_db

gcloud datastream connection-profiles create bq-dest-profile \
    --location="$REGION" --type=bigquery --display-name="ShopLatam BigQuery (Iceberg)"

cat <<'EOF' > pg_source.json
{
  "includeObjects": { "postgresqlSchemas": [ { "schema": "shop" } ] },
  "replicationSlot": "datastream_slot",
  "publication": "datastream_pub"
}
EOF

cat <<EOF > bq_dest.json
{
  "singleTargetDataset": { "datasetId": "${PROJECT_ID}:bronze" },
  "appendOnly": {},
  "blmtConfig": {
    "bucket": "${BRONZE_BUCKET}",
    "rootPath": "/datastream",
    "connectionName": "${PROJECT_ID}.${REGION}.shop_bronze_conn",
    "fileFormat": "PARQUET",
    "tableFormat": "ICEBERG"
  }
}
EOF

gcloud datastream streams create shop-cdc-stream \
    --location="$REGION" --display-name="ShopLatam CDC -> Bronze Iceberg" \
    --source=pg-source-profile --destination=bq-dest-profile \
    --postgresql-source-config=pg_source.json \
    --bigquery-destination-config=bq_dest.json \
    --backfill-all

gcloud datastream streams update shop-cdc-stream --location="$REGION" \
    --state=RUNNING --update-mask=state
```

Qué hace cada parte de `bq_dest.json`:

- **`blmtConfig`**: activa el destino Iceberg ("BigLake Managed Tables", el nombre que todavía usa la API). Solo admite `PARQUET` + `ICEBERG`.
- **`appendOnly`**: es **obligatorio** con Iceberg y, además, es lo que queremos en Bronze: cada `INSERT`, `UPDATE` y `DELETE` queda como una fila nueva. Bronze es el **registro inmutable de todo lo que pasó**, y desde ahí se puede reconstruir cualquier cosa.
- **`includeObjects`** con solo el esquema: replica las 9 tablas sin listarlas una por una.

### 1.4. Inspeccionar Bronze

Espera 3-5 minutos al backfill inicial y consulta en BigQuery:

```sql
-- Datastream creó una tabla Iceberg por cada tabla de origen: <esquema>_<tabla>
SELECT table_name, ddl
FROM `TU_PROYECTO_ID.bronze.INFORMATION_SCHEMA.TABLES`;

-- Anatomía de un evento CDC
SELECT order_id, status,
       datastream_metadata.change_type,
       datastream_metadata.source_timestamp,        -- milisegundos epoch, del ORIGEN
       datastream_metadata.change_sequence_number   -- desempata eventos del mismo milisegundo
FROM `TU_PROYECTO_ID.bronze.shop_orders`
ORDER BY datastream_metadata.source_timestamp DESC
LIMIT 10;
```

En el `ddl` vas a ver `table_format = 'ICEBERG'` y un `storage_uri` en `gs://…-shop-bronze/datastream/…`. Ahora mira los archivos físicos:

```bash
gcloud storage ls -r "gs://${BRONZE_BUCKET}/datastream/**" | head -20
```

Son **Parquet**: BigQuery escribe los datos en formato abierto, pero guarda los metadatos de Iceberg en su propio almacenamiento. Para que otro motor pueda leer la tabla con un catálogo Iceberg cualquiera, BigQuery **exporta** esos metadatos cuando se lo pides:

```sql
EXPORT TABLE METADATA FROM `TU_PROYECTO_ID.bronze.shop_orders`;
```

```bash
gcloud storage ls -r "gs://${BRONZE_BUCKET}/datastream/**/metadata/**"
```

Aparecen un `v….metadata.json` y archivos **`.avro`**: los *manifest list* y *manifests* de Iceberg **son Avro**. Avro sí está en el lakehouse, pero en la capa de metadatos, no en la de datos (ver [teoría §4.2](teoria.md#42-dónde-está-el-avro)).

### 1.5. Encender el tráfico continuo

Deja el generador corriendo en segundo plano durante todo el lab. Cada 20 segundos crea pedidos y mueve otros por el embudo:

```bash
nohup python generator.py traffic --host "$PG_IP" --password "$PG_PASSWORD" \
      --minutes 150 --interval 20 > /tmp/traffic.log 2>&1 &
sleep 30 && tail -3 /tmp/traffic.log
```

> [!NOTE]
> Si Cloud Shell se reinicia, el generador se detiene. Vuelve a lanzarlo con el mismo comando, después de `source ~/lab09.env && source /tmp/lab09-venv/bin/activate && cd ~/lab09`. Si el venv de `/tmp` desapareció, repite el `pip install` del 1.2.

---

## Paso 2 — Silver con Spark (25 min)

### 2.1. El job de Silver

Lee esto antes de pegarlo: es el corazón del lab. El diccionario `TABLES` es el **contrato de cada tabla** (clave primaria, limpieza, reglas y opciones físicas). Todo lo demás es **mecánica genérica** que se aplica igual a las 9 tablas. Agregar una tabla nueva al lakehouse es agregar una entrada al diccionario.

```bash
cat <<'EOF' > silver_job.py
"""Job SILVER: Bronze (Iceberg append-only escrito por Datastream) -> Silver (Iceberg, estado actual).

Por cada tabla: lee los eventos nuevos (watermark - lookback), limpia y enriquece, manda a cuarentena
lo que no pasa las reglas, se queda con el último evento por clave y hace MERGE idempotente.
"""
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampNTZType

EMAIL_RE = r"^[a-z0-9._%+-]+@[a-z0-9.-]+\.[a-z]{2,}$"
FREE_EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "outlook.com", "yahoo.com"]
CITY_ALIASES = {
    "bogota": "Bogotá", "bogotá": "Bogotá", "bogotá d.c.": "Bogotá", "medellin": "Medellín",
    "medellín": "Medellín", "cali": "Cali", "cdmx": "Ciudad de México",
    "ciudad de méxico": "Ciudad de México", "guadalajara": "Guadalajara", "monterrey": "Monterrey",
    "lima": "Lima", "arequipa": "Arequipa", "santiago": "Santiago", "santiago de chile": "Santiago",
    "valparaíso": "Valparaíso", "buenos aires": "Buenos Aires", "córdoba": "Córdoba",
}
REGIONS = {"CO": "Andina", "PE": "Andina", "MX": "Norteamérica", "CL": "Cono Sur", "AR": "Cono Sur"}
DELETE_TYPES = ["DELETE", "UPDATE-DELETE"]


def as_map(d):
    return F.create_map(*[F.lit(x) for kv in d.items() for x in kv])


# ---------- Limpieza y enriquecimiento por tabla (transformaciones puras) ----------

def clean_categories(df):
    return df.withColumn("name", F.trim("name"))


def clean_products(df):
    price = F.col("list_price_usd").cast("decimal(12,2)")
    return (df.withColumn("name", F.trim("name"))
              .withColumn("sku", F.upper(F.trim("sku")))
              .withColumn("list_price_usd", price)
              .withColumn("price_band", F.when(price < 50, "LOW").when(price < 300, "MID").otherwise("HIGH")))


def clean_inventory(df):
    return df


def clean_customers(df):
    email = F.lower(F.trim("email"))
    domain = F.when(email.contains("@"), F.substring_index(email, "@", -1))
    return (df.withColumn("email", email)
              .withColumn("email_domain", domain)
              .withColumn("is_free_email", domain.isin(FREE_EMAIL_DOMAINS))
              .withColumn("full_name", F.initcap(F.trim("full_name")))
              .withColumn("phone", F.regexp_replace("phone", r"[^0-9+]", ""))
              .withColumn("segment", F.upper(F.trim("segment"))))


def clean_addresses(df):
    city_key = F.lower(F.trim("city"))
    country = F.upper(F.trim("country_code"))
    return (df.withColumn("city", F.coalesce(as_map(CITY_ALIASES)[city_key], F.initcap(F.trim("city"))))
              .withColumn("country_code", country)
              .withColumn("region", as_map(REGIONS)[country]))


def clean_orders(df):
    return (df.withColumn("status", F.upper(F.trim("status")))
              .withColumn("currency", F.upper(F.trim("currency")))
              .withColumn("order_date", F.to_date("order_ts")))


def clean_order_items(df):
    gross = F.col("quantity") * F.col("unit_price") * (1 - F.col("discount_pct") / 100)
    return (df.withColumn("unit_price", F.col("unit_price").cast("decimal(14,2)"))
              .withColumn("discount_pct", F.col("discount_pct").cast("decimal(5,2)"))
              .withColumn("line_amount", gross.cast("decimal(16,2)")))


def clean_payments(df):
    return (df.withColumn("method", F.upper(F.trim("method")))
              .withColumn("status", F.upper(F.trim("status")))
              .withColumn("currency", F.upper(F.trim("currency")))
              .withColumn("amount", F.col("amount").cast("decimal(14,2)")))


def clean_shipments(df):
    hours = (F.unix_timestamp("delivered_at") - F.unix_timestamp("shipped_at")) / 3600
    return (df.withColumn("carrier", F.initcap(F.trim("carrier")))
              .withColumn("status", F.upper(F.trim("status")))
              .withColumn("transit_hours", F.round(hours, 2)))


# ---------- Contrato de cada tabla: clave, limpieza, reglas por fila y opciones físicas ----------
# Agregar una tabla nueva al lakehouse = agregar una entrada aquí.
TABLES = {
    "categories": {"pk": ["category_id"], "clean": clean_categories,
                   "rules": lambda: [("nombre_presente", F.col("name") != "")]},
    "products": {"pk": ["product_id"], "clean": clean_products,
                 "rules": lambda: [("precio_positivo", F.col("list_price_usd") > 0)]},
    "inventory": {"pk": ["product_id"], "clean": clean_inventory,
                  "rules": lambda: [("stock_no_negativo", F.col("stock_on_hand") >= 0)]},
    "customers": {"pk": ["customer_id"], "clean": clean_customers, "history": True,
                  "rules": lambda: [("email_valido", F.col("email").rlike(EMAIL_RE)),
                                    ("segmento_valido", F.col("segment").isin("NEW", "REGULAR", "VIP"))]},
    "addresses": {"pk": ["address_id"], "clean": clean_addresses,
                  "rules": lambda: [("pais_iso2", F.col("country_code").rlike("^[A-Z]{2}$"))]},
    "orders": {"pk": ["order_id"], "clean": clean_orders, "partition": "days(order_ts)",
               "rules": lambda: [("estado_valido", F.col("status").isin(
                                     "CREATED", "PAID", "SHIPPED", "DELIVERED", "CANCELLED")),
                                 ("moneda_iso", F.col("currency").rlike("^[A-Z]{3}$"))]},
    "order_items": {"pk": ["order_id", "line_number"], "clean": clean_order_items,
                    "rules": lambda: [("cantidad_positiva", F.col("quantity") > 0),
                                      ("precio_no_negativo", F.col("unit_price") >= 0),
                                      ("descuento_0_100", F.col("discount_pct").between(0, 100))]},
    "payments": {"pk": ["payment_id"], "clean": clean_payments,
                 "rules": lambda: [("monto_positivo", F.col("amount") > 0),
                                   ("estado_pago_valido", F.col("status").isin("APPROVED", "REJECTED"))]},
    "shipments": {"pk": ["shipment_id"], "clean": clean_shipments,
                  "rules": lambda: [("entrega_despues_de_envio",
                                     F.col("delivered_at").isNull() | (F.col("delivered_at") >= F.col("shipped_at")))]},
}


# ---------- Mecánica genérica (igual para todas las tablas) ----------

def read_bronze(spark, project, table):
    """Datastream nombra las tablas <esquema>_<tabla>; aquí el esquema de origen es `shop`."""
    return spark.read.format("bigquery").load(f"{project}.bronze.shop_{table}")


def to_events(bronze_df, since):
    """Aplana datastream_metadata y normaliza a UTC los TIMESTAMP sin zona de Postgres."""
    df = (bronze_df
          .withColumn("_source_ts", F.timestamp_millis(F.col("datastream_metadata.source_timestamp")))
          .withColumn("_change_type", F.col("datastream_metadata.change_type"))
          .withColumn("_change_seq", F.col("datastream_metadata.change_sequence_number"))
          .drop("datastream_metadata")
          .where(F.col("_source_ts") >= F.lit(since)))
    for field in df.schema.fields:
        if isinstance(field.dataType, TimestampNTZType):
            df = df.withColumn(field.name, F.col(field.name).cast("timestamp_ltz"))
    return df


def apply_rules(df, rules):
    """Devuelve el DataFrame con `_dq_errors`: los nombres de las reglas que la fila no cumple.
    Los borrados no se validan: Datastream solo trae la clave primaria en un DELETE."""
    failed = [F.when(~F.coalesce(cond, F.lit(False)), F.lit(name)) for name, cond in rules]
    errors = F.filter(F.array(*failed), lambda e: e.isNotNull())
    is_delete = F.col("_change_type").isin(DELETE_TYPES)
    return df.withColumn("_dq_errors", F.when(is_delete, F.array().cast("array<string>")).otherwise(errors))


def latest_per_key(df, pk):
    w = Window.partitionBy(*pk).orderBy(F.col("_source_ts").desc(), F.col("_change_seq").desc())
    return (df.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
              .withColumn("_is_deleted", F.col("_change_type").isin(DELETE_TYPES)))


def create_if_missing(spark, name, view, partition=None):
    partitioned = f"PARTITIONED BY ({partition})" if partition else ""
    spark.sql(f"CREATE TABLE IF NOT EXISTS {name} USING iceberg {partitioned} "
              f"TBLPROPERTIES ('format-version'='2') AS SELECT * FROM {view} WHERE false")


def merge_current(spark, table, latest, pk, partition=None):
    """Upsert idempotente: correr dos veces el mismo lote deja la tabla igual.
    `>=` (y no `>`) permite reprocesar a propósito para aplicar una regla de limpieza nueva."""
    latest.createOrReplaceTempView("src")
    target = f"silver.{table}"
    create_if_missing(spark, target, "src", partition)
    on = " AND ".join(f"t.{k} = s.{k}" for k in pk)
    spark.sql(f"""
        MERGE INTO {target} t USING src s ON {on}
        WHEN MATCHED AND s._is_deleted AND s._source_ts >= t._source_ts THEN UPDATE SET
            t._is_deleted = true, t._source_ts = s._source_ts, t._change_type = s._change_type,
            t._change_seq = s._change_seq, t._processed_at = s._processed_at
        WHEN MATCHED AND NOT s._is_deleted AND s._source_ts >= t._source_ts THEN UPDATE SET *
        WHEN NOT MATCHED AND NOT s._is_deleted THEN INSERT *
    """)


def append_history(spark, table, valid, pk):
    """Historia limpia de cada versión (para el SCD2 de Gold). Idempotente por (pk, evento)."""
    hist = valid.where(~F.col("_change_type").isin(DELETE_TYPES))
    hist.createOrReplaceTempView("hist_src")
    target = f"silver.{table}_history"
    create_if_missing(spark, target, "hist_src")
    on = " AND ".join(f"t.{k} = s.{k}" for k in pk + ["_source_ts", "_change_seq"])
    spark.sql(f"MERGE INTO {target} t USING hist_src s ON {on} WHEN NOT MATCHED THEN INSERT *")


def quarantine(spark, table, invalid, run_id):
    business_cols = [c for c in invalid.columns if not c.startswith("_")]
    q = invalid.select(
        F.lit(table).alias("table_name"),
        F.to_json(F.struct(*business_cols)).alias("payload"),
        F.col("_dq_errors").alias("failed_rules"),
        "_source_ts", "_change_type",
        F.lit(run_id).alias("run_id"),
        F.current_timestamp().alias("quarantined_at"))
    q.createOrReplaceTempView("q_src")
    create_if_missing(spark, "silver.quarantine", "q_src")
    spark.sql("""
        MERGE INTO silver.quarantine t USING q_src s
        ON t.table_name = s.table_name AND t._source_ts = s._source_ts AND t.payload = s.payload
        WHEN NOT MATCHED THEN INSERT *
    """)


def get_watermark(spark, table):
    row = spark.sql(f"SELECT last_source_ts FROM ops.watermarks WHERE table_name = '{table}'").first()
    return row[0] if row else None


def set_watermark(spark, table, ts):
    spark.createDataFrame([(table, ts)], "table_name string, last_source_ts timestamp") \
         .createOrReplaceTempView("wm_src")
    spark.sql("""
        MERGE INTO ops.watermarks t USING wm_src s ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.last_source_ts = s.last_source_ts, t.updated_at = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, last_source_ts, updated_at)
                              VALUES (s.table_name, s.last_source_ts, current_timestamp())
    """)


def log_run(spark, run_id, layer, table, metrics, started_at):
    spark.createDataFrame(
        [(run_id, layer, table, metrics.get("read", 0), metrics.get("valid", 0),
          metrics.get("quarantined", 0), metrics.get("keys", 0), started_at, datetime.now(timezone.utc))],
        "run_id string, layer string, table_name string, rows_read long, rows_valid long, "
        "rows_quarantined long, keys_merged long, started_at timestamp, finished_at timestamp",
    ).writeTo("ops.pipeline_runs").append()


def ensure_ops_tables(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS ops")
    spark.sql("CREATE TABLE IF NOT EXISTS ops.watermarks (table_name string, last_source_ts timestamp, "
              "updated_at timestamp) USING iceberg")
    spark.sql("CREATE TABLE IF NOT EXISTS ops.pipeline_runs (run_id string, layer string, table_name string, "
              "rows_read long, rows_valid long, rows_quarantined long, keys_merged long, "
              "started_at timestamp, finished_at timestamp) USING iceberg")


def process_table(spark, project, table, spec, lookback, run_id):
    started_at = datetime.now(timezone.utc)
    watermark = get_watermark(spark, table)
    since = (watermark - lookback) if watermark else datetime(1970, 1, 1, tzinfo=timezone.utc)

    events = to_events(read_bronze(spark, project, table), since).cache()
    read = events.count()
    if read == 0:
        log_run(spark, run_id, "silver", table, {}, started_at)
        print(f"[{table}] sin eventos nuevos desde {since}")
        return

    checked = apply_rules(spec["clean"](events), spec["rules"]()) \
        .withColumn("_processed_at", F.current_timestamp()).cache()
    valid = checked.where(F.size("_dq_errors") == 0).drop("_dq_errors")
    invalid = checked.where(F.size("_dq_errors") > 0)

    n_invalid = invalid.count()
    if n_invalid:
        quarantine(spark, table, invalid, run_id)
    latest = latest_per_key(valid, spec["pk"])
    merge_current(spark, table, latest, spec["pk"], spec.get("partition"))
    if spec.get("history"):
        append_history(spark, table, valid, spec["pk"])

    set_watermark(spark, table, events.agg(F.max("_source_ts")).first()[0])
    metrics = {"read": read, "valid": read - n_invalid, "quarantined": n_invalid, "keys": latest.count()}
    log_run(spark, run_id, "silver", table, metrics, started_at)
    print(f"[{table}] {metrics}")
    checked.unpersist()
    events.unpersist()


def load_fx(spark, fx_csv):
    """Tasas de cambio que publica Finanzas como CSV: datos de referencia, no CDC."""
    (spark.read.option("header", True).csv(fx_csv)
          .select(F.upper("currency").alias("currency"), "year_month",
                  F.col("units_per_usd").cast("double").alias("units_per_usd"))
          .writeTo("silver.ref_fx_rates").using("iceberg").createOrReplace())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--catalog", default="shop_lake")
    parser.add_argument("--fx_csv", required=True)
    parser.add_argument("--lookback_minutes", type=int, default=30)
    parser.add_argument("--tables", default=",".join(TABLES))
    parser.add_argument("--run_id", default=f"manual-{datetime.now():%Y%m%d%H%M%S}")
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("shoplatam-silver")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.catalog.setCurrentCatalog(args.catalog)
    ensure_ops_tables(spark)

    load_fx(spark, args.fx_csv)
    lookback = timedelta(minutes=args.lookback_minutes)
    for table in args.tables.split(","):
        process_table(spark, args.project, table, TABLES[table], lookback, args.run_id)


if __name__ == "__main__":
    main()
EOF
```

El recorrido de `process_table`, paso a paso:

| Paso | Función | Qué resuelve |
|---|---|---|
| 1 | `get_watermark` + `lookback` | Lee **solo eventos nuevos**: desde el último `source_timestamp` procesado **menos 30 minutos**. El margen atrapa eventos que llegan tarde a Bronze. |
| 2 | `to_events` | Aplana `datastream_metadata` y convierte a UTC los `TIMESTAMP` sin zona de Postgres. |
| 3 | `clean_*` | **Limpieza** (trim, mayúsculas, tipos, ciudades canónicas) y **enriquecimiento** (dominio del email, región, banda de precio, horas en tránsito, importe de línea). |
| 4 | `apply_rules` | Agrega `_dq_errors`, con la lista de reglas que la fila no cumple. Los `DELETE` no se validan, porque solo traen la clave. |
| 5 | `quarantine` | Las filas inválidas van a `silver.quarantine` con el motivo, y **el pipeline sigue**. Un email mal escrito no detiene el negocio. |
| 6 | `latest_per_key` | Se queda con el último evento por clave (`source_timestamp` y luego `change_sequence_number`). |
| 7 | `merge_current` | `MERGE` **idempotente**: los borrados se marcan (`_is_deleted`) en vez de eliminarse, y un evento viejo nunca pisa uno nuevo. |
| 8 | `append_history` | Para `customers`, guarda **cada versión** limpia. Es la materia prima del SCD2 de Gold. |
| 9 | `set_watermark` + `log_run` | Avanza el watermark **solo al final**: si el job falla a mitad, la próxima corrida reprocesa sin perder nada. Además deja métricas en `ops.pipeline_runs`. |

### 2.2. Subir el código y los datos de referencia

Las tasas de cambio no vienen del sistema transaccional: las publica Finanzas una vez al mes. Son **datos de referencia** y entran como CSV:

```bash
MES=$(date -u +%Y-%m)
cat <<EOF > fx_rates.csv
currency,year_month,units_per_usd
COP,${MES},4000
MXN,${MES},18.5
PEN,${MES},3.7
CLP,${MES},950
EOF

gcloud storage cp silver_job.py "gs://${OPS_BUCKET}/jobs/"
gcloud storage cp fx_rates.csv "gs://${OPS_BUCKET}/ref/"
```

### 2.3. Primera corrida manual (antes de automatizar)

Un equipo real **nunca** sube un job a Airflow sin haberlo corrido antes a mano. Las propiedades de Spark configuran el catálogo Iceberg REST. El runtime **3.0** (Spark 4) ya trae Iceberg y el conector de BigQuery, así que no hay JARs que instalar:

```bash
export SPARK_PROPS="spark.sql.catalog.${CATALOG}=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.${CATALOG}.type=rest,\
spark.sql.catalog.${CATALOG}.uri=https://biglake.googleapis.com/iceberg/v1/restcatalog,\
spark.sql.catalog.${CATALOG}.warehouse=bl://projects/${PROJECT_ID}/catalogs/${CATALOG},\
spark.sql.catalog.${CATALOG}.header.x-goog-user-project=${PROJECT_ID},\
spark.sql.catalog.${CATALOG}.rest.auth.type=org.apache.iceberg.gcp.auth.GoogleAuthManager,\
spark.sql.catalog.${CATALOG}.io-impl=org.apache.iceberg.gcp.gcs.GCSFileIO,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.session.timeZone=UTC,spark.executor.instances=2,spark.dynamicAllocation.maxExecutors=2"
echo "export SPARK_PROPS=\"${SPARK_PROPS}\"" >> ~/lab09.env

gcloud dataproc batches submit pyspark "gs://${OPS_BUCKET}/jobs/silver_job.py" \
  --batch="silver-manual-$(date +%s)" \
  --region="$REGION" --version=3.0 \
  --service-account="$SPARK_SA" --subnet=default \
  --properties="$SPARK_PROPS" \
  -- --project="$PROJECT_ID" --fx_csv="gs://${OPS_BUCKET}/ref/fx_rates.csv"
```

El comando espera a que el batch termine (2-4 min) y muestra la salida del driver. Al final verás una línea por tabla, por ejemplo `[customers] {'read': 151, 'valid': 150, 'quarantined': 1, 'keys': 150}`.

> [!TIP]
> Si falla con `PERMISSION_DENIED` sobre el catálogo, revisa que `sa-spark-lakehouse` tenga `roles/biglake.editor` (Paso 0.2). Si falla por cuota de CPU, baja a `spark.executor.instances=1`.

### 2.4. Validar Silver **desde BigQuery**

Spark escribió las tablas, y BigQuery las lee del mismo catálogo con un **nombre de 4 partes**: `proyecto.catálogo.namespace.tabla`. No hay copias ni tablas externas que mantener:

```sql
-- Limpieza: ciudades canónicas y región enriquecida
SELECT city, region, COUNT(*) AS direcciones
FROM `TU_PROYECTO_ID.shop_lake.silver.addresses`
WHERE NOT _is_deleted
GROUP BY 1, 2 ORDER BY 1;

-- Enriquecimiento: dominio y tipo de email
SELECT email, email_domain, is_free_email, phone
FROM `TU_PROYECTO_ID.shop_lake.silver.customers` LIMIT 10;

-- Cuarentena: qué se rechazó y por qué
SELECT table_name, failed_rules, payload
FROM `TU_PROYECTO_ID.shop_lake.silver.quarantine`;

-- Observabilidad del pipeline
SELECT table_name, rows_read, rows_valid, rows_quarantined, keys_merged, finished_at
FROM `TU_PROYECTO_ID.shop_lake.ops.pipeline_runs`
ORDER BY finished_at DESC;
```

Compara con el origen: en Postgres hay `bogota`, `BOGOTÁ D.C.` y `Bogotá`, y en Silver solo **Bogotá**.

### 2.5. Prueba de idempotencia

Vuelve a correr **exactamente el mismo comando del 2.3**. Como el lookback vuelve a leer los últimos 30 minutos, el job relee eventos que ya procesó. Comprueba que nada se duplicó:

```sql
SELECT COUNT(*) AS filas, COUNT(DISTINCT order_id) AS pedidos
FROM `TU_PROYECTO_ID.shop_lake.silver.orders`;
```

`filas` y `pedidos` deben ser iguales. Esa propiedad es la que permite **reintentar sin miedo**, y es la base de todo lo que hace Airflow en el Paso 4.

---

## Paso 3 — Gold con Spark y las Reglas de Calidad (25 min)

### 3.1. El job de Gold

```bash
cat <<'EOF' > gold_job.py
"""Job GOLD: Silver (Iceberg) -> datamart Kimball en BigQuery (dataset gold_staging).

Construye dim_date, dim_customer (SCD2), dim_product (SCD1) y tres tipos de hechos:
transaccional (fact_order_lines), acumulativo (fact_order_fulfillment) y snapshot
periódico (fact_inventory_daily). Escribe en gold_staging; Airflow valida y recién
entonces publica en gold (Write-Audit-Publish).
"""
import argparse
from datetime import date, datetime, timezone

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

OPEN_END = "9999-12-31 00:00:00"    # fin "abierto" de la versión vigente de un SCD2
OPEN_START = "1900-01-01 00:00:00"  # inicio de la primera versión
MONTHS_ES = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio", "agosto",
             "septiembre", "octubre", "noviembre", "diciembre"]


def current(spark, table):
    return spark.table(f"silver.{table}").where(~F.col("_is_deleted"))


def hours_between(start, end):
    return F.round((F.unix_timestamp(end) - F.unix_timestamp(start)) / 3600, 2)


def build_dim_date(orders, snapshot_date):
    bounds = orders.agg(F.min("order_date").alias("lo")).first()
    lo = min(bounds["lo"] or snapshot_date, snapshot_date)
    spark = orders.sparkSession
    days = spark.sql(f"SELECT explode(sequence(DATE'{lo}', date_add(DATE'{snapshot_date}', 30))) AS date")
    return days.select(
        F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"), "date",
        F.year("date").alias("year"), F.quarter("date").alias("quarter"), F.month("date").alias("month"),
        F.array(*[F.lit(m) for m in MONTHS_ES])[F.month("date") - 1].alias("month_name"),
        F.dayofweek("date").alias("day_of_week"),
        F.dayofweek("date").isin(1, 7).alias("is_weekend"))


def build_dim_customer(spark):
    """SCD2 sobre `segment` (cada cambio abre una versión) + SCD1 sobre el resto (siempre el valor actual).
    La historia viene de silver.customers_history, que guarda cada evento CDC limpio."""
    w = Window.partitionBy("customer_id").orderBy("_source_ts", "_change_seq")
    changes = (spark.table("silver.customers_history")
               .select("customer_id", "segment", "_source_ts", "_change_seq")
               .withColumn("_prev", F.lag("segment").over(w))
               .where(F.col("_prev").isNull() | (F.col("_prev") != F.col("segment"))))
    wv = Window.partitionBy("customer_id").orderBy("_source_ts")
    versions = (changes
                .withColumn("_n", F.row_number().over(wv))
                # La primera versión arranca en 1900: los pedidos anteriores al backfill también deben encontrarla.
                .withColumn("valid_from", F.when(F.col("_n") == 1, F.lit(OPEN_START).cast("timestamp_ltz")).otherwise(F.col("_source_ts")))
                .withColumn("valid_to", F.coalesce(F.lead("_source_ts").over(wv), F.lit(OPEN_END).cast("timestamp_ltz")))
                .withColumn("is_current", F.col("valid_to") == F.lit(OPEN_END).cast("timestamp_ltz")))

    default_address = (current(spark, "addresses").where("is_default")
                       .select("customer_id", "city", "country_code", "region"))
    type1 = (current(spark, "customers")
             .select("customer_id", "email", "email_domain", "is_free_email", "full_name")
             .join(default_address, "customer_id", "left"))

    dim = (versions.join(type1, "customer_id", "inner")
           .select(F.xxhash64("customer_id", "valid_from").alias("customer_sk"),
                   "customer_id", "full_name", "email", "email_domain", "is_free_email",
                   "city", "country_code", "region", "segment", "valid_from", "valid_to", "is_current"))
    unknown = spark.createDataFrame(
        [(-1, -1, "Desconocido", None, None, None, None, None, None, "DESCONOCIDO", True)],
        "customer_sk long, customer_id int, full_name string, email string, email_domain string, "
        "is_free_email boolean, city string, country_code string, region string, segment string, "
        "is_current boolean",
    ).withColumn("valid_from", F.lit(OPEN_START).cast("timestamp_ltz")).withColumn("valid_to", F.lit(OPEN_END).cast("timestamp_ltz"))
    return dim.unionByName(unknown)


def build_dim_product(spark):
    cats = current(spark, "categories")
    parents = cats.select(F.col("category_id").alias("parent_id"), F.col("name").alias("parent_name"))
    flat = (cats.join(parents, "parent_id", "left")
            .select("category_id",
                    F.coalesce("parent_name", "name").alias("category_l1"),
                    F.when(F.col("parent_name").isNotNull(), F.col("name")).alias("category_l2")))
    dim = (current(spark, "products").join(flat, "category_id", "left")
           .select(F.xxhash64("product_id").alias("product_sk"), "product_id", "sku", "name",
                   "category_l1", "category_l2", "list_price_usd", "price_band", "is_active"))
    unknown = spark.createDataFrame(
        [(-1, -1, "N/A", "Desconocido", "Desconocido", None, None, None, False)],
        "product_sk long, product_id int, sku string, name string, category_l1 string, "
        "category_l2 string, list_price_usd decimal(12,2), price_band string, is_active boolean")
    return dim.unionByName(unknown)


def with_customer_sk(df, dim_customer):
    """Join point-in-time: la versión del cliente vigente en el momento del pedido."""
    d = dim_customer.select(F.col("customer_id").alias("_cid"), "customer_sk", "valid_from", "valid_to")
    cond = ((df.customer_id == d._cid) & (df.order_ts >= d.valid_from) & (df.order_ts < d.valid_to))
    return (df.join(d, cond, "left")
              .withColumn("customer_sk", F.coalesce("customer_sk", F.lit(-1)))  # dimensión que llega tarde
              .drop("_cid", "valid_from", "valid_to"))


def build_fact_order_lines(spark, orders, dim_customer, dim_product):
    items = current(spark, "order_items")
    fx = spark.table("silver.ref_fx_rates")
    products = dim_product.select("product_id", "product_sk")
    lines = (items.join(orders.select("order_id", "customer_id", "order_ts", "order_date",
                                      F.col("status").alias("order_status"), "currency"), "order_id", "inner")
             .withColumn("year_month", F.date_format("order_ts", "yyyy-MM")))
    lines = with_customer_sk(lines, dim_customer)
    return (lines.join(products, "product_id", "left")
            .join(fx, ["currency", "year_month"], "left")
            .select("order_id", "line_number", "order_date",
                    F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
                    "customer_sk", F.coalesce("product_sk", F.lit(-1)).alias("product_sk"),
                    "order_status", "currency", "quantity", "unit_price", "discount_pct",
                    F.col("line_amount").alias("line_amount_local"),
                    # Si falta la tasa de la moneda, queda NULL a propósito: la puerta de calidad lo detecta.
                    (F.col("line_amount") / F.col("units_per_usd")).cast("decimal(18,2)").alias("line_amount_usd"),
                    "order_ts"))


def build_fact_order_fulfillment(spark, orders, dim_customer, order_lines):
    paid = (current(spark, "payments").where("status = 'APPROVED'")
            .groupBy("order_id").agg(F.min("paid_at").alias("paid_at")))
    shipped = (current(spark, "shipments").groupBy("order_id")
               .agg(F.min("shipped_at").alias("shipped_at"), F.max("delivered_at").alias("delivered_at")))
    amounts = order_lines.groupBy("order_id").agg(
        F.sum("line_amount_local").alias("order_amount_local"),
        F.sum("line_amount_usd").alias("order_amount_usd"),
        F.count("*").alias("lines"))
    base = orders.select("order_id", "customer_id", "order_ts", "order_date", "status", "currency")
    base = with_customer_sk(base, dim_customer)
    return (base.join(paid, "order_id", "left").join(shipped, "order_id", "left")
            .join(amounts, "order_id", "left")
            .select("order_id", "customer_sk", "order_date",
                    F.date_format("order_date", "yyyyMMdd").cast("int").alias("date_key"),
                    F.col("status").alias("current_stage"), "currency", "lines",
                    "order_amount_local", "order_amount_usd",
                    F.col("order_ts").alias("ordered_at"), "paid_at", "shipped_at", "delivered_at",
                    hours_between("order_ts", "paid_at").alias("hours_to_pay"),
                    hours_between("paid_at", "shipped_at").alias("hours_to_ship"),
                    hours_between("shipped_at", "delivered_at").alias("hours_in_transit"),
                    hours_between("order_ts", "delivered_at").alias("hours_order_to_door")))


def build_fact_inventory_daily(spark, project, dim_product, snapshot_date):
    """Snapshot periódico: una foto del stock por día. Idempotente por fecha: re-correr
    el mismo día reemplaza esa foto y conserva las de los días anteriores."""
    today = (current(spark, "inventory").join(dim_product.select("product_id", "product_sk", "list_price_usd"),
                                              "product_id", "inner")
             .select(F.lit(snapshot_date).cast("date").alias("snapshot_date"),
                     F.lit(int(snapshot_date.strftime("%Y%m%d"))).alias("date_key"),
                     "product_sk", "stock_on_hand",
                     (F.col("stock_on_hand") * F.col("list_price_usd")).cast("decimal(18,2)")
                     .alias("stock_value_usd")))
    try:
        previous = (spark.read.format("bigquery").load(f"{project}.gold.fact_inventory_daily")
                    .where(F.col("snapshot_date") != F.lit(snapshot_date)))
        return previous.unionByName(today)
    except Exception:  # primera corrida: todavía no hay nada publicado en gold
        return today


def write_bq(df, project, table, temp_bucket, partition_field=None, cluster=None):
    writer = (df.write.format("bigquery")
              .option("writeMethod", "indirect")   # Parquet en GCS + load job (gratis en BigQuery)
              .option("temporaryGcsBucket", temp_bucket)
              .mode("overwrite"))
    if partition_field:
        writer = writer.option("partitionField", partition_field).option("partitionType", "DAY")
    if cluster:
        writer = writer.option("clusteredFields", ",".join(cluster))
    writer.save(f"{project}.gold_staging.{table}")


def reconcile(spark, orders, fact_lines, dim_customer):
    """Controles que, si fallan, detienen el pipeline antes de que Gold se publique."""
    items = current(spark, "order_items")
    expected = items.join(orders.select("order_id"), "order_id", "inner").count()
    orphans = items.join(orders.select("order_id"), "order_id", "left_anti").count()
    actual = fact_lines.count()
    overlaps = (dim_customer.where("customer_sk <> -1").groupBy("customer_id")
                .agg(F.sum(F.col("is_current").cast("int")).alias("n")).where("n <> 1").count())
    metrics = {"expected_lines": expected, "fact_lines": actual, "orphan_items": orphans,
               "customers_without_single_current": overlaps}
    print(f"[reconciliación] {metrics}")
    if expected != actual:
        raise RuntimeError(f"fact_order_lines tiene {actual} filas y Silver {expected}: el join multiplicó o perdió filas")
    if overlaps:
        raise RuntimeError(f"{overlaps} clientes no tienen exactamente una versión vigente en dim_customer")
    return metrics


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--catalog", default="shop_lake")
    parser.add_argument("--temp_bucket", required=True)
    parser.add_argument("--snapshot_date", default=date.today().isoformat())
    parser.add_argument("--run_id", default=f"manual-{datetime.now():%Y%m%d%H%M%S}")
    args = parser.parse_args()
    snapshot_date = date.fromisoformat(args.snapshot_date)
    started_at = datetime.now(timezone.utc)

    spark = (SparkSession.builder.appName("shoplatam-gold")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.catalog.setCurrentCatalog(args.catalog)

    orders = current(spark, "orders").cache()
    dim_customer = build_dim_customer(spark).cache()
    dim_product = build_dim_product(spark).cache()
    fact_lines = build_fact_order_lines(spark, orders, dim_customer, dim_product).cache()

    metrics = reconcile(spark, orders, fact_lines, dim_customer)

    p, b = args.project, args.temp_bucket
    write_bq(build_dim_date(orders, snapshot_date), p, "dim_date", b)
    write_bq(dim_customer, p, "dim_customer", b, cluster=["customer_id"])
    write_bq(dim_product, p, "dim_product", b)
    write_bq(fact_lines, p, "fact_order_lines", b, "order_date", ["customer_sk", "product_sk"])
    write_bq(build_fact_order_fulfillment(spark, orders, dim_customer, fact_lines),
             p, "fact_order_fulfillment", b, "order_date", ["current_stage"])
    write_bq(build_fact_inventory_daily(spark, p, dim_product, snapshot_date),
             p, "fact_inventory_daily", b, "snapshot_date", ["product_sk"])

    spark.createDataFrame(
        [(args.run_id, "gold", "fact_order_lines", metrics["expected_lines"], metrics["fact_lines"],
          0, metrics["fact_lines"], started_at, datetime.now(timezone.utc))],
        "run_id string, layer string, table_name string, rows_read long, rows_valid long, "
        "rows_quarantined long, keys_merged long, started_at timestamp, finished_at timestamp",
    ).writeTo("ops.pipeline_runs").append()


if __name__ == "__main__":
    main()
EOF
gcloud storage cp gold_job.py "gs://${OPS_BUCKET}/jobs/"
```

Las decisiones de modelado de este job (detalle en la [teoría §7](teoria.md#7-gold-modelado-dimensional-que-aguanta-el-mundo-real)):

| Tabla | Tipo | Grano | Lo que enseña |
|---|---|---|---|
| `dim_customer` | **SCD2** en `segment`, SCD1 en el resto | una fila por versión | Cada cambio de segmento abre una versión. La primera arranca en `1900-01-01` para que los pedidos anteriores al backfill también encuentren una. |
| `dim_product` | SCD1 | un producto | Aplana la jerarquía de categorías (`category_l1` / `category_l2`). |
| `dim_date` | Calendario | un día | Generada, no extraída de ningún origen. |
| `fact_order_lines` | **Transaccional** | una línea de pedido | *Join point-in-time* con el SCD2 y conversión a USD con la tasa del mes. |
| `fact_order_fulfillment` | **Accumulating snapshot** | un pedido | Una fila que se **actualiza** a medida que el pedido avanza: horas a pagar, a despachar y en tránsito. |
| `fact_inventory_daily` | **Periodic snapshot** | producto × día | Una foto por día. Re-correr el mismo día reemplaza la foto de ese día y conserva las anteriores. |

Además:
- **Claves sustitutas deterministas** (`xxhash64(customer_id, valid_from)`). Gold se reconstruye en cada corrida, y una clave con `ROW_NUMBER()` cambiaría de valor entre corridas y rompería cualquier tablero que la guardara.
- **Miembro desconocido (`-1`)**. Un pedido cuyo cliente todavía no llegó a Silver, o quedó en cuarentena, no se pierde: queda asignado a "Desconocido".
- **`reconcile`** detiene el job si el modelo **multiplicó o perdió filas**. Es el error más común con SCD2: si dos versiones se solapan, cada pedido se cuenta dos veces.

### 3.2. Corrida manual hacia `gold_staging`

```bash
gcloud dataproc batches submit pyspark "gs://${OPS_BUCKET}/jobs/gold_job.py" \
  --batch="gold-manual-$(date +%s)" \
  --region="$REGION" --version=3.0 \
  --service-account="$SPARK_SA" --subnet=default \
  --properties="$SPARK_PROPS" \
  -- --project="$PROJECT_ID" --temp_bucket="$OPS_BUCKET"
```

```sql
-- SCD2: clientes con más de una versión
SELECT customer_id, segment, valid_from, valid_to, is_current
FROM `TU_PROYECTO_ID.gold_staging.dim_customer`
WHERE customer_id IN (SELECT customer_id FROM `TU_PROYECTO_ID.gold_staging.dim_customer`
                      GROUP BY 1 HAVING COUNT(*) > 1)
ORDER BY customer_id, valid_from;

-- Accumulating snapshot: el embudo logístico
SELECT current_stage, COUNT(*) AS pedidos,
       ROUND(AVG(hours_to_pay), 3) AS h_pago, ROUND(AVG(hours_order_to_door), 3) AS h_puerta
FROM `TU_PROYECTO_ID.gold_staging.fact_order_fulfillment`
GROUP BY 1;
```

> [!NOTE]
> Las horas salen muy pequeñas porque el generador mueve un pedido de punta a punta en minutos. Con datos reales serían horas o días. La lógica es la misma.

### 3.3. La puerta de calidad: escaneos de Knowledge Catalog

Silver valida **fila por fila** (¿este email es válido?). Hay errores que solo se ven **sobre la tabla completa**: ¿hay líneas duplicadas?, ¿cuántos pedidos quedaron sin cliente?, ¿cada cliente tiene exactamente una versión vigente? Para eso existe la segunda puerta. Es el equivalente de las *assertions* de Dataform, pero independiente del motor que escribió los datos.

```bash
cat <<'EOF' > dq_fact_order_lines.yaml
rules:
- name: monto-usd-completo
  column: line_amount_usd
  dimension: COMPLETENESS
  nonNullExpectation: {}
  threshold: 1.0
- name: cantidad-positiva
  column: quantity
  dimension: VALIDITY
  rangeExpectation:
    minValue: '1'
  threshold: 1.0
- name: linea-unica
  dimension: UNIQUENESS
  sqlAssertion:
    sqlStatement: SELECT order_id, line_number FROM ${data()} GROUP BY 1, 2 HAVING COUNT(*) > 1
- name: clientes-desconocidos-max-5pct
  dimension: CONSISTENCY
  tableConditionExpectation:
    sqlExpression: COUNTIF(customer_sk = -1) <= 0.05 * COUNT(*)
postScanActions:
  bigqueryExport:
    resultsTable: //bigquery.googleapis.com/projects/TU_PROYECTO_ID/datasets/quality/tables/dq_results
EOF

cat <<'EOF' > dq_dim_customer.yaml
rules:
- name: una-version-vigente
  dimension: UNIQUENESS
  sqlAssertion:
    sqlStatement: >-
      SELECT customer_id FROM ${data()} WHERE customer_sk <> -1
      GROUP BY customer_id HAVING COUNTIF(is_current) <> 1
- name: vigencia-coherente
  dimension: VALIDITY
  rowConditionExpectation:
    sqlExpression: valid_from < valid_to
  threshold: 1.0
- name: segmento-conocido
  column: segment
  dimension: VALIDITY
  setExpectation:
    values: ['NEW', 'REGULAR', 'VIP', 'DESCONOCIDO']
  threshold: 1.0
postScanActions:
  bigqueryExport:
    resultsTable: //bigquery.googleapis.com/projects/TU_PROYECTO_ID/datasets/quality/tables/dq_results
EOF
sed -i "s/TU_PROYECTO_ID/${PROJECT_ID}/" dq_fact_order_lines.yaml dq_dim_customer.yaml

gcloud dataplex datascans create data-quality dq-fact-order-lines \
  --location="$REGION" --data-quality-spec-file=dq_fact_order_lines.yaml \
  --data-source-resource="//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/gold_staging/tables/fact_order_lines"

gcloud dataplex datascans create data-quality dq-dim-customer \
  --location="$REGION" --data-quality-spec-file=dq_dim_customer.yaml \
  --data-source-resource="//bigquery.googleapis.com/projects/${PROJECT_ID}/datasets/gold_staging/tables/dim_customer"

# Prueba manual de uno de ellos
gcloud dataplex datascans run dq-fact-order-lines --location="$REGION"
```

En 1-2 minutos, el resultado aparece en **Consola → Knowledge Catalog → Data profiling & quality → `dq-fact-order-lines`** y en la tabla `quality.dq_results`. En este momento todas las reglas deberían pasar.

> [!NOTE]
> Knowledge Catalog también puede escanear **directamente tablas Iceberg del catálogo REST** (Silver). Aquí se escanea `gold_staging` porque es justo el punto donde una falla debe **impedir la publicación**.

---

## Paso 4 — Orquestación con Airflow 3 (25 min)

### 4.1. Los DAGs

Cuatro archivos, cada uno con una sola responsabilidad:

```bash
mkdir -p dags
cat <<'EOF' > dags/shop_common.py
"""Configuración compartida de los DAGs de ShopLatam (no define ningún DAG)."""
import logging
import os

from airflow.sdk import Asset

PROJECT = os.environ["SHOP_PROJECT"]
REGION = os.environ["SHOP_REGION"]
OPS_BUCKET = os.environ["SHOP_OPS_BUCKET"]
SPARK_SA = os.environ["SHOP_SPARK_SA"]
CATALOG = os.environ.get("SHOP_CATALOG", "shop_lake")

# El DAG de Silver lo "produce" al terminar; el de Gold se dispara cuando se actualiza.
SILVER_READY = Asset(name="shop_silver_ready", uri=f"gs://{OPS_BUCKET}/assets/silver_ready")

SPARK_PROPERTIES = {
    f"spark.sql.catalog.{CATALOG}": "org.apache.iceberg.spark.SparkCatalog",
    f"spark.sql.catalog.{CATALOG}.type": "rest",
    f"spark.sql.catalog.{CATALOG}.uri": "https://biglake.googleapis.com/iceberg/v1/restcatalog",
    f"spark.sql.catalog.{CATALOG}.warehouse": f"bl://projects/{PROJECT}/catalogs/{CATALOG}",
    f"spark.sql.catalog.{CATALOG}.header.x-goog-user-project": PROJECT,
    f"spark.sql.catalog.{CATALOG}.rest.auth.type": "org.apache.iceberg.gcp.auth.GoogleAuthManager",
    f"spark.sql.catalog.{CATALOG}.io-impl": "org.apache.iceberg.gcp.gcs.GCSFileIO",
    "spark.sql.extensions": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    "spark.sql.session.timeZone": "UTC",
    # Tamaño mínimo: 1 driver + 2 executors de 4 vCPU (12 vCPU por batch)
    "spark.executor.instances": "2",
    "spark.dynamicAllocation.maxExecutors": "2",
}


def spark_batch(job_file, args):
    """Cuerpo de un batch de Managed Service for Apache Spark (serverless, runtime 3.0 = Spark 4)."""
    return {
        "pyspark_batch": {"main_python_file_uri": f"gs://{OPS_BUCKET}/jobs/{job_file}", "args": args},
        "runtime_config": {"version": "3.0", "properties": SPARK_PROPERTIES},
        "environment_config": {"execution_config": {
            "service_account": SPARK_SA,
            "subnetwork_uri": f"projects/{PROJECT}/regions/{REGION}/subnetworks/default",
        }},
    }


def alert_on_failure(context):
    """En producción esto llama a PagerDuty/Slack/Chat; aquí deja una línea fácil de alertar en Cloud Logging."""
    ti = context["task_instance"]
    logging.getLogger("airflow.task").error(
        "PIPELINE_ALERT dag=%s task=%s run=%s", ti.dag_id, ti.task_id, context["run_id"])
EOF

cat <<'EOF' > dags/shop_silver_dag.py
"""Bronze -> Silver cada 30 minutos. Al terminar actualiza el Asset que dispara Gold."""
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sdk import DAG, Param

from shop_common import (OPS_BUCKET, PROJECT, REGION, SILVER_READY, alert_on_failure,
                         spark_batch)

with DAG(
    dag_id="shop_silver",
    schedule="*/30 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # dos MERGE simultáneos sobre la misma tabla = conflicto de commit
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": alert_on_failure},
    # Reprocesar = disparar con un lookback grande; el MERGE idempotente lo hace seguro.
    params={"lookback_minutes": Param(30, type="integer", minimum=0)},
    tags=["shoplatam", "silver"],
) as dag:
    DataprocCreateBatchOperator(
        task_id="bronze_to_silver",
        project_id=PROJECT,
        region=REGION,
        # Un id nuevo por intento: reintentar no choca con el batch fallido anterior
        batch_id="silver-{{ dag_run.run_after.strftime('%Y%m%d-%H%M%S') }}-{{ ti.try_number }}",
        batch=spark_batch("silver_job.py", [
            "--project", PROJECT,
            "--fx_csv", f"gs://{OPS_BUCKET}/ref/fx_rates.csv",
            "--lookback_minutes", "{{ params.lookback_minutes }}",
            "--run_id", "{{ run_id }}",
        ]),
        outlets=[SILVER_READY],
    )
EOF

cat <<'EOF' > dags/shop_gold_dag.py
"""Silver -> Gold con Write-Audit-Publish: construye en gold_staging, valida con
Knowledge Catalog y solo si pasa copia a gold (lo que ven BI y los analistas)."""
from datetime import datetime, timedelta

from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.dataplex import DataplexRunDataQualityScanOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sdk import DAG

from shop_common import OPS_BUCKET, PROJECT, REGION, SILVER_READY, alert_on_failure, spark_batch

GOLD_TABLES = ["dim_date", "dim_customer", "dim_product",
               "fact_order_lines", "fact_order_fulfillment", "fact_inventory_daily"]
QUALITY_SCANS = ["dq-fact-order-lines", "dq-dim-customer"]


def copy_job(table):
    ref = lambda dataset: {"projectId": PROJECT, "datasetId": dataset, "tableId": table}
    return {"copy": {"sourceTable": ref("gold_staging"), "destinationTable": ref("gold"),
                     "writeDisposition": "WRITE_TRUNCATE", "createDisposition": "CREATE_IF_NEEDED"}}


with DAG(
    dag_id="shop_gold",
    schedule=[SILVER_READY],  # data-aware: corre cuando Silver termina, no a una hora fija
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2),
                  "on_failure_callback": alert_on_failure},
    tags=["shoplatam", "gold"],
) as dag:
    write = DataprocCreateBatchOperator(
        task_id="write_gold_staging",
        project_id=PROJECT,
        region=REGION,
        batch_id="gold-{{ dag_run.run_after.strftime('%Y%m%d-%H%M%S') }}-{{ ti.try_number }}",
        batch=spark_batch("gold_job.py", [
            "--project", PROJECT,
            "--temp_bucket", OPS_BUCKET,
            "--snapshot_date", "{{ (dag_run.logical_date or dag_run.run_after).strftime('%Y-%m-%d') }}",
            "--run_id", "{{ run_id }}",
        ]),
    )

    # Audit: una tarea por escaneo (dynamic task mapping). Si una regla falla, la tarea falla.
    audit = DataplexRunDataQualityScanOperator.partial(
        task_id="audit_quality",
        project_id=PROJECT,
        region=REGION,
        fail_on_dq_failure=True,
        retries=0,  # un dato malo no se arregla reintentando
    ).expand(data_scan_id=QUALITY_SCANS)

    # Publish: copia staging -> gold. Los copy jobs de BigQuery no cobran.
    publish = BigQueryInsertJobOperator.partial(
        task_id="publish",
        project_id=PROJECT,
        location=REGION,
    ).expand(configuration=[copy_job(t) for t in GOLD_TABLES])

    write >> audit >> publish
EOF

cat <<'EOF' > dags/shop_maintenance_dag.py
"""Mantenimiento semanal de las tablas Iceberg de Silver."""
from datetime import datetime

from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.sdk import DAG

from shop_common import PROJECT, REGION, alert_on_failure, spark_batch

with DAG(
    dag_id="shop_iceberg_maintenance",
    schedule="0 3 * * 0",  # domingos 03:00 UTC, fuera de la ventana de carga
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args={"on_failure_callback": alert_on_failure},
    tags=["shoplatam", "maintenance"],
) as dag:
    DataprocCreateBatchOperator(
        task_id="compact_and_expire",
        project_id=PROJECT,
        region=REGION,
        batch_id="maint-{{ dag_run.run_after.strftime('%Y%m%d-%H%M%S') }}-{{ ti.try_number }}",
        batch=spark_batch("maintenance_job.py", ["--namespace", "silver"]),
    )
EOF

# El job que corre el DAG de mantenimiento (se explica en el Paso 6.3)
cat <<'EOF' > maintenance_job.py
"""Job de MANTENIMIENTO Iceberg: compacta archivos chicos, reescribe manifests y expira snapshots viejos."""
import argparse
from datetime import datetime, timedelta, timezone

from pyspark.sql import SparkSession


def file_stats(spark, table):
    row = spark.sql(f"SELECT count(*) AS files, coalesce(sum(file_size_in_bytes), 0) AS bytes "
                    f"FROM {table}.files").first()
    return row["files"], row["bytes"]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="shop_lake")
    parser.add_argument("--namespace", default="silver")
    parser.add_argument("--older_than_hours", type=int, default=72)
    parser.add_argument("--retain_last", type=int, default=10)
    args = parser.parse_args()

    spark = (SparkSession.builder.appName("shoplatam-maintenance")
             .config("spark.sql.session.timeZone", "UTC").getOrCreate())
    spark.catalog.setCurrentCatalog(args.catalog)
    cutoff = (datetime.now(timezone.utc) - timedelta(hours=args.older_than_hours)).strftime("%Y-%m-%d %H:%M:%S")
    system = f"{args.catalog}.system"

    for row in spark.sql(f"SHOW TABLES IN {args.namespace}").collect():
        table = f"{args.namespace}.{row['tableName']}"
        files_before, _ = file_stats(spark, table)
        # 1. Compactación: junta los archivos chicos que deja cada MERGE en archivos de ~512 MB
        spark.sql(f"CALL {system}.rewrite_data_files(table => '{table}', "
                  f"options => map('min-input-files', '2'))").collect()
        # 2. Manifests: menos archivos de metadata que leer al planear cada consulta
        spark.sql(f"CALL {system}.rewrite_manifests(table => '{table}')").collect()
        # 3. Expirar snapshots: libera almacenamiento, pero acorta la ventana de time travel
        spark.sql(f"CALL {system}.expire_snapshots(table => '{table}', "
                  f"older_than => TIMESTAMP '{cutoff}', retain_last => {args.retain_last})").collect()
        files_after, _ = file_stats(spark, table)
        print(f"[{table}] archivos de datos: {files_before} -> {files_after}")


if __name__ == "__main__":
    main()
EOF
```

Las prácticas de producción que aplican estos DAGs:

- **El DAG orquesta, no transforma.** No hay ni una línea de lógica de datos en Airflow: solo lanza batches, valida y copia. Toda la transformación vive en los jobs, que se pueden probar fuera de Airflow, como hiciste en los Pasos 2 y 3.
- **Data-aware scheduling.** `shop_gold` no tiene horario: corre cuando `shop_silver` actualiza el **Asset** `shop_silver_ready` (en Airflow 2 se llamaban *Datasets*). Si Silver tarda 4 minutos o 12, Gold siempre arranca justo después.
- **Write-Audit-Publish.** `write_gold_staging >> audit_quality >> publish`. Si la auditoría falla, `publish` queda en `upstream_failed` y **`gold` no cambia**: negocio sigue viendo los últimos datos buenos.
- **Dynamic task mapping.** `.partial(...).expand(...)` crea una tarea por escaneo y una por tabla a copiar a partir de una lista. Agregar una tabla es cambiar una lista, no copiar bloques de código.
- **Reprocesar es un parámetro.** `lookback_minutes` se puede subir al disparar el DAG. Es el *backfill* de un pipeline CDC.
- **`max_active_runs=1`** evita dos `MERGE` simultáneos sobre la misma tabla Iceberg.

### 4.2. Desplegar

```bash
until [[ "$(gcloud composer environments describe "$AIRFLOW_ENV" --location="$REGION" --format='value(state)')" == "RUNNING" ]]; do
  echo "Esperando al entorno de Airflow..."; sleep 30
done

gcloud storage cp maintenance_job.py "gs://${OPS_BUCKET}/jobs/"
for f in dags/*.py; do
  gcloud composer environments storage dags import \
    --environment="$AIRFLOW_ENV" --location="$REGION" --source="$f"
done

gcloud composer environments describe "$AIRFLOW_ENV" --location="$REGION" \
  --format="value(config.airflowUri)"
```

Abre la URL de la UI de Airflow. En 1-2 minutos aparecen los tres DAGs (`shop_silver`, `shop_gold`, `shop_iceberg_maintenance`). Actívalos y dispara Silver:

```bash
for DAG in shop_silver shop_gold; do
  gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags unpause -- "$DAG"
done
gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags trigger -- shop_silver
```

En la UI:
1. **`shop_silver`** corre su batch (2-4 min). Puedes seguirlo también en **Consola → Managed Service for Apache Spark → Batches**.
2. Al terminar, en **Assets** verás `shop_silver_ready` actualizado, y **`shop_gold` arranca solo**.
3. En `shop_gold`, la vista de grilla muestra `write_gold_staging`, luego 2 instancias de `audit_quality` y luego 6 de `publish`.

### 4.3. Consumir Gold

```sql
-- Ventas en USD por país y categoría
SELECT c.country_code, p.category_l1, ROUND(SUM(f.line_amount_usd), 2) AS ventas_usd
FROM `TU_PROYECTO_ID.gold.fact_order_lines` f
JOIN `TU_PROYECTO_ID.gold.dim_customer` c USING (customer_sk)
JOIN `TU_PROYECTO_ID.gold.dim_product`  p USING (product_sk)
WHERE f.order_status <> 'CANCELLED'
GROUP BY 1, 2 ORDER BY ventas_usd DESC;

-- El valor del SCD2: segmento AL MOMENTO de comprar vs. segmento HOY
SELECT d.segment AS segmento_al_comprar, hoy.segment AS segmento_hoy,
       COUNT(DISTINCT f.order_id) AS pedidos, ROUND(SUM(f.line_amount_usd), 2) AS ventas_usd
FROM `TU_PROYECTO_ID.gold.fact_order_lines` f
JOIN `TU_PROYECTO_ID.gold.dim_customer` d   USING (customer_sk)
JOIN `TU_PROYECTO_ID.gold.dim_customer` hoy ON hoy.customer_id = d.customer_id AND hoy.is_current
GROUP BY 1, 2 ORDER BY 1, 2;

-- Snapshot periódico: valor del inventario por categoría hoy
SELECT p.category_l1, SUM(i.stock_on_hand) AS unidades, SUM(i.stock_value_usd) AS valor_usd
FROM `TU_PROYECTO_ID.gold.fact_inventory_daily` i
JOIN `TU_PROYECTO_ID.gold.dim_product` p USING (product_sk)
WHERE i.snapshot_date = CURRENT_DATE()
GROUP BY 1 ORDER BY valor_usd DESC;

-- Frescura: ¿qué tan atrasado está Gold respecto del origen?
SELECT TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(order_ts), MINUTE) AS minutos_de_atraso
FROM `TU_PROYECTO_ID.gold.fact_order_lines`;
```

Sin el SCD2, la segunda consulta atribuiría las compras que un cliente hizo como `NEW` a su segmento **actual** (`VIP`), y el área comercial sobreestimaría cuánto compran los VIP.

---

## Paso 5 — 🚨 Incidente: ShopLatam Lanza Argentina (25 min)

Es lunes. Comercial lanzó la operación en **Argentina** sin avisarle a datos, y el equipo de devoluciones desplegó un bug que registra **cantidades negativas**. Nadie te avisa: te enteras por el pipeline.

### 5.1. Provocar el incidente

```bash
python generator.py chaos --host "$PG_IP" --password "$PG_PASSWORD"
sleep 60
gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags trigger -- shop_silver
```

### 5.2. Diagnosticar

Espera a que terminen ambos DAGs (~8 min). En la UI de Airflow:

- **`shop_silver`: ✅ éxito.** Las 3 líneas con cantidad negativa son un problema **por fila**, así que fueron a cuarentena y el resto siguió.
- **`shop_gold`: ❌ falla en `audit_quality[0]`** (`dq-fact-order-lines`), y `publish` queda en `upstream_failed`.

```sql
-- 1) Silver atrapó las cantidades negativas
SELECT table_name, failed_rules, payload, quarantined_at
FROM `TU_PROYECTO_ID.shop_lake.silver.quarantine`
WHERE table_name = 'order_items';

-- 2) Staging tiene líneas en ARS sin monto en USD: no hay tasa de cambio para ARS
SELECT currency, COUNT(*) AS lineas, COUNT(line_amount_usd) AS con_usd
FROM `TU_PROYECTO_ID.gold_staging.fact_order_lines` GROUP BY 1;

-- 3) Pero Gold publicado NO tiene ARS: negocio sigue viendo datos consistentes
SELECT currency, COUNT(*) AS lineas
FROM `TU_PROYECTO_ID.gold.fact_order_lines` GROUP BY 1;

-- 4) La evidencia de la puerta de calidad
SELECT * FROM `TU_PROYECTO_ID.quality.dq_results` LIMIT 50;  -- una fila por regla y ejecución
```

Y la alerta que dejó el `on_failure_callback`:

```bash
gcloud logging read 'textPayload:"PIPELINE_ALERT"' --freshness=1h --limit=5 \
  --format="value(timestamp,textPayload)"
```

> [!IMPORTANT]
> Este es **el** momento del lab. Sin Write-Audit-Publish, el tablero de ventas de hoy habría mostrado ventas en Argentina **en cero dólares**, y alguien habría tomado decisiones con ese número. Con WAP, el pipeline falla en voz alta y **lo publicado sigue siendo correcto**.

### 5.3. Corregir y re-ejecutar

Hay dos problemas distintos, y cada uno tiene un dueño distinto:

| Problema | Tipo | Dónde se detectó | Corrección | Dueño |
|---|---|---|---|---|
| Cantidades negativas | Por fila | Silver (cuarentena) | Arreglar el bug en la app de devoluciones; los datos corregidos entrarán por CDC | Equipo de devoluciones |
| ARS sin tasa | Datos de referencia faltantes | Gold (puerta de calidad) | Publicar la tasa de ARS | Finanzas / datos |

Corrige el segundo, que te toca a ti:

```bash
echo "ARS,$(date -u +%Y-%m),1450" >> fx_rates.csv
gcloud storage cp fx_rates.csv "gs://${OPS_BUCKET}/ref/"
gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags trigger -- shop_silver
```

Silver recarga `ref_fx_rates`, Gold se dispara por el Asset, la auditoría pasa y `publish` copia a `gold`:

```sql
SELECT currency, COUNT(*) AS lineas, ROUND(SUM(line_amount_usd), 2) AS ventas_usd
FROM `TU_PROYECTO_ID.gold.fact_order_lines` GROUP BY 1;
```

Ahora aparece **ARS con su monto en USD**.

> [!TIP]
> **¿Por qué la conversión a USD está en Gold y no en Silver?** Si Silver hubiera guardado `amount_usd = NULL` para ARS, al publicar la tasa habría que reprocesar Silver con un lookback de días para recalcular esas filas. Uniendo la tasa en Gold, que se reconstruye en cada corrida, la corrección se aplica sola. **Regla práctica: los datos de referencia que pueden corregirse se unen lo más tarde posible.**

> [!NOTE]
> **Reprocesar a propósito.** Si cambias una regla de limpieza y quieres aplicarla a los datos de la última semana, dispara Silver con un lookback grande. El `MERGE` idempotente hace que sea seguro:
> `gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags trigger -- shop_silver --conf '{"lookback_minutes": 10080}'`

---

## Paso 6 — Iceberg en Serio: Rollback y Mantenimiento (15 min)

### 6.1. Un despliegue con un bug

Alguien corre un "fix" de segmentos que marca a **todos** los clientes como VIP. Anota la hora **antes** de que ocurra, como harías al revisar el registro de despliegues:

```bash
export BEFORE_OOPS=$(date -u +%Y-%m-%dT%H:%M:%S)
sleep 5

cat <<'EOF' > oops.sql
-- Simula un despliegue con un bug: un "fix" de segmentos que marca a TODOS los clientes como VIP.
UPDATE shop_lake.silver.customers SET segment = 'VIP';
SELECT segment, count(*) AS clientes FROM shop_lake.silver.customers GROUP BY segment;
EOF
gcloud storage cp oops.sql "gs://${OPS_BUCKET}/jobs/"

gcloud dataproc batches submit spark-sql "gs://${OPS_BUCKET}/jobs/oops.sql" \
  --batch="oops-$(date +%s)" --region="$REGION" --version=3.0 \
  --service-account="$SPARK_SA" --subnet=default --properties="$SPARK_PROPS"
```

```sql
SELECT segment, COUNT(*) FROM `TU_PROYECTO_ID.shop_lake.silver.customers` GROUP BY 1;
-- todos VIP 😱
```

### 6.2. Time travel y rollback

Cada `MERGE` y cada `UPDATE` crearon un **snapshot**. Iceberg puede leer la tabla como estaba en cualquiera de ellos y volver atrás **sin copiar datos**: solo cambia a qué snapshot apunta la tabla.

```bash
cat <<'EOF' > time_travel.sql
-- 1. Historial de commits de la tabla: cada MERGE, UPDATE o compactación es un snapshot
SELECT committed_at, snapshot_id, operation,
       summary['added-records'] AS filas_agregadas, summary['deleted-records'] AS filas_borradas
FROM shop_lake.silver.customers.snapshots
ORDER BY committed_at;

-- 2. Time travel: cómo se veía la tabla ANTES del despliegue con el bug
SELECT segment, count(*) AS clientes
FROM shop_lake.silver.customers TIMESTAMP AS OF '${before_oops}'
GROUP BY segment;

-- 3. Rollback: la tabla vuelve a apuntar al último snapshot anterior a ese instante
CALL shop_lake.system.rollback_to_timestamp('silver.customers', TIMESTAMP '${before_oops}');

SELECT segment, count(*) AS clientes FROM shop_lake.silver.customers GROUP BY segment;
EOF
gcloud storage cp time_travel.sql "gs://${OPS_BUCKET}/jobs/"

gcloud dataproc batches submit spark-sql "gs://${OPS_BUCKET}/jobs/time_travel.sql" \
  --batch="rollback-$(date +%s)" --region="$REGION" --version=3.0 \
  --service-account="$SPARK_SA" --subnet=default --properties="$SPARK_PROPS" \
  --vars="before_oops=${BEFORE_OOPS}"
```

En la salida verás la lista de snapshots (`append`, `overwrite`…), los segmentos como estaban antes del bug y el resultado del `rollback_to_timestamp`. Confírmalo desde BigQuery:

```sql
SELECT segment, COUNT(*) FROM `TU_PROYECTO_ID.shop_lake.silver.customers` GROUP BY 1;
-- NEW / REGULAR / VIP otra vez
```

> [!NOTE]
> El rollback es **por tabla**. Aquí alcanza, porque el bug solo tocó `silver.customers`. Gold no se contaminó, ya que el SCD2 se construye desde `customers_history`, que el bug no tocó. Si un error afecta a varias tablas, hay que hacer rollback de cada una (ver [teoría §10](teoria.md#10-mantenimiento-de-tablas-iceberg)).

### 6.3. Mantenimiento: compactación y expiración

Cada corrida de Silver deja archivos pequeños y snapshots nuevos. Con el tiempo, eso degrada las lecturas y llena el almacenamiento. El DAG `shop_iceberg_maintenance` (con el `maintenance_job.py` que creaste en el Paso 4.1) corre semanalmente; córrelo ahora a mano:

```bash
gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags unpause -- shop_iceberg_maintenance
gcloud composer environments run "$AIRFLOW_ENV" --location="$REGION" dags trigger -- shop_iceberg_maintenance
```

En los logs del batch verás líneas como `[silver.quarantine] archivos de datos: 6 -> 1`. `expire_snapshots` conserva las últimas 72 horas, así que el time travel del 6.2 sigue funcionando. **Expirar snapshots es exactamente lo que acorta la ventana de time travel:** la retención es una decisión de negocio, no solo técnica.

---

## Paso 7 — Hibernar o Limpiar (10 min)

### 7.1. Si vas a continuar otro día: hibernar

```bash
pkill -f "generator.py traffic" || true
gcloud beta composer environments hibernate "$AIRFLOW_ENV" --location="$REGION"
gcloud sql instances patch "$PG_INSTANCE" --activation-policy=NEVER
# Para retomar: 'gcloud beta composer environments resume ...' y '--activation-policy=ALWAYS'
```

> [!WARNING]
> Con la instancia detenida, el slot de replicación no avanza y Datastream marca error, pero el stream **sigue existiendo y cobrando** si procesa datos. Si la pausa va a ser larga, pausa también el stream (`--state=PAUSED`, como en el 7.2).

### 7.2. Limpieza total

**Si creaste un proyecto solo para este lab (recomendado), esto borra todo:**

```bash
gcloud projects delete "$PROJECT_ID"
```

**Si usaste un proyecto compartido**, borra recurso por recurso:

```bash
pkill -f "generator.py traffic" || true

# 1. Airflow (el entorno y su bucket)
AIRFLOW_BUCKET=$(gcloud composer environments describe "$AIRFLOW_ENV" --location="$REGION" \
                 --format="value(config.dagGcsPrefix)" | sed 's#/dags##')
gcloud composer environments delete "$AIRFLOW_ENV" --location="$REGION" --quiet --async

# 2. Datastream (pausar no es instantáneo: espera a PAUSED antes de borrar)
gcloud datastream streams update shop-cdc-stream --location="$REGION" --state=PAUSED --update-mask=state
until [[ "$(gcloud datastream streams describe shop-cdc-stream --location="$REGION" --format='value(state)')" == "PAUSED" ]]; do
  sleep 10
done
gcloud datastream streams delete shop-cdc-stream --location="$REGION" --quiet
gcloud datastream connection-profiles delete pg-source-profile --location="$REGION" --quiet
gcloud datastream connection-profiles delete bq-dest-profile --location="$REGION" --quiet

# 3. Cloud SQL
gcloud sql instances delete "$PG_INSTANCE" --quiet

# 4. Silver: borrar tablas Iceberg y el catálogo
cat <<'EOF' > drop_lakehouse.sql
-- Borra las tablas Iceberg del catálogo y sus archivos (PURGE) antes de eliminar el catálogo.
DROP TABLE IF EXISTS shop_lake.silver.categories PURGE;
DROP TABLE IF EXISTS shop_lake.silver.products PURGE;
DROP TABLE IF EXISTS shop_lake.silver.inventory PURGE;
DROP TABLE IF EXISTS shop_lake.silver.customers PURGE;
DROP TABLE IF EXISTS shop_lake.silver.customers_history PURGE;
DROP TABLE IF EXISTS shop_lake.silver.addresses PURGE;
DROP TABLE IF EXISTS shop_lake.silver.orders PURGE;
DROP TABLE IF EXISTS shop_lake.silver.order_items PURGE;
DROP TABLE IF EXISTS shop_lake.silver.payments PURGE;
DROP TABLE IF EXISTS shop_lake.silver.shipments PURGE;
DROP TABLE IF EXISTS shop_lake.silver.quarantine PURGE;
DROP TABLE IF EXISTS shop_lake.silver.ref_fx_rates PURGE;
DROP TABLE IF EXISTS shop_lake.ops.watermarks PURGE;
DROP TABLE IF EXISTS shop_lake.ops.pipeline_runs PURGE;
DROP NAMESPACE IF EXISTS shop_lake.silver;
DROP NAMESPACE IF EXISTS shop_lake.ops;
EOF
gcloud storage cp drop_lakehouse.sql "gs://${OPS_BUCKET}/jobs/"
gcloud dataproc batches submit spark-sql "gs://${OPS_BUCKET}/jobs/drop_lakehouse.sql" \
  --batch="drop-$(date +%s)" --region="$REGION" --version=3.0 \
  --service-account="$SPARK_SA" --subnet=default --properties="$SPARK_PROPS"
gcloud biglake iceberg catalogs delete "$CATALOG" --project="$PROJECT_ID" --quiet

# 5. Escaneos de calidad, datasets y conexión
gcloud dataplex datascans delete dq-fact-order-lines --location="$REGION" --quiet
gcloud dataplex datascans delete dq-dim-customer --location="$REGION" --quiet
for DS in bronze gold_staging gold quality; do bq rm -r -f "${PROJECT_ID}:${DS}"; done
bq rm -f --connection "${PROJECT_ID}.${REGION}.shop_bronze_conn"

# 6. Buckets (incluido el del entorno de Airflow)
for B in "$BRONZE_BUCKET" "$LAKE_BUCKET" "$OPS_BUCKET"; do gcloud storage rm -r "gs://${B}"; done
[[ -n "$AIRFLOW_BUCKET" ]] && gcloud storage rm -r "$AIRFLOW_BUCKET"

# 7. Cuentas de servicio
gcloud iam service-accounts delete "$SPARK_SA" --quiet
gcloud iam service-accounts delete "$AIRFLOW_SA" --quiet
deactivate
```

---

## Retos Opcionales (Extensión)

### Reto 1: Evolución de esquema de punta a punta

Comercial pide un programa de lealtad: `ALTER TABLE shop.customers ADD COLUMN loyalty_tier VARCHAR(10);`. Haz que `loyalty_tier` llegue hasta `dim_customer`.

<details>
<summary>👀 Ver Pista de Solución Reto 1</summary>

Son tres saltos, y cada uno se comporta distinto:
1. **Datastream → Bronze:** verifica con `INFORMATION_SCHEMA.COLUMNS` si la columna nueva apareció en `bronze.shop_customers`. La documentación del destino Iceberg no detalla cómo propaga columnas agregadas, así que **compruébalo antes de depender de ello**.
2. **Bronze → Silver:** `UPDATE SET *` del `MERGE` falla si la fuente tiene una columna que la tabla destino no tiene. Agrega primero la columna a Silver con `ALTER TABLE shop_lake.silver.customers ADD COLUMN loyalty_tier string` (en Iceberg es un cambio solo de metadatos, no reescribe datos). Haz lo mismo en `customers_history`.
3. **Silver → Gold:** agrégala al `select` de `build_dim_customer` y decide si es SCD1 o SCD2. Esa decisión es de negocio.

</details>

### Reto 2: Write-Audit-Publish *dentro* de Iceberg con ramas

Iceberg permite escribir en una **rama** (`branch`), validarla y recién entonces hacer *fast-forward* de `main`. Implementa WAP en Silver con `spark.wap.branch` y `CALL system.fast_forward(...)`.

<details>
<summary>👀 Ver Pista de Solución Reto 2</summary>

Primero verifica que el Lakehouse runtime catalog soporte ramas y el procedimiento `fast_forward` en tu versión: no encontramos documentación oficial que lo confirme. Ten en cuenta que las ramas son **por tabla**, así que publicar 9 tablas no es atómico (es el mismo límite que tiene el `publish` del DAG de Gold, que copia tabla por tabla).

</details>

### Reto 3: Un batch por tabla con dynamic task mapping

Cambia `shop_silver` para que lance un batch por tabla (`--tables=orders`, `--tables=customers`…) con `.expand()`, y limita la concurrencia con `max_active_tis_per_dagrun`.

<details>
<summary>👀 Ver Pista de Solución Reto 3</summary>

Ganas aislamiento (una tabla que falla no frena a las demás) y reintentos más finos. Pierdes costo y tiempo: cada batch serverless tarda ~1 minuto en arrancar y usa 12 vCPU, así que 9 batches en paralelo necesitan 108 vCPU de cuota. Por eso este lab usa un batch por capa. En producción se suele agrupar por dominio.

</details>

### Reto 4: Alerta real por correo

Crea una **alerta basada en logs** en Cloud Monitoring que te envíe un correo cuando aparezca `PIPELINE_ALERT`.

<details>
<summary>👀 Ver Pista de Solución Reto 4</summary>

**Monitoring → Alerting → Create policy → Log-based alert**, con el filtro `textPayload:"PIPELINE_ALERT"` y un canal de notificación por email. Es más robusto que mandar el correo desde Airflow: si el propio entorno de Airflow falla, la alerta sigue funcionando.

</details>

### Reto 5: CI para los jobs de Spark

Los jobs de este lab se pueden probar **sin GCP**: Spark local con un catálogo Iceberg de tipo `hadoop`, y un Bronze falso con la forma de Datastream (una columna `datastream_metadata` de tipo struct). Escribe un test que corra `silver_job.process_table` dos veces y verifique la idempotencia.

<details>
<summary>👀 Ver Pista de Solución Reto 5</summary>

Reemplaza `silver_job.read_bronze` por una función que lea Parquet local, configura `spark.sql.catalog.shop_lake.type=hadoop` con un `warehouse` en un directorio temporal y usa `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12` como paquete. Así se validó este lab antes de publicarlo: la prueba local encontró dos bugs que en GCP habrían costado un batch fallido cada uno.

</details>

---

## Resumen de lo Aprendido

- **Bronze append-only es un seguro.** Guardar cada evento CDC permite reconstruir Silver, armar historia para un SCD2 y reprocesar cuando cambia una regla.
- **Un lakehouse abierto en GCP usa dos catálogos.** Datastream escribe Iceberg administrado por BigQuery, y Spark escribe Iceberg en el Lakehouse runtime catalog (REST). BigQuery lee ambos sin copias.
- **Idempotencia antes que nada.** Watermark con lookback, `MERGE` que no deja que un evento viejo pise uno nuevo, watermark que avanza al final y claves sustitutas deterministas hacen que reintentar y reprocesar sea seguro.
- **La calidad se valida en dos niveles.** Las reglas por fila mandan a cuarentena sin detener el pipeline. Las reglas por tabla, con Knowledge Catalog, bloquean la publicación con Write-Audit-Publish.
- **Airflow orquesta, no transforma.** Los Assets encadenan DAGs por datos y no por horario, y el dynamic task mapping evita copiar bloques de código.
- **Iceberg hace los errores reversibles.** Time travel y rollback deshacen un despliegue malo en segundos, y el mantenimiento (compactación, expiración) es parte del pipeline, no algo opcional.

---

## Referencias

- [Datastream — Apache Iceberg tables destination](https://docs.cloud.google.com/datastream/docs/destination-blmt)
- [Datastream — BigQuery destination (append-only y `datastream_metadata`)](https://docs.cloud.google.com/datastream/docs/destination-bigquery)
- [Lakehouse — About the Lakehouse runtime catalog](https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs)
- [Lakehouse — Set up the Iceberg REST catalog endpoint](https://docs.cloud.google.com/lakehouse/docs/set-up-lakehouse-iceberg-rest-catalog)
- [Lakehouse — Compare table types](https://docs.cloud.google.com/lakehouse/docs/lakehouse-tables)
- [Managed Service for Apache Spark — Transform data and write to an Apache Iceberg table](https://docs.cloud.google.com/managed-spark/docs/tutorials/spark-to-iceberg)
- [Managed Service for Apache Spark — Runtime 3.0](https://docs.cloud.google.com/managed-spark/docs/concepts/versions/spark-runtime-3.0)
- [Managed Service for Apache Airflow — Create environments](https://docs.cloud.google.com/composer/docs/composer-3/create-environments)
- [Managed Service for Apache Airflow — Hibernate development mode environments](https://docs.cloud.google.com/composer/docs/composer-3/hibernate-environments)
- [Knowledge Catalog — Auto data quality overview](https://docs.cloud.google.com/knowledge-catalog/docs/auto-data-quality-overview)
- [Knowledge Catalog — Use auto data quality (reglas en YAML)](https://docs.cloud.google.com/dataplex/docs/use-auto-data-quality)
- [Apache Airflow — Google provider: Dataproc, BigQuery y Knowledge Catalog operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/dataplex.html)
- [Apache Iceberg — Spark procedures](https://iceberg.apache.org/docs/latest/spark-procedures/)
