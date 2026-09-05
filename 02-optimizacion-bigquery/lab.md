# Lab 02 — BigQuery: Particionamiento, Clustering y Optimización de SQL

## Codelab paso a paso

En este laboratorio vas a **crear un dataset y varias tablas en BigQuery**, poblarlas con
datos de una **tabla pública** (viajes de taxi amarillo de Nueva York, 2018), y luego
**medir el costo y el rendimiento de tus consultas antes y después** de aplicar
particionamiento, clustering y buenas prácticas de SQL.

---

### Objetivos

Al terminar serás capaz de:

1. Crear un **dataset** desde la consola web (BigQuery Studio) y desde CLI/SQL.
2. Crear tablas con `CREATE TABLE AS SELECT` a partir de datos públicos.
3. Diseñar tablas **particionadas** (`PARTITION BY`) y **clusterizadas** (`CLUSTER BY`).
4. Usar `require_partition_filter` como control de costos.
5. **Medir** bytes facturados y *slot time* con el validador de la consola, `bq --dry_run`
   e `INFORMATION_SCHEMA.JOBS`.
6. Aplicar buenas prácticas de SQL: proyección de columnas (evitar `SELECT *`), poda de
   particiones, filtros antes del `JOIN`, orden de `JOIN`, funciones aproximadas, etc.

### Prerrequisitos

- Un proyecto de Google Cloud con la **API de BigQuery** habilitada y permisos
  `roles/bigquery.user` + `roles/bigquery.dataEditor`.
- `bq` CLI (viene con `gcloud`) o acceso a la consola web.
- En los ejemplos, reemplaza `TU_PROYECTO` por el ID de tu proyecto.

### Costo estimado

| Concepto | Cantidad en el lab | Precio (multi-región US) | Costo |
|---|---|---|---|
| Almacenamiento | ~9 GB (tabla base ×3 copias ≈ 9–12 GB lógicos, comprimido menos) | Primeros **10 GiB/mes gratis**, luego $0.02/GiB | ~$0 |
| Consultas (on-demand) | < 5 GiB escaneados en todo el lab | Primer **1 TiB/mes gratis**, luego $6.25/TiB | ~$0 |

En la práctica el laboratorio completo cae dentro de la capa gratuita. Aun así, **al final
hay un paso de limpieza** (`DROP SCHEMA`).

---

### Arquitectura del laboratorio

```
FUENTE PÚBLICA                                  TU DATASET: TU_PROYECTO.taxi_lab
bigquery-public-data                          ┌─────────────────────────────────────────┐
.new_york_taxi_trips                          │  trips_raw          (baseline, sin nada) │
  .tlc_yellow_trips_2018  ──CREATE TABLE──►   │  trips_partitioned  (partición por DÍA)  │
     (~112M filas, ~9 GB)      AS SELECT      │  trips_optimized    (partición por MES + │
  .taxi_zone_geom         ──referencia──►     │                      CLUSTER + guardrail)│
     (~263 filas, lookup de zonas)            └─────────────────────────────────────────┘
                                                              │
                                              Batería de consultas ANTES / DESPUÉS
                                              → bytes facturados, slot ms, USD aprox.
```

> ⚠️ **Ubicación**: `bigquery-public-data` vive en la multi-región **US**. Tu dataset
> destino **debe crearse también en `US`**; si lo creas en `us-central1` u otra región,
> el `CREATE TABLE ... AS SELECT` fallará con un error de *cross-region*.

---

## Paso 1 — Crear el dataset (consola web)

#### 1.1 Ruta visual en BigQuery Studio

1. Consola de Google Cloud → menú de navegación (☰) → **BigQuery** → **BigQuery Studio**.
2. En el panel **Explorer** (izquierda), localiza tu proyecto `TU_PROYECTO`.
3. Pasa el cursor sobre el nombre del proyecto y haz clic en el menú de acciones **⋮**
   (*Ver acciones*).
4. Elige **Crear conjunto de datos** (*Create dataset*).
5. En el panel lateral **Crear conjunto de datos**:
   - **ID del conjunto de datos**: `taxi_lab`
   - **Tipo de ubicación**: **Multirregión** → **US**  ← *(obligatorio, ver aviso arriba)*
   - **Vencimiento predeterminado de la tabla**: *(opcional)* activa y pon `7 días` para
     que el lab se autolimpie.
   - **Cifrado**: *Clave gestionada por Google* (por defecto).
6. Clic en **Crear conjunto de datos**.
7. El dataset aparece bajo `TU_PROYECTO` en el panel **Explorer**. Expándelo: aquí irán
   las tablas de los siguientes pasos.

```
┌─ Explorer ───────────────┐     ┌─ Crear conjunto de datos ─────────────┐
│ ▸ TU_PROYECTO        ⋮ ──┼───► │ ID del conjunto de datos │ taxi_lab   │
│     ▸ (datasets...)      │     │ Tipo de ubicación        │ Multirreg. │
│                          │     │ Región                   │ US       ▾ │
│                          │     │ ☑ Habilitar vencimiento  │ 7   días   │
│                          │     │                                       │
│                          │     │        [ Crear conjunto de datos ]    │
└──────────────────────────┘     └───────────────────────────────────────┘
```
---

<img width="1868" height="766" alt="Screenshot 2026-09-03 at 11 39 45 a m" src="https://github.com/user-attachments/assets/9f7ac690-9d76-41fc-84c7-63cdf6e4cd9c" />

---

<img width="1040" height="628" alt="Screenshot 2026-09-03 at 11 40 09 a m" src="https://github.com/user-attachments/assets/652ccf26-36f7-4a34-b255-518cb40172e8" />

---

#### 1.2 Equivalente en SQL

Pega esto en el editor de consultas de BigQuery y ejecuta (**Run**):

```sql
CREATE SCHEMA IF NOT EXISTS `TU_PROYECTO.taxi_lab`
OPTIONS (
  location = 'US',
  description = 'Lab 02 - Particionamiento, clustering y optimización de SQL',
  default_table_expiration_days = 7
);
```

#### 1.3 Equivalente en CLI

```bash
bq --location=US mk \
  --dataset \
  --description "Lab 02 - Particionamiento, clustering y optimización de SQL" \
  --default_table_expiration 604800 \
  TU_PROYECTO:taxi_lab
```

---

## Paso 2 — Explorar la tabla pública fuente (sin gastar nada)

Antes de copiar datos, inspecciónalos **sin lanzar consultas** (explorar así cuesta $0):

1. En **Explorer**, pulsa **+ Agregar** → **Star a project by name** → escribe
   `bigquery-public-data` → **Star**.
2. Expande `bigquery-public-data` → `new_york_taxi_trips` → haz clic en
   `tlc_yellow_trips_2018`.
3. Revisa las pestañas:
   - **Schema**: nombres y tipos de columnas. Anota el tipo real de
     `pickup_datetime` (debe ser `TIMESTAMP`), `payment_type`, `pickup_location_id`.
   - **Details**: número de filas (~112M), tamaño lógico (~9 GB), fecha de última
     modificación.
   - **Preview**: primeras filas **sin costo** (no ejecuta consulta).

---

<img width="869" height="578" alt="Screenshot 2026-09-03 at 11 41 50 a m" src="https://github.com/user-attachments/assets/cb14526b-4ad4-48f4-a13c-2a546a6feca2" />

---


> 📌 **Verifica los tipos**. Si en tu región `payment_type` o `pickup_location_id` son
> `INTEGER` en vez de `STRING`, ajusta los literales de los ejemplos (`'2'` → `2`).
> El resto del lab no cambia. Si `pickup_datetime` fuera `DATETIME`, usa
> `DATETIME_TRUNC` en lugar de `TIMESTAMP_TRUNC`.

#### 2.1 Estimar el costo de una consulta sin ejecutarla

- **En la consola**: escribe la consulta y mira arriba a la derecha el mensaje
  *“Esta consulta procesará X cuando se ejecute”* (validador / *dry run* gratuito).
- **En CLI**:

```bash
bq query --use_legacy_sql=false --dry_run \
'SELECT payment_type, COUNT(*) AS c
 FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
 GROUP BY payment_type'
```

<img width="1535" height="703" alt="Screenshot 2026-09-03 at 11 59 41 a m" src="https://github.com/user-attachments/assets/28167114-eabe-43a5-b137-6f9fde35dafb" />

---

## Paso 3 — Crear las tres tablas destino

Vamos a crear **la misma tabla tres veces**, con distinto diseño físico, para comparar.
En todas: proyectamos **solo las columnas que necesitamos** (primera buena práctica) y
**filtramos las fechas sucias** (la fuente tiene filas con `pickup_datetime` fuera de 2018).

#### 3.1 `trips_raw` — línea base (sin partición ni clustering)

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO.taxi_lab.trips_raw` AS
SELECT
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  rate_code,
  payment_type,
  fare_amount,
  tip_amount,
  tolls_amount,
  total_amount,
  pickup_location_id,
  dropoff_location_id
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2019-01-01');
```

#### 3.2 `trips_partitioned` — partición por DÍA

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO.taxi_lab.trips_partitioned`
PARTITION BY DATE(pickup_datetime)
AS
SELECT
  vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
  rate_code, payment_type, fare_amount, tip_amount, tolls_amount, total_amount,
  pickup_location_id, dropoff_location_id
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2019-01-01');
```

> 2018 tiene 365 días → 365 particiones (el máximo por tabla es **10.000**). El filtro de
> fechas es lo que mantiene el número de particiones acotado.

---

<img width="501" height="690" alt="Screenshot 2026-09-03 at 12 12 47 p m" src="https://github.com/user-attachments/assets/bc101dfc-381f-4d34-8d9c-1f4a81032b52" />

---


#### 3.3 `trips_optimized` — partición por MES + clustering + guardrail

```sql
CREATE OR REPLACE TABLE `TU_PROYECTO.taxi_lab.trips_optimized`
PARTITION BY TIMESTAMP_TRUNC(pickup_datetime, MONTH)
CLUSTER BY payment_type, pickup_location_id
OPTIONS (require_partition_filter = TRUE)
AS
SELECT
  vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
  rate_code, payment_type, fare_amount, tip_amount, tolls_amount, total_amount,
  pickup_location_id, dropoff_location_id
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2019-01-01');
```

- `PARTITION BY TIMESTAMP_TRUNC(..., MONTH)` → 12 particiones grandes (~9M filas c/u);
  a este tamaño el **clustering dentro de la partición** sí se nota.
- `CLUSTER BY payment_type, pickup_location_id` → ordena físicamente cada partición;
  el **orden de las columnas importa** (primero la más usada en filtros/`GROUP BY`).
- `require_partition_filter = TRUE` → toda consulta **debe** filtrar `pickup_datetime`
  o falla. Es un freno de costos.

---

<img width="510" height="735" alt="Screenshot 2026-09-03 at 12 13 46 p m" src="https://github.com/user-attachments/assets/e99ea904-da29-4c26-ad9b-f26121ee8e3e" />

---

<img width="815" height="356" alt="Screenshot 2026-09-03 at 12 09 51 p m" src="https://github.com/user-attachments/assets/fa2dea1e-0a5d-4807-a341-3c8122395295" />

---

#### 3.4 Versión CLI (opcional)

```bash
bq query --use_legacy_sql=false \
'CREATE OR REPLACE TABLE `TU_PROYECTO.taxi_lab.trips_raw` AS
 SELECT vendor_id, pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
        rate_code, payment_type, fare_amount, tip_amount, tolls_amount, total_amount,
        pickup_location_id, dropoff_location_id
 FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
 WHERE pickup_datetime >= TIMESTAMP("2018-01-01")
   AND pickup_datetime <  TIMESTAMP("2019-01-01")'
```

---

## Paso 4 — Inspeccionar la estructura física

#### 4.1 Número de particiones y filas por partición

```sql
SELECT
  table_name,
  COUNT(*)                        AS num_particiones,
  SUM(total_rows)                 AS filas_totales,
  MIN(total_rows)                 AS filas_particion_min,
  MAX(total_rows)                 AS filas_particion_max
FROM `TU_PROYECTO.taxi_lab.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name LIKE 'trips_%'
GROUP BY table_name
ORDER BY table_name;
```

Esperado (aprox.): `trips_partitioned` → ~365 particiones; `trips_optimized` → 12;
`trips_raw` → 1 partición especial (`__UNPARTITIONED__`).


<img width="827" height="229" alt="Screenshot 2026-09-03 at 12 16 59 p m" src="https://github.com/user-attachments/assets/571cf453-5d44-49bd-98e8-344d543f35d0" />

---

## Paso 5 — Batería de consultas: ANTES / DESPUÉS

Vamos a ejecutar **tres consultas fijas** contra **cada una** de las tres tablas
(9 ejecuciones) y anotar los resultados.

Para cada ejecución, mira en el panel **Job information / Execution details**:
- **Bytes processed** y **Bytes billed**
- **Slot time consumed**

#### Consulta Q1 — filtro por rango de fechas (1 mes)

```sql
-- Cambia trips_raw por trips_partitioned y trips_optimized
SELECT COUNT(*) AS viajes, ROUND(AVG(fare_amount), 2) AS tarifa_promedio
FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');
```

<img width="447" height="440" alt="Screenshot 2026-09-03 at 12 29 05 p m" src="https://github.com/user-attachments/assets/329ec6bd-c1c8-4156-89ce-f39ac08dbccd" />

---

#### Consulta Q2 — filtro por fecha + columna de cluster

```sql
SELECT payment_type, COUNT(*) AS viajes, ROUND(SUM(tip_amount), 2) AS propinas
FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND payment_type = '2'
GROUP BY payment_type;
```

<img width="419" height="443" alt="Screenshot 2026-09-03 at 12 31 01 p m" src="https://github.com/user-attachments/assets/af57fd36-f1fd-4086-9dec-f7a75a8e7f7a" />

--

#### Consulta Q3 — agregación por zona (3 meses) con `ORDER BY ... LIMIT`

```sql
SELECT
  pickup_location_id,
  COUNT(*)                      AS viajes,
  ROUND(AVG(total_amount), 2)   AS ticket_promedio,
  ROUND(AVG(trip_distance), 2)  AS distancia_promedio
FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2018-04-01')
GROUP BY pickup_location_id
ORDER BY viajes DESC
LIMIT 20;
```

<img width="434" height="399" alt="Screenshot 2026-09-03 at 12 31 45 p m" src="https://github.com/user-attachments/assets/011d23d4-014a-4ac4-957f-4e16a6c63d41" />

--

#### Consulta Q1 — filtro por rango de fechas (1 mes) - Partitioned

```sql
-- Cambia trips_raw por trips_partitioned y trips_optimized
SELECT COUNT(*) AS viajes, ROUND(AVG(fare_amount), 2) AS tarifa_promedio
FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');
```

<img width="445" height="404" alt="Screenshot 2026-09-03 at 12 32 49 p m" src="https://github.com/user-attachments/assets/244d13b8-8fff-4a31-a084-f8907dfb6d16" />

---

#### Consulta Q2 — filtro por fecha + columna de cluster - Partitioned

```sql
SELECT payment_type, COUNT(*) AS viajes, ROUND(SUM(tip_amount), 2) AS propinas
FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND payment_type = '2'
GROUP BY payment_type;
```

<img width="433" height="440" alt="Screenshot 2026-09-03 at 12 34 35 p m" src="https://github.com/user-attachments/assets/5c4a3e89-5216-4124-bd3d-d45c08964ba4" />

--

#### Consulta Q3 — agregación por zona (3 meses) con `ORDER BY ... LIMIT` - Partitioned

```sql
SELECT
  pickup_location_id,
  COUNT(*)                      AS viajes,
  ROUND(AVG(total_amount), 2)   AS ticket_promedio,
  ROUND(AVG(trip_distance), 2)  AS distancia_promedio
FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2018-04-01')
GROUP BY pickup_location_id
ORDER BY viajes DESC
LIMIT 20;
```

<img width="436" height="455" alt="Screenshot 2026-09-03 at 12 35 33 p m" src="https://github.com/user-attachments/assets/91a63b16-3ea4-486e-beab-e3840122a048" />

--

#### Consulta Q1 — filtro por rango de fechas (1 mes) - Partitioned & Clustering

```sql
-- Cambia trips_raw por trips_partitioned y trips_optimized
SELECT COUNT(*) AS viajes, ROUND(AVG(fare_amount), 2) AS tarifa_promedio
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');
```

<img width="435" height="397" alt="Screenshot 2026-09-03 at 12 46 02 p m" src="https://github.com/user-attachments/assets/95d4df2f-38d8-462e-a7eb-a90577943627" />

---

#### Consulta Q2 — filtro por fecha + columna de cluster - Partitioned & Clustering

```sql
SELECT payment_type, COUNT(*) AS viajes, ROUND(SUM(tip_amount), 2) AS propinas
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND payment_type = '2'
GROUP BY payment_type;
```

<img width="432" height="444" alt="Screenshot 2026-09-03 at 12 46 51 p m" src="https://github.com/user-attachments/assets/228b9906-d7f6-4fc6-88d0-78b0234db26b" />

--

#### Consulta Q3 — agregación por zona (3 meses) con `ORDER BY ... LIMIT` - Partitioned & Clustering

```sql
SELECT
  pickup_location_id,
  COUNT(*)                      AS viajes,
  ROUND(AVG(total_amount), 2)   AS ticket_promedio,
  ROUND(AVG(trip_distance), 2)  AS distancia_promedio
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2018-04-01')
GROUP BY pickup_location_id
ORDER BY viajes DESC
LIMIT 20;
```

<img width="425" height="403" alt="Screenshot 2026-09-03 at 12 47 30 p m" src="https://github.com/user-attachments/assets/46590ae0-1a1e-4eee-962e-e296cda6fecd" />

--

#### Tabla comparativa (rellénala con tus números)

| Consulta | Tabla | Bytes facturados | Slot ms | USD aprox. | Comentario |
|---|---|---:|---:|---:|---|
| Q1 | `trips_raw`         |   |   |   | sin poda: lee todas las filas |
| Q1 | `trips_partitioned` |   |   |   | poda: solo ~30 particiones-día |
| Q1 | `trips_optimized`   |   |   |   | poda: solo 1 partición-mes |
| Q2 | `trips_raw`         |   |   |   | |
| Q2 | `trips_partitioned` |   |   |   | poda de partición, sin cluster |
| Q2 | `trips_optimized`   |   |   |   | poda + **block pruning** por `payment_type` |
| Q3 | `trips_raw`         |   |   |   | |
| Q3 | `trips_partitioned` |   |   |   | |
| Q3 | `trips_optimized`   |   |   |   | |

**Qué deberías observar (orden de magnitud, tus cifras variarán):**

- **Q1/Q3**: `trips_raw` escanea los valores de las columnas referenciadas de **toda la
  tabla** (no hay poda). Con partición, el filtro de fecha reduce lo escaneado
  proporcionalmente al rango (~1/12 en Q1, ~1/4 en Q3) → **varias veces menos bytes**.
- **Q2**: entre `trips_partitioned` y `trips_optimized`, el clustering por `payment_type`
  recorta bloques adicionales dentro de la partición → **reducción extra** (moderada:
  depende de la selectividad del valor).
- Con particiones pequeñas puedes toparte con el **mínimo de facturación de ~10 MB por
  tabla y consulta**: es normal que consultas muy podadas “no bajen más”.

#### Medir todas las consultas de una vez con `INFORMATION_SCHEMA`

```sql
SELECT
  job_id,
  TIMESTAMP_TRUNC(creation_time, SECOND)                       AS momento,
  ROUND(total_bytes_billed / POW(1024, 3), 4)                  AS gb_facturados,
  ROUND(total_bytes_billed / POW(1024, 4) * 6.25, 6)           AS usd_aprox,
  total_slot_ms,
  SUBSTR(REGEXP_REPLACE(query, r'\s+', ' '), 1, 90)            AS consulta
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 2 HOUR)
  AND statement_type = 'SELECT'
  AND state = 'DONE'
ORDER BY creation_time DESC;
```

> `usd_aprox` es orientativo: como tienes 1 TiB/mes gratis, el costo **facturado** real
> suele ser $0. Lo que importa aquí es la **relación** de bytes entre diseños.

---

## Paso 6 — Lección A: Proyección de columnas (evitar `SELECT *`)

BigQuery es **columnar**: pagas por los **bytes de las columnas que lees**, no por filas.

Ejecuta las tres sobre `trips_raw` y compara **Bytes billed** (usa el validador, no hace
falta ni ejecutarlas):

```sql
-- A1: lee TODAS las columnas
SELECT * FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');

-- A2: solo las 2 columnas necesarias
SELECT pickup_datetime, fare_amount FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');

-- A3: casi todas, excepto un par
SELECT * EXCEPT (vendor_id, rate_code) FROM `TU_PROYECTO.taxi_lab.trips_raw`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');
```

**Observa:** A2 factura una fracción de A1. Regla: **enumera las columnas**; si de verdad
necesitas casi todas, usa `SELECT * EXCEPT (...)`. Nunca `SELECT *` para “ver qué hay”
(para eso está **Preview**, que es gratis).

---

## Paso 7 — Lección B: Poda de particiones (partition pruning)

La poda **solo funciona** si comparas la columna de partición contra un **valor
constante**, sin envolverla en una función.

```sql
-- ❌ MAL: función sobre la columna de partición → NO poda → escaneo completo
SELECT COUNT(*) FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE EXTRACT(YEAR  FROM pickup_datetime) = 2018
  AND EXTRACT(MONTH FROM pickup_datetime) = 6;

-- ✅ BIEN: comparación con literales → poda → ~1/12 de los bytes
SELECT COUNT(*) FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01');
```

Compara **Bytes billed** de ambas: la primera escanea toda la tabla.

#### El guardrail `require_partition_filter`

```sql
-- ❌ Falla de inmediato en trips_optimized: no hay filtro de partición
SELECT COUNT(*) FROM `TU_PROYECTO.taxi_lab.trips_optimized`;
```

Error esperado:
`Cannot query over table 'TU_PROYECTO.taxi_lab.trips_optimized' without a filter over
column(s) 'pickup_datetime' that can be used for partition elimination.`

Añade el filtro de fecha y funciona. Este `OPTIONS` evita escaneos accidentales de la
tabla entera.

<img width="1120" height="593" alt="Screenshot 2026-09-03 at 12 52 47 p m" src="https://github.com/user-attachments/assets/7ecfba84-92aa-476a-b930-cd7d72ef1b3d" />

---

## Paso 8 — Lección C: Clustering

El clustering ordena los datos **dentro de cada partición**. Solo acelera si la consulta
**filtra o agrupa por el prefijo** de columnas del `CLUSTER BY`
(`payment_type`, luego `pickup_location_id`).

```sql
-- C1: filtra por la 1ª columna de cluster → block pruning efectivo
SELECT COUNT(*) , SUM(tip_amount)
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND payment_type = '2';

-- C2: misma consulta contra trips_partitioned (sin cluster) → compara Bytes billed
SELECT COUNT(*), SUM(tip_amount)
FROM `TU_PROYECTO.taxi_lab.trips_partitioned`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND payment_type = '2';

-- C3: filtra SOLO por la 2ª columna de cluster, saltándose la 1ª → beneficio parcial/nulo
SELECT COUNT(*)
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
  AND pickup_datetime <  TIMESTAMP('2018-07-01')
  AND pickup_location_id = '132';
```

**Conclusiones:**
- C1 < C2 en bytes facturados → el clustering recortó bloques.
- C3 ≈ sin beneficio → saltarse la primera columna del cluster rompe el aprovechamiento.
- Ordena el `CLUSTER BY` de **más filtrada / más selectiva** a menos.
- En tablas clusterizadas, `LIMIT` sí puede reducir bytes escaneados (en tablas normales
  no).

---

## Paso 9 — Lección D: Filtrar y agregar ANTES del `JOIN`

Reduce el volumen **antes** de cruzar tablas. Y expresa primero el filtro **más
selectivo** (hábito de legibilidad y, a veces, de rendimiento).

```sql
-- ❌ Menos eficiente: cruza toda la tabla y filtra después
SELECT z.borough, COUNT(*) AS viajes
FROM `TU_PROYECTO.taxi_lab.trips_optimized` t
JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` z
  ON t.pickup_location_id = z.zone_id
WHERE t.pickup_datetime >= TIMESTAMP('2018-06-01')
  AND t.pickup_datetime <  TIMESTAMP('2018-07-01')
  AND t.payment_type = '2'
GROUP BY z.borough;

-- ✅ Mejor: filtra y agrega primero, luego une el resultado pequeño con el lookup
WITH viajes_junio AS (
  SELECT pickup_location_id, COUNT(*) AS viajes
  FROM `TU_PROYECTO.taxi_lab.trips_optimized`
  WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
    AND pickup_datetime <  TIMESTAMP('2018-07-01')
    AND payment_type = '2'
  GROUP BY pickup_location_id
)
SELECT z.borough, SUM(v.viajes) AS viajes
FROM viajes_junio v
JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` z
  ON v.pickup_location_id = z.zone_id
GROUP BY z.borough;
```

Compara **slot time** y **Bytes shuffled** (en *Execution details*): la segunda mueve
muchos menos datos entre etapas.

---

## Paso 10 — Lección E: Orden de `JOIN` (tabla más grande a la izquierda)

Guía documentada de BigQuery: **coloca primero la tabla con más filas**, y luego las de
menor tamaño. El optimizador con frecuencia reordena por su cuenta, así que aquí lo
importante es **saber leer el plan** para confirmar qué está pasando.

```sql
-- Forma recomendada: grande a la izquierda, lookup a la derecha
SELECT z.zone_name, COUNT(*) AS viajes
FROM `TU_PROYECTO.taxi_lab.trips_optimized` t          -- ~9M filas (1 mes)
JOIN `bigquery-public-data.new_york_taxi_trips.taxi_zone_geom` z  -- ~263 filas
  ON t.pickup_location_id = z.zone_id
WHERE t.pickup_datetime >= TIMESTAMP('2018-06-01')
  AND t.pickup_datetime <  TIMESTAMP('2018-07-01')
GROUP BY z.zone_name
ORDER BY viajes DESC
LIMIT 10;
```

**Cómo verificarlo:** ejecuta y abre **Execution graph / Execution details**. En la etapa
de `JOIN` mira:
- Qué entrada se marca como **broadcast** (debería ser la tabla chica, `taxi_zone_geom`).
- **Records read** por etapa: la tabla grande alimenta el lado *probe*; la chica, el lado
  *build*.

Invierte el orden de las tablas en el `FROM/JOIN` y vuelve a ejecutar: comprueba si el
plan cambia o si el optimizador produce el mismo gráfico. Anota tu observación.

---

## Paso 11 — Lección F: Otros anti-patrones frecuentes

| Anti-patrón | Alternativa | Por qué |
|---|---|---|
| `ORDER BY` sin `LIMIT` en resultados grandes | `ORDER BY ... LIMIT n`, y solo en la consulta más externa | ordenar todo el resultado consume slots y puede fallar por *resources exceeded* |
| Self-join para comparar con la fila anterior | `LAG()` / `LEAD()` / `SUM() OVER (...)` | la función de ventana lee la tabla una vez |
| `COUNT(DISTINCT campo_alta_cardinalidad)` | `APPROX_COUNT_DISTINCT(campo)` | exacto es caro en memoria; el aproximado tiene ~1% de error |
| `SELECT *` en subconsultas / CTEs | proyecta solo lo necesario en cada nivel | las columnas de más se arrastran por todo el plan |
| Muchos `UPDATE`/`INSERT` fila a fila | cargar/`MERGE` en lote | cada DML es un job; hay cuotas y sobrecarga |
| Tablas “sharded” por fecha (`t_20180601`, `t_20180602`, …) | una tabla **particionada** por fecha | menos metadatos, poda nativa, sin `UNION ALL` |

Ejemplos:

```sql
-- Exacto vs aproximado (compara tiempo y resultado)
SELECT COUNT(DISTINCT pickup_location_id)        AS zonas_exacto
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2018-04-01');

SELECT APPROX_COUNT_DISTINCT(pickup_location_id) AS zonas_aprox
FROM `TU_PROYECTO.taxi_lab.trips_optimized`
WHERE pickup_datetime >= TIMESTAMP('2018-01-01')
  AND pickup_datetime <  TIMESTAMP('2018-04-01');
```

```sql
-- Ranking de días con más viajes usando función de ventana (sin self-join)
SELECT
  dia,
  viajes,
  viajes - LAG(viajes) OVER (ORDER BY dia) AS delta_vs_dia_anterior
FROM (
  SELECT DATE(pickup_datetime) AS dia, COUNT(*) AS viajes
  FROM `TU_PROYECTO.taxi_lab.trips_optimized`
  WHERE pickup_datetime >= TIMESTAMP('2018-06-01')
    AND pickup_datetime <  TIMESTAMP('2018-07-01')
  GROUP BY dia
)
ORDER BY dia;
```

---

## Paso 12 — Cuadro resumen final

Vuelve a correr la consulta de `INFORMATION_SCHEMA.JOBS_BY_PROJECT` del Paso 5 y arma tu
resumen definitivo: consulta, diseño de tabla, GB facturados, slot ms y USD aproximado.
La conclusión que deberías poder defender con números:

1. **Proyección** (no `SELECT *`) → menos bytes en cualquier tabla.
2. **Partición + filtro con literal** → reduce bytes proporcional al rango consultado.
3. **Clustering** → recorte extra cuando filtras/agrupas por el prefijo del cluster.
4. **Filtrar/agregar antes del `JOIN`** → menos datos en shuffle, menos slot time.
5. **`require_partition_filter`** → evita el escaneo accidental de la tabla completa.

---

## Paso 13 — Limpieza

```sql
DROP SCHEMA IF EXISTS `TU_PROYECTO.taxi_lab` CASCADE;
```

o en CLI:

```bash
bq rm -r -f -d TU_PROYECTO:taxi_lab
```

Revisa en **Billing → Reports** filtrando por servicio *BigQuery* que el consumo quede
dentro de la capa gratuita.

---

## Retos

1. **Partición por rango de enteros:** crea `trips_by_zone` con
   `PARTITION BY RANGE_BUCKET(CAST(pickup_location_id AS INT64), GENERATE_ARRAY(1, 265, 5))`
   y compara Q3 contra la versión particionada por fecha.
2. **Vista materializada:** crea una `MATERIALIZED VIEW` que preagregue viajes por zona y
   día sobre `trips_optimized`; mide Q3 usando la vista.
3. **Escalado:** repite el lab añadiendo `tlc_yellow_trips_2019` a las tablas (usa
   `INSERT` o recrea con `UNION ALL`). ¿Se mantiene la relación de bytes entre diseños al
   duplicar el volumen?
4. **`_PARTITIONTIME`:** crea una tabla con partición por tiempo de ingesta y carga los
   datos en dos lotes; consulta con `WHERE _PARTITIONTIME = '2018-01-01'`.

---

## Referencias

- [BigQuery — Optimize query computation (best practices)](https://docs.cloud.google.com/bigquery/docs/best-practices-performance-compute)
- [BigQuery — Introduction to partitioned tables](https://docs.cloud.google.com/bigquery/docs/partitioned-tables)
- [BigQuery — Introduction to clustered tables](https://docs.cloud.google.com/bigquery/docs/clustered-tables)
- [BigQuery — Control costs / estimate query costs](https://docs.cloud.google.com/bigquery/docs/best-practices-costs)
- [BigQuery — INFORMATION_SCHEMA.JOBS](https://docs.cloud.google.com/bigquery/docs/information-schema-jobs)
- [Dataset público: `bigquery-public-data.new_york_taxi_trips`](https://console.cloud.google.com/marketplace/product/city-of-new-york/nyc-tlc-trips)
