# ELT Programático con Apache Beam/Dataflow sobre un Lakehouse Iceberg

> **Audiencia Objetivo:** Data Engineers, Analytics Engineers y Data Architects que ya conocen el patrón CDC + Medallion (Módulo 01) y los formatos de tabla abiertos (Módulo 03), y quieren entender **cuándo y cómo reemplazar el motor SQL declarativo (Dataform/dbt) por un motor de programación de datos (Apache Beam/Dataflow)** para las transformaciones Silver y Gold.
>
> **Alcance:** Este módulo **no repite** la teoría de CDC/Medallion (ver [Módulo 01](../01-patrones-y-modelado/teoria.md)) ni la de formatos de tabla abiertos (ver [Módulo 03](../03-formatos-y-lakehouse/teoria.md)). Se enfoca en lo nuevo: el **modelo de programación de Apache Beam**, cómo **Datastream entrega el CDC como archivos Avro en Cloud Storage** (en vez de tablas BigQuery), y cómo **Dataflow escribe en tablas Iceberg gestionadas por BigQuery** sin usar el conector nativo `IcebergIO` (que hoy es Java-only y exige BigLake Metastore).
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 05 — ELT Batch con Dataflow: CDC (Datastream) + Bronze/Silver Iceberg + Datamart Gold](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo: SQL Declarativo vs. Programación de Datos](#1-resumen-ejecutivo-sql-declarativo-vs-programación-de-datos)
2. [El Modelo de Programación de Apache Beam](#2-el-modelo-de-programación-de-apache-beam)
3. [Dataflow como Servicio Gestionado](#3-dataflow-como-servicio-gestionado)
4. [Datastream hacia Cloud Storage: CDC como Archivos, no como Tablas](#4-datastream-hacia-cloud-storage-cdc-como-archivos-no-como-tablas)
5. [Escribir Iceberg desde Dataflow: la Decisión de Diseño de este Módulo](#5-escribir-iceberg-desde-dataflow-la-decisión-de-diseño-de-este-módulo)
6. [Costo Real de Escribir en Tablas Iceberg Gestionadas](#6-costo-real-de-escribir-en-tablas-iceberg-gestionadas)
7. [Arquitectura de Referencia del Módulo](#7-arquitectura-de-referencia-del-módulo)
8. [Referencias Técnicas](#8-referencias-técnicas)

---

## 1. Resumen Ejecutivo: SQL Declarativo vs. Programación de Datos

En el [Módulo 01](../01-patrones-y-modelado/teoria.md) las capas Silver y Gold se construyeron con **Dataform**: SQL declarativo que el propio motor de BigQuery ejecuta por *pushdown*. Es el camino correcto cuando la transformación se puede expresar como `SELECT`s encadenados.

Hay un segundo escenario, igual de común en la práctica: la transformación necesita **lógica imperativa** antes de que el dato exista como fila de una tabla —parsear un formato binario propietario, unificar esquemas heterogéneos (un CDC en Avro y un CSV plano en el mismo paso), aplicar reglas de limpieza con ramas condicionales complejas, o correr el mismo código en modo batch hoy y en modo streaming mañana sin reescribirlo—. Para eso existe **Apache Beam**, y **Dataflow** como el servicio administrado de Google Cloud que lo ejecuta.

```
DOS FORMAS DE EXPRESAR LA MISMA TRANSFORMACIÓN "T" DEL ELT

A. SQL DECLARATIVO (Módulo 01 — Dataform/dbt)
   El motor del almacén (BigQuery) interpreta el SQL y decide el plan de ejecución.
   [Tabla Bronze] --SELECT...FROM...WHERE--> [Tabla Silver] --SELECT...JOIN--> [Tabla Gold]

B. PROGRAMACIÓN DE DATOS (Módulo 05 — Apache Beam/Dataflow)
   El desarrollador construye explícitamente el grafo de pasos (DAG) en código.
   [PCollection Bronze] --ParDo/Map/GroupByKey--> [PCollection Silver] --ParDo--> [PCollection Gold]
   Un clúster elástico de workers (Dataflow) ejecuta ese grafo.
```

Ninguno sustituye al otro: en este módulo usamos Beam/Dataflow **precisamente** para el tramo donde SQL no basta —unificar un flujo CDC en Avro con archivos CSV planos antes de que lleguen a BigQuery— y dejamos que BigQuery (vía tablas Iceberg gestionadas, ver Módulo 03) siga siendo el lugar donde el dato *vive* y se consulta.

---

## 2. El Modelo de Programación de Apache Beam

Beam es un **SDK unificado de procesamiento de datos** (Java, Python, Go) que separa la lógica de la transformación del motor que la ejecuta (*Runner*).

```
ANATOMÍA DE UN PIPELINE DE BEAM

Pipeline
 └── PCollection (colección distribuida e inmutable de elementos)
      └── PTransform (una operación: Map, ParDo, GroupByKey, Combine, Filter...)
           └── produce una nueva PCollection

[Leer Avro CDC] --PCollection--> [ParDo: parsear evento] --PCollection--> [GroupByKey: dedup]
      --PCollection--> [Map: tipar campos] --PCollection--> [WriteToBigQuery]
```

### 2.1. Conceptos Clave

1. **`PCollection`:** No es una lista en memoria; es una colección **distribuida** que el Runner particiona entre workers. No tiene orden garantizado salvo que se imponga explícitamente (como hacemos en el Paso 3 del lab para quedarnos con el evento CDC más reciente).
2. **`PTransform`:** La unidad de transformación. Los más usados en este lab:
   - `beam.Map(fn)`: una función que transforma **un** elemento en **un** elemento nuevo (equivalente a `SELECT expr`).
   - `beam.Filter(fn)`: descarta elementos que no cumplen un predicado (equivalente a `WHERE`).
   - `beam.GroupByKey()`: agrupa por clave, análogo a `GROUP BY` pero entregando la lista completa de valores para procesarla en código (lo usamos para deduplicar el CDC de `orders`, donde en SQL habrías usado `ROW_NUMBER() OVER (PARTITION BY... ORDER BY...)` como en el Módulo 01).
   - `beam.CoGroupByKey()`: hace un *join* de dos o más `PCollection` por clave común (el equivalente programático de un `JOIN`).
3. **`ParDo` / `DoFn`:** La forma general de un `PTransform`: una clase (`DoFn`) con un método `process()` que recibe un elemento y puede emitir cero, uno o varios elementos de salida. `Map` y `Filter` son azúcar sintáctico sobre `ParDo`.
4. **Bounded vs. Unbounded:** Una `PCollection` **bounded** (acotada) proviene de una fuente finita —un archivo, una tabla— y el pipeline termina cuando la procesa toda: es el caso de **todo este laboratorio (batch)**. Una `PCollection` **unbounded** proviene de una fuente infinita —Pub/Sub, Kafka— y el pipeline corre indefinidamente. **El mismo código Beam puede, en muchos casos, correr en ambos modos**; eso es lo que hace atractivo a Beam frente a escribir un script batch y, por separado, un consumidor streaming.
5. **Runners:** El mismo pipeline se ejecuta con `DirectRunner` (localmente, para probar la lógica con una muestra pequeña de datos) o con `DataflowRunner` (delegando la ejecución al servicio administrado de Google Cloud). En el lab usamos `DirectRunner` implícitamente al validar localmente y `DataflowRunner` para las corridas reales.

---

## 3. Dataflow como Servicio Gestionado

Dataflow toma el grafo de un pipeline Beam y lo ejecuta sobre una flota de máquinas virtuales (*workers*) que **auto-escala** según el volumen de datos y la presión de cómputo, sin que el desarrollador aprovisione ni gestione clústeres.

```
CICLO DE VIDA DE UN JOB DE DATAFLOW

[python bronze_pipeline.py --runner=DataflowRunner ...]
              │
              ▼
     Dataflow construye el GRAFO DE EJECUCIÓN (Job Graph)
     y lo optimiza (fusiona pasos: "fusion" de ParDo consecutivos)
              │
              ▼
     Aprovisiona WORKERS (Compute Engine) elásticamente
     ──► Shuffle Service gestionado (para GroupByKey/CoGroupByKey)
              │
              ▼
     Ejecuta, expone métricas en Cloud Monitoring / Job UI
              │
              ▼
     Libera los workers al terminar (pipeline batch = job efímero)
```

**Diferencia clave frente a Dataform (Módulo 01):** Dataform no tiene clúster propio — empuja SQL a los *slots* de BigQuery. Dataflow **sí** aprovisiona su propio clúster efímero de VMs para ejecutar código Python/Java arbitrario. Esto da más flexibilidad (puedes parsear cualquier formato, llamar librerías externas, mantener estado complejo) a cambio de una unidad de cómputo adicional que hay que pagar y liberar (ver §6 y la sección de limpieza del lab).

---

## 4. Datastream hacia Cloud Storage: CDC como Archivos, no como Tablas

En el Módulo 01, Datastream escribía directo a BigQuery: el propio servicio creaba y mantenía la tabla `_raw` con los metadatos de CDC como columnas. Aquí usamos **Datastream con destino Cloud Storage**, el patrón típico cuando la capa Bronze vive en el *lake* (GCS) y no directamente en el warehouse.

### 4.1. Estructura de un Evento CDC en Avro

Cada archivo Avro que Datastream escribe en GCS contiene eventos con esta forma (campos en el orden en que aparecen):

```
EVENTO DATASTREAM (AVRO) — CDC DE POSTGRESQL HACIA GCS

{
  "stream_name": "orders-to-gcs-stream",
  "read_method": "postgresql-cdc" | "postgresql-backfill",
  "object": "core_orders",
  "uuid": "…",
  "read_timestamp": "2026-…",
  "source_timestamp": "2026-…",          ← usado para deduplicar (Paso 3 del lab)
  "sort_keys": […],
  "source_metadata": {                    ← específico de PostgreSQL
      "schema": "core",
      "table": "orders",
      "change_type": "INSERT" | "UPDATE" | "DELETE",   ← para PostgreSQL, estos son los únicos 3 valores documentados
      "is_deleted": false,
      "primary_keys": ["order_id"],
      "lsn": "…",
      "tx_id": "…"
  },
  "payload": {                            ← las columnas reales de la fila
      "order_id": "ORD-0004",
      "customer_id": "CUST-03",
      "product_id": "PROD-01",
      "quantity": 1,
      "unit_price": "45.00",
      "status": "SHIPPED",
      "order_ts": "2026-09-…"
  }
}
```

Nótese la diferencia con la tabla `_raw` que generaba el Módulo 01 en BigQuery: allí Datastream **aplanaba** `source_metadata` en columnas con prefijo `_metadata_*`. Aquí, al escribir a GCS, la estructura se mantiene **anidada**: el pipeline de Beam del Paso 2 del lab lee `evento["payload"]["order_id"]` y `evento["source_metadata"]["change_type"]` explícitamente.

> [!NOTE]
> La documentación oficial de Datastream **no especifica** qué valor toma `change_type` en los eventos del backfill inicial (solo documenta `INSERT`/`UPDATE`/`DELETE` para cambios de CDC real). Por eso `parse_order_event` en el lab usa `meta.get("change_type", "BACKFILL")`: si el campo viene ausente o nulo, **nuestro propio código** le pone la etiqueta `"BACKFILL"` — no es un valor que Datastream envíe. Para distinguir de forma confiable un evento de backfill de uno de CDC en vivo, el campo correcto a mirar es el top-level `read_method` (`postgresql-backfill` vs. `postgresql-cdc`), no `change_type`.

### 4.2. Por qué Bronze necesita deduplicación

Con `--backfill-all`, Datastream primero **vuelca todo el estado actual** de la tabla (`read_method = postgresql-backfill`) y **luego** sigue con el CDC continuo (`read_method = postgresql-cdc`). Si una fila cambia después del backfill, GCS acumula **dos eventos para el mismo `order_id`**: el snapshot inicial y el cambio. Esto es exactamente el mismo problema que resolvía el `ROW_NUMBER() OVER (PARTITION BY... ORDER BY _metadata_timestamp DESC)` del Módulo 01 — aquí se resuelve con `GroupByKey` + selección del evento con mayor `source_timestamp` (ver §2.1, punto 2, y Paso 3 del lab).

---

## 5. Escribir Iceberg desde Dataflow: la Decisión de Diseño de este Módulo

Apache Beam tiene, desde 2024, un conector *Managed I/O* para Iceberg (`beam.managed.Write`/`Read` tipo `ICEBERG`) que escribe directamente al formato de tabla usando un catálogo REST (por ejemplo, el **catálogo REST de BigLake/BigQuery Metastore**). Es la vía "nativa" y la dirección a la que se mueve el ecosistema.

**Por qué este lab no lo usa en un taller de 2 horas:**

- Hoy es una funcionalidad **expuesta solo en el SDK de Java** de Beam (el conector Python la invoca vía *cross-language transforms*, con la complejidad operativa que eso implica).
- Requiere tener ya desplegado un **catálogo REST de BigLake/BigQuery Metastore** y gestionar la autenticación (tokens OAuth con refresco horario) contra él.
- El *setup* adicional (permisos del metastore, bucket single-region dedicado, gestión de tokens) excede por sí solo el tiempo disponible en una sesión de 2 horas.

**La alternativa que sí usamos, y que es igual de "Iceberg real":** en el [Módulo 03](../03-formatos-y-lakehouse/teoria.md) ya viste que BigQuery puede **crear y gestionar tablas Iceberg** (`CREATE TABLE ... OPTIONS(table_format='ICEBERG', storage_uri='gs://...')`): los datos quedan como Parquet + metadata Iceberg estándar en tu propio bucket, pero se escriben y consultan con las herramientas normales de BigQuery. Dataflow, entonces, **no necesita hablar Iceberg**: solo necesita hablar **BigQuery**, usando el conector `WriteToBigQuery`/`ReadFromBigQuery` del SDK de Python (estable, bien documentado, el mismo que usarías contra cualquier tabla nativa).

```
DOS RUTAS PARA QUE DATAFLOW ESCRIBA ICEBERG (2026)

RUTA A — Beam IcebergIO nativo (Java, catálogo REST propio)
[Dataflow Java] --beam.managed.Write(ICEBERG)--> [Catálogo REST BigLake] --> [Parquet+metadata en GCS]
   Requiere: SDK Java, BigLake Metastore desplegado, refresco de tokens OAuth.
   Es la vía de producción a mediano plazo; NO se usa en este lab por su setup.

RUTA B — WriteToBigQuery contra una tabla Iceberg gestionada (la que usa este lab)
[Dataflow Python] --WriteToBigQuery--> [Tabla BigQuery con table_format='ICEBERG']
                                              │
                                              ▼ (mismo resultado físico)
                                     [Parquet+metadata Iceberg en GCS]
   Requiere: crear la tabla una vez con SQL (igual que en el Módulo 03) y luego
   escribir con el conector estándar de BigQuery — cero código Iceberg-específico.
```

**Limitación práctica de la Ruta B que verás en el lab:** `WriteToBigQuery` con `create_disposition=CREATE_IF_NEEDED` solo sabe crear **tablas BigQuery nativas simples** a partir de un esquema; no conoce la cláusula `WITH CONNECTION ... OPTIONS(table_format='ICEBERG', storage_uri=...)`. Por eso, para Bronze y Silver (que sí son tablas Iceberg), el lab **pre-crea las tablas con SQL** antes de correr Dataflow y usa `create_disposition=CREATE_NEVER`. Para Gold (tabla BigQuery nativa, sin necesidad de ser Iceberg), Dataflow sí puede crear la tabla sobre la marcha.

---

## 6. Costo Real de Escribir en Tablas Iceberg Gestionadas

Esto es importante para el FinOps del lab y **no aplica** a las tablas BigQuery normales que usaste en Módulos 01–03:

> [!WARNING]
> Los **jobs de carga (`LOAD JOB`)** hacia una tabla Iceberg gestionada en BigQuery — que es exactamente el método que usa `WriteToBigQuery` en modo batch (`FILE_LOADS`) — se facturan con **slots Enterprise *pay-as-you-go***. Las tablas BigQuery normales **no cobran** por operaciones de carga. El mantenimiento en segundo plano de una tabla Iceberg (compactación, *garbage collection*) también consume **Data Compute Units (DCU)**. Para el volumen de este taller (unos pocos miles de filas) el costo es de céntimos de dólar, pero es una diferencia real frente al "$0.00" de BigQuery on-demand que viste en los Módulos 02 y 03 — se refleja en la tabla de costos del [lab](lab.md).

---

## 7. Arquitectura de Referencia del Módulo

```
┌─────────────────────────┐        ┌──────────────────────────────┐
│ Cloud SQL (PostgreSQL)  │        │  GCS landing/ (CSV batch)     │
│ tabla core.orders       │        │  customers.csv, products.csv │
└────────────┬─────────────┘        └───────────────┬────────────────┘
             │ Datastream (CDC, --backfill-all)      │
             ▼ salida: Avro en GCS                    │
┌─────────────────────────────────────────────────────┴────────────┐
│                    DATAFLOW — JOB BRONZE (Beam Python)            │
│  ReadFromAvro(cdc) + ReadFromText(csv) → parseo → WriteToBigQuery │
└───────────────────────────────┬────────────────────────────────────┘
                                 ▼
        BIGQUERY — TABLAS ICEBERG GESTIONADAS (dataset bronze)
        storage físico: Parquet + metadata Iceberg en tu bucket GCS
                                 │ Dataflow — JOB SILVER (dedup, tipado, calidad)
                                 ▼
        BIGQUERY — TABLAS ICEBERG GESTIONADAS (dataset silver)
                                 │ Dataflow — JOB GOLD (cálculo de medidas)
                                 ▼
        BIGQUERY — TABLAS NATIVAS: fact_sales, dim_customer, dim_product
                                 │
                                 ▼
                         Looker Studio / BI
```

---

## 8. Referencias Técnicas

1. **Apache Beam Programming Guide.** [beam.apache.org/documentation/programming-guide](https://beam.apache.org/documentation/programming-guide/).
2. **Google Cloud Dataflow Documentation — Beam Runner.** [docs.cloud.google.com/dataflow](https://docs.cloud.google.com/dataflow/docs/guides/deploying-a-pipeline).
3. **Datastream — Events and streams (estructura de eventos CDC).** [docs.cloud.google.com/datastream/docs/events-and-streams](https://docs.cloud.google.com/datastream/docs/events-and-streams).
4. **Datastream — Unified types mapping.** [docs.cloud.google.com/datastream/docs/unified-types](https://docs.cloud.google.com/datastream/docs/unified-types).
5. **BigQuery — Apache Iceberg managed tables.** [docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery](https://docs.cloud.google.com/bigquery/docs/biglake-iceberg-tables-in-bigquery).
6. **Dataflow — Streaming write to Apache Iceberg with the BigLake REST catalog (Ruta A, Java, no usada en este lab).** [docs.cloud.google.com/dataflow/docs/guides/streaming-write-to-iceberg-biglake](https://docs.cloud.google.com/dataflow/docs/guides/streaming-write-to-iceberg-biglake).
7. **Apache Beam — BigQuery I/O connector (Python).** [beam.apache.org/documentation/io/built-in/google-bigquery](https://beam.apache.org/documentation/io/built-in/google-bigquery/).
8. **Kleppmann, M. (2017).** *Designing Data-Intensive Applications.* O'Reilly Media. (Modelo de procesamiento batch/stream unificado, Cap. 10-11).
