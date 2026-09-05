# Formatos de Archivo y Formatos de Tabla en Big Data & Lakehouse

> **Audiencia:** Principal Data Architects, Analytics Engineers, Tech Leads, Cloud Infrastructure Engineers y CDOs.
>
> **Alcance:** De la serialización binaria en disco (Avro, Parquet, ORC) a la gobernanza transaccional ACID y metadatos desacoplados (Delta Lake, Apache Iceberg, Apache Hudi).
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 03 — Formatos de Archivo y Formatos de Tabla Abiertos](lab.md)

## Tabla de Contenidos

1. [Resumen Ejecutivo y Marco Conceptual](#1-resumen-ejecutivo-y-marco-conceptual)
2. [Formatos de Archivo Físicos (File Formats)](#2-formatos-de-archivo-físicos-file-formats)
   - [2.1. Apache Avro: Serialización Densa Orientada a Filas](#21-apache-avro-serialización-densa-orientada-a-filas)
   - [2.2. Apache Parquet: Almacenamiento Columnar Analítico](#22-apache-parquet-almacenamiento-columnar-analítico)
   - [2.3. Apache ORC: Optimized Row Columnar](#23-apache-orc-optimized-row-columnar)
   - [2.4. Matriz Comparativa de Formatos Físicos](#24-matriz-comparativa-de-formatos-físicos)
3. [La Crisis de los Archivos Planos: El Nacimiento de los Formatos de Tabla](#3-la-crisis-de-los-archivos-planos-el-nacimiento-de-los-formatos-de-tabla)
4. [Formatos de Tabla Abiertos (Open Table Formats)](#4-formatos-de-tabla-abiertos-open-table-formats)
   - [4.1. Delta Lake: El Log Transaccional Serializado](#41-delta-lake-el-log-transaccional-serializado)
   - [4.2. Apache Iceberg: El Árbol Jerárquico de Snapshots](#42-apache-iceberg-el-árbol-jerárquico-de-snapshots)
   - [4.3. Apache Hudi: Especialista en Streaming y Mutaciones CDC](#43-apache-hudi-especialista-en-streaming-y-mutaciones-cdc)
5. [Matriz Maestra de Decisión Técnica](#5-matriz-maestra-de-decisión-técnica)
6. [Patrón de Arquitectura de Referencia en Producción](#6-patrón-de-arquitectura-de-referencia-en-producción)
7. [Referencias Técnicas y Bibliografía Fundacional](#7-referencias-técnicas-y-bibliografía-fundacional)

---

## 1. Resumen Ejecutivo y Marco Conceptual

En plataformas analíticas modernas (Data Lakehouses, arquitecturas Medallion y Data Mesh), la persistencia ya no es un detalle pasivo de infraestructura. La elección coordinada de **formatos de archivo físicos** y **formatos de tabla lógicos** determina cuatro variables de impacto financiero y operativo:

1. **Eficiencia Financiera (Cloud FinOps):** Costos de escaneo en motores serverless (BigQuery, AWS Athena, Snowflake) y almacenamiento en Cloud Object Storage (GCS, S3, ADLS).
2. **Latencia Operacional (SLAs):** Velocidad de ingestión continua en streaming frente a tiempos de respuesta sub-segundo en tableros de visualización de BI.
3. **Integridad Transaccional:** Prevención de escrituras parciales, lecturas sucias y aislamiento ACID sin bloqueos distribuidos costosos.
4. **Independencia Tecnológica:** Neutralidad ante proveedores (*zero vendor lock-in*) permitiendo que múltiples motores consulten el mismo repositorio físico.

```
PILA ARQUITECTÓNICA DE ALMACENAMIENTO MODERNO
┌────────────────────────────────────────────────────────────────────────┐
│ 1. CAPA DE CONSUMO: BI (Looker, Tableau), ML (Vertex AI), SQL Ad-Hoc   │
├────────────────────────────────────────────────────────────────────────┤
│ 2. MOTOR MPP / QUERY ENGINE: BigQuery, Snowflake, Databricks, Trino   │
├────────────────────────────────────────────────────────────────────────┤
│ 3. FORMATO DE TABLA (Metadatos ACID): Apache Iceberg / Delta / Hudi    │
├────────────────────────────────────────────────────────────────────────┤
│ 4. FORMATO DE ARCHIVO (Bytes en Disco): Apache Parquet / ORC / Avro    │
├────────────────────────────────────────────────────────────────────────┤
│ 5. STORAGE CLOUD (Objetos Desacoplados): Google Cloud Storage / S3 / ADLS│
└────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Formatos de Archivo Físicos (File Formats)

Los formatos de archivo determinan la codificación binaria de los datos en bloques contiguos. Se dividen fundamentalmente en **orientados a filas** y **orientados a columnas**.

```
CONTRASTE FÍSICO EN DISCO

ORIENTACIÓN A FILAS (AVRO)
[ID:1, Nombre:Ana, Score:750] [ID:2, Nombre:Carlos, Score:680] [ID:3, Nombre:Elena, Score:810]
  ───► Óptimo para escrituras atómicas de fila completa. Pésimo para agregar solo "Score".

ORIENTACIÓN A COLUMNAS (PARQUET / ORC)
Columna ID:     [1, 2, 3]
Columna Nombre: [Ana, Carlos, Elena]
Columna Score:  [750, 680, 810]
  ───► El motor solo lee el bloque "Score". Cero I/O desperdiciado en ID y Nombre.
```

### 2.1. Apache Avro: Serialización Densa Orientada a Filas

Apache Avro es un framework de serialización y RPC diseñado para superar las ineficiencias de JSON y XML en sistemas distribuidos.

```
ANATOMÍA FÍSICA DE UN ARCHIVO AVRO (.avro)

┌────────────────────────────────────────────────────────────────────────┐
│ 1. CABECERA (HEADER)                                                   │
│    ├── 4 Bytes Magic: 'A', 'V', 'R', 0x01                              │
│    ├── Metadata Map:                                                   │
│    │    ├── "avro.schema": '{"type":"record","name":"User",...}'       │
│    │    └── "avro.codec": "snappy" | "deflate" | "null"                │
│    └── 16-Byte Sync Marker (Marcador aleatorio de sincronización)      │
├────────────────────────────────────────────────────────────────────────┤
│ 2. DATA BLOCK 1                                                        │
│    ├── Conteo de Objetos/Filas (Varint)                                │
│    ├── Tamaño del Bloque en Bytes (Varint)                             │
│    ├── Payloads Binarios Consecutivos (Valores puros sin metadatos)    │
│    └── 16-Byte Sync Marker (Idéntico al de la cabecera)                │
├────────────────────────────────────────────────────────────────────────┤
│ 3. DATA BLOCK 2 ... N                                                  │
│    └── [Conteo] + [Tamaño] + [Bytes de Datos] + [16-Byte Sync Marker]  │
└────────────────────────────────────────────────────────────────────────┘
```

#### Características Clave
* **Cero sobrecarga por registro:** Los nombres de campos (`"cliente_id"`) se declaran una sola vez en la cabecera; cada fila contiene únicamente los bytes de sus valores en estricto orden posicional.
* **Codificación Zigzag Varint:** Los números enteros pequeños ocupan la mínima cantidad de bytes posible (un entero `1` ocupa un solo byte en lugar de 4 u 8 bytes).
* **Sync Marker (16 Bytes):** Permite a motores distribuidos (Spark, MapReduce, Flink) dividir un archivo de 50 GB en fragmentos independientes sin necesidad de leer desde el byte cero. Cada worker busca el marcador de sincronización para identificar el inicio exacto de un bloque estructurado válido.
* **Patrón de Streaming con Schema Registry:** En streaming masivo (Kafka, Cloud Pub/Sub), la cabecera JSON no se envía en cada mensaje. Se utiliza un registro central:
  * El mensaje transporta una cabecera de **5 bytes**: `1 Magic Byte (0x00)` + `4 Bytes con el Schema ID`.
  * Los consumidores resuelven el esquema desde memoria caché.

### 2.2. Apache Parquet: Almacenamiento Columnar Analítico

Inspirado en el paper seminal de Google sobre **Dremel (2010)**, Parquet optimiza el escaneo analítico OLAP donde las consultas filtran miles de millones de filas pero solo proyectan un subconjunto reducido de columnas.

```
ANATOMÍA FÍSICA DE UN ARCHIVO PARQUET (.parquet)

┌────────────────────────────────────────────────────────────────────────┐
│ 4 Bytes Magic: 'P', 'A', 'R', '1'                                      │
├────────────────────────────────────────────────────────────────────────┤
│ ROW GROUP 1 (Segmento Horizontal: ~128 MB a 1 GB de filas)             │
│  ├── Column Chunk 1 (ej. "cliente_id")                                 │
│  │    ├── Page 1 (Data Page: Diccionario / RLE / Bit-Packed)           │
│  │    └── Page 2 (Data Page comprimida en Snappy o ZSTD)               │
│  ├── Column Chunk 2 (ej. "monto_transaccion")                          │
│  │    └── Pages 1..N                                                   │
│  └── Column Chunk N (ej. "fecha_hora")                                 │
├────────────────────────────────────────────────────────────────────────┤
│ ROW GROUP 2 ... N                                                      │
├────────────────────────────────────────────────────────────────────────┤
│ PIE DE PÁGINA (FILE FOOTER - METADATOS)                                │
│  ├── Schema Completo de la Tabla                                       │
│  ├── Estadísticas por Columna en cada Row Group:                       │
│  │    ├── min_value / max_value                                        │
│  │    ├── null_count                                                   │
│  │    └── uncompressed_size / compressed_size                          │
│  ├── Offsets exactos de inicio de cada Column Chunk                    │
│  ├── 4 Bytes Footer Length                                             │
│  └── 4 Bytes Magic: 'P', 'A', 'R', '1'                                 │
└────────────────────────────────────────────────────────────────────────┘
```

#### Mecanismos de Aceleración de Consultas
1. **Single-Pass Streaming Write:** Parquet escribe primero los datos de las filas procesadas y guarda las estadísticas y offsets en el **Footer al final**. De este modo no requiere reabrir el archivo para registrar mínimos, máximos y tamaños reales.
2. **HTTP Range Requests:** Motores en la nube (BigQuery, Athena, Trino) consultan únicamente los últimos bytes del archivo para parsear el Footer y planificar la lectura de las columnas requeridas sin descargar el archivo completo.
3. **Projection Pushdown (Poda de Columnas):** Un `SELECT cliente_id, monto` lee y transfiere únicamente los bloques de bytes asociados a esas dos columnas, ignorando el resto del archivo.
4. **Predicate Pushdown (Poda de Filas por Estadísticas):** Ante un filtro `WHERE monto > 50000`, el motor evalúa el `max_value` del Row Group en los metadatos del Footer; si el valor máximo es `32000`, descarta el Row Group entero sin leer sus páginas.
5. **Algoritmo Dremel (Estructuras Anidadas):** Almacena estructuras complejas (JSONs, Arrays, Structs) contiguamente sin aplanarlas mediante dos enteros auxiliares por valor:
   * **Definition Level:** Indica cuántos niveles opcionales ancestrales están presentes (resuelve `NULL`s).
   * **Repetition Level:** Indica en qué nivel de profundidad del árbol de repetición cambia el valor (controla arrays anidados).

### 2.3. Apache ORC (Optimized Row Columnar)

Desarrollado para el ecosistema Apache Hive y optimizado para entornos Hadoop y motores derivados como Trino y Presto.

```
ANATOMÍA FÍSICA DE UN ARCHIVO ORC (.orc)

┌────────────────────────────────────────────────────────────────────────┐
│ STRIPE 1 (~250 MB)                                                     │
│  ├── Index Data: Estadísticas min/max y Bloom Filters cada 10,000 filas│
│  ├── Row Data: Column Chunks comprimidos (ZLIB, Snappy, ZSTD)          │
│  └── Stripe Footer: Codificación de columnas y offsets internos        │
├────────────────────────────────────────────────────────────────────────┤
│ STRIPE 2 ... N                                                         │
├────────────────────────────────────────────────────────────────────────┤
│ FILE FOOTER: Lista de Stripes, esquema y conteo total de filas         │
├────────────────────────────────────────────────────────────────────────┤
│ POSTSCRIPT: Longitud del footer, códec de compresión y Magic String    │
└────────────────────────────────────────────────────────────────────────┘
```

#### Ventajas y Diferencias frente a Parquet
* **Índices Ligeros a Nivel de Stripe:** ORC almacena estadísticas no solo por Stripe, sino por cada grupo de **10,000 filas (Index Stream)** dentro del archivo, permitiendo saltos más granulares dentro de un mismo bloque.
* **Bloom Filters Nativos:** Permite descartar bloques con búsquedas de alta cardinalidad (`WHERE id = 'XYZ'`) de forma probabilística.
* **Adopción:** Aunque es técnicamente equivalente o superior a Parquet en ciertas cargas de Hive/Trino, Parquet se convirtió en el estándar general de la industria debido a su adopción universal y neutralidad fuera de Java.

### 2.4. Matriz Comparativa de Formatos Físicos

| Dimensión Técnica | Apache Avro | Apache Parquet | Apache ORC |
| :--- | :--- | :--- | :--- |
| **Orientación Primaria** | Filas (Row-oriented) | Columnas (Column-oriented) | Columnas (Column-oriented) |
| **Rendimiento de Escritura** | Ultrarrápido, append continuo | Costoso (requiere búfer en RAM) | Costoso (requiere búfer en RAM) |
| **Rendimiento de Lectura Analítica**| Lento (debe leer todas las columnas)| Óptimo (Projection Pushdown) | Óptimo (Index Streams y Bloom Filters)|
| **Búsqueda Puntual por Clave** | Rápida (registro contiguo en disco) | Lenta (reconstruye fila de N columnas) | Moderada (Bloom filters aceleran lookup)|
| **Poda de Estadísticas** | No disponible | Predicate Pushdown por Row Group | Predicate Pushdown cada 10,000 filas |
| **Evolución de Esquemas** | Nativa con Schema Registry | Limitada a nivel de archivo | Limitada a nivel de archivo |
| **Uso Óptimo en Plataforma** | Ingesta, Streaming (Kafka), CDC | Capas Silver y Gold en Lakehouses | Clusters Hive, EMR y cargas Trino legacy |

---

## 3. La Crisis de los Archivos Planos: El Nacimiento de los Formatos de Tabla

Durante años, los Data Lakes consistieron en carpetas organizadas por particiones jerárquicas conteniendo archivos Parquet:
`/tabla/anio=2026/mes=09/archivo_01.parquet`.

Este modelo colapsó en arquitecturas analíticas a escala por cuatro patologías críticas:

| Patología | Impacto en Producción |
| :--- | :--- |
| **Carencia de Transacciones ACID** | Si un trabajo de escritura fallaba a mitad de camino, dejaba archivos huérfanos que corrompían reportes de negocio downstream. |
| **Listado Lento en Storage ($O(N)$)** | Para saber qué archivos existían, el motor ejecutaba llamadas `LIST` en buckets S3/GCS. En tablas con 100,000 archivos, la llamada tardaba minutos antes de procesar un solo registro. |
| **Mutaciones Ineficientes (Upserts/Deletes)** | Modificar un registro por normativas de privacidad (GDPR / Ley de olvido) obligaba a reescribir particiones completas manualmente. |
| **Falta de Aislamiento Concurrente** | Un lector y un escritor sobre la misma partición causaban lecturas inconsistentes (*dirty reads*) o colisiones no recuperables. |

Para resolver esto surgen los **Formatos de Tabla Abiertos (Open Table Formats)**: capas de metadatos transaccionales que dotan al Cloud Storage de las garantías de un motor relacional.

---

## 4. Formatos de Tabla Abiertos (Open Table Formats)

### 4.1. Delta Lake: El Log Transaccional Serializado

Creado por Databricks y parte de la Linux Foundation, Delta Lake fundamenta su arquitectura en un log de cambios secuencial.

```
ARQUITECTURA DE METADATOS DE DELTA LAKE

  mi_tabla_delta/
  ├── _delta_log/
  │    ├── 000000.json               <── Commit inicial: archivos agregados
  │    ├── 000001.json               <── Commit 1: inserciones/mutaciones
  │    ├── ...
  │    ├── 000010.checkpoint.parquet <── Checkpoint consolidado cada 10 commits
  │    └── 000011.json
  ├── part-00000-xyz.parquet         <── Archivos físicos Parquet de datos
  └── part-00001-abc.parquet
```

#### Funcionamiento Interno
* **Log Transaccional (`_delta_log`):** Cada operación confirmada genera un archivo JSON secuencial y atómico que especifica los archivos Parquet físicos agregados (`add`) o removidos lógicamente (`remove`).
* **Checkpoints Consolidados:** Cada 10 commits, Delta compila el log acumulado en un archivo Parquet optimizado. De este modo, un lector solo consulta el último checkpoint más los JSONs incrementales posteriores.
* **Control de Concurrencia Optimista (OCC):** Varios escritores procesan sus transacciones en paralelo. Si dos transacciones modifican los mismos archivos lógicos, el motor aborta la última y reintenta automáticamente.
* **Universal Format (UniForm):** Permite escribir tablas Delta Lake y generar simultáneamente metadatos de Iceberg y Hudi, permitiendo que motores que solo leen Iceberg consuman la tabla sin duplicar los Parquets base.
* **Liquid Clustering:** Sustituye el particionamiento estático tradicional reorganizando los datos internamente de forma dinámica para evitar desbalances (*skew*).

### 4.2. Apache Iceberg: El Árbol Jerárquico de Snapshots

Creado por Netflix y gestionado por la Apache Software Foundation, Iceberg fue diseñado específicamente para eliminar el cuello de botella del metastore de Hive y las llamadas de listado en Object Storage.

```
ÁRBOL JERÁRQUICO DE METADATOS DE APACHE ICEBERG

                       ┌────────────────────────┐
                       │    Iceberg Catalog     │ (Puntero a Metadata File actual)
                       └───────────┬────────────┘
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │   v3.metadata.json     │ (Esquema, particiones, lista snapshots)
                       └───────────┬────────────┘
                                   │
                                   ▼
                       ┌────────────────────────┐
                       │  snap-123.avro         │ (Manifest List: Lista de manifiestos)
                       └─────┬────────────┬─────┘
                             │            │
             ┌───────────────┘            └───────────────┐
             ▼                                            ▼
   ┌───────────────────┐                        ┌───────────────────┐
   │ manifest-A.avro   │                        │ manifest-B.avro   │
   │ (Rango: ID 1-1000)│                        │ (Rango: ID >1000) │
   └─────────┬─────────┘                        └─────────┬─────────┘
             │                                            │
      ┌──────┴──────┐                              ┌──────┴──────┐
      ▼             ▼                              ▼             ▼
[data-1.parquet] [data-2.parquet]            [data-3.parquet] [data-4.parquet]
```

#### Innovaciones Fundamentales
1. **Acceso $O(1)$ a los Datos:** El motor no lista carpetas en el bucket. Lee el archivo de manifiesto correspondiente, el cual contiene la ruta exacta de cada archivo Parquet junto con sus estadísticas completas.
2. **Hidden Partitioning (Particionamiento Oculto):** El usuario ya no consulta columnas de partición artificiales (como `WHERE anio=2026 AND mes=09`). Puede consultar directamente la marca de tiempo `WHERE created_at >= '2026-09-01'` e Iceberg traduce la función de partición automáticamente en poda de archivos.
3. **Partition Evolution:** Permite cambiar la granularidad del particionamiento (por ejemplo, de mensual a diaria) **sin reescribir los datos históricos**. Los datos nuevos adoptan la nueva estrategia y los antiguos retienen la anterior; las consultas transparentemente leen ambos.
4. **Schema Evolution Seguro con Field IDs:** Cada columna posee un identificador numérico único que no cambia nunca. Renombrar, reordenar o añadir columnas jamás rompe datos históricos ni consultas existentes, eliminando errores por nombres de campos ambiguos.

### 4.3. Apache Hudi: Especialista en Streaming y Mutaciones CDC

Creado por Uber para gestionar millones de mutaciones de viajes y estados de conductores en tiempo real con latencias de segundos.

```
FLUJO MERGE-ON-READ (MoR) EN APACHE HUDI

1. ESCRITURA INMEDIATA
   Eventos CDC (Upserts) ────► [Delta Logs en Avro] (Escritura veloz a baja latencia)
                                      │
                                      │ (Junto al Parquet base existente)
                                      ▼
                               [Base File Parquet]

2. LECTURA FLEXIBLE
   ├── Read-Optimized View: Lee solo el archivo Parquet (Sin costo de merge; dato batch).
   └── Real-time View: Combina en memoria [Parquet Base] + [Delta Avro] (Dato al segundo).

3. COMPACTION ASÍNCRONO
   Proceso en background une [Parquet] + [Delta Avro] ────► [Nuevo Parquet Base Consolidado].
```

#### Modos de Almacenamiento y Mutación
* **Copy-on-Write (CoW):** Cada `UPDATE` reescribe por completo los archivos Parquet afectados con las filas nuevas integradas. Produce lecturas analíticas limpias y rápidas, pero penaliza el tiempo de escritura.
* **Merge-on-Read (MoR):** Las actualizaciones se escriben en archivos de log delta en **formato Avro** contiguos al archivo base Parquet. La escritura es casi instantánea.
* **Timeline Service:** Núcleo transaccional que rastrea todas las acciones (commits, compactions, cleans) cronológicamente, permitiendo capacidades de streaming incremental nativas.

---

## 5. Matriz Maestra de Decisión Técnica

| Dimensión de Análisis | Delta Lake | Apache Iceberg | Apache Hudi |
| :--- | :--- | :--- | :--- |
| **Estructura de Metadatos** | Log lineal secuencial (`JSON` + Checkpoints `Parquet`) | Árbol jerárquico de Snapshots (`JSON` + `Avro`) | Línea de tiempo (`Timeline`) e índices por registro |
| **Patrón de Mutaciones** | CoW con soporte Merge optimizado | CoW y MoR (mediante *Delete Files* posicionales/igualdad) | CoW nativo y MoR avanzado con deltas en Avro |
| **Evolución de Particiones** | Restringido (favorece *Liquid Clustering*) | **Totalmente transparente** (*Hidden Partitioning*) | Limitada (suele requerir migración lógica) |
| **Gobernanza Multimotor** | Excelente en Databricks/Fabric; abierto vía UniForm | **Máxima neutralidad** (BigQuery, Snowflake, Trino, Spark) | Fuerte en ecosistemas Spark, Flink y Presto |
| **Carga de Trabajo Ideal** | Plataformas Spark/Databricks empresariales | Lakehouses multi-cloud abiertos sin dependencia de vendor | Ingesta continua CDC y micro-batching de alta frecuencia |

---

## 6. Patrón de Arquitectura de Referencia en Producción

En una arquitectura empresarial madura, los formatos colaboran simbióticamente a lo largo de las distintas zonas de refinamiento:

```
[Sistemas Transaccionales / CDC / IoT]
                   │
                   ▼ (Streaming continuo)
        ┌──────────────────────┐
        │     Apache Avro      │  <─── Ingesta a baja latencia con Schema Registry
        │ (Kafka / Datastream) │       (Cero impacto en las fuentes transaccionales).
        └──────────┬───────────┘
                   │
                   ▼ (Carga continua / Micro-batch)
        ┌──────────────────────┐
        │  Capa Bronze (Raw)   │  <─── Persistencia inmutable append-only en Storage.
        └──────────┬───────────┘
                   │
                   ▼ (Transformaciones ELT / dbt / Spark)
        ┌──────────────────────┐
        │  Capa Silver (Clean) │  <─── Apache Iceberg o Delta Lake sobre Parquet.
        │                      │       Deduplicación, tipos validados, particiones.
        └──────────┬───────────┘
                   │
                   ▼ (Modelado Dimensional Kimball / Data Marts)
        ┌──────────────────────┐
        │   Capa Gold (Mart)   │  <─── Parquet ultra-comprimido (ZSTD) gobernado
        │                      │       por Iceberg (BigQuery BigLake / Trino / BI).
        └──────────────────────┘
```

---

## 7. Referencias Técnicas y Bibliografía Fundacional

### 7.1. Papers Académicos y Fundacionales de Formatos y Motores
* **Melnik, S., et al. (2010).** *Dremel: Interactive Analysis of Web-Scale Datasets*. Proceedings of the VLDB Endowment (PVLDB), 3(1-2), 330–339. [DOI: 10.14778/1920841.1920886](https://doi.org/10.14778/1920841.1920886).
  *Paper que introdujo el algoritmo de almacenamiento columnar para datos anidados que define la arquitectura interna de Apache Parquet.*
* **Armbrust, M., et al. (2020).** *Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores*. Proceedings of the VLDB Endowment, 13(12), 3411–3424. [DOI: 10.14778/3415478.3415560](https://doi.org/10.14778/3415478.3415560).
  *Presentación formal del diseño del log transaccional y control de concurrencia optimista sobre almacenamiento de objetos desacoplado.*

### 7.2. Especificaciones Técnicas y Estándares Abiertos Oficiales
* **Apache Iceberg Specification (2025).** *The Apache Iceberg Table Spec*. [Apache Iceberg Docs](https://iceberg.apache.org/spec/).
  *Documentación de referencia sobre la arquitectura de tres niveles de metadatos, formato de Manifest Lists y particionamiento oculto.*
* **Apache Parquet Format Specification (2024).** *Parquet Format and Encodings (RLE, Dictionary, Page Headers)*. [Apache Parquet Docs](https://parquet.apache.org/docs/).
* **Apache Avro Specification (2024).** *Avro Schema and Object Container File Format Spec*. [Apache Avro Docs](https://avro.apache.org/docs/).
* **Apache Hudi Technical Architecture (2025).** *Storage Types (CoW / MoR) and Timeline Service Architecture*. [Apache Hudi Docs](https://hudi.apache.org/docs/overview).
* **Delta Lake Protocol Specification (2024).** *The Delta Transaction Log Protocol*. [Delta Lake GitHub Protocol](https://github.com/delta-io/delta/blob/master/PROTOCOL.md).

### 7.3. Literatura de Ingeniería y Sistemas Distribuidos
* **Kleppmann, M. (2017).** *Designing Data-Intensive Applications: The Big Ideas Behind Reliable, Scalable, and Maintainable Systems*. O'Reilly Media. ISBN: 978-1449373320.
  *Capítulo 3: Storage and Retrieval (Column-oriented storage, Avro encoding, SSTables y B-Trees).*
* **Google Cloud Architecture Center (2025).** *BigLake Architecture: Unifying Data Lakes and Warehouses with Apache Iceberg*. [Google Cloud Documentation](https://cloud.google.com/bigquery/docs/biglake-intro).
