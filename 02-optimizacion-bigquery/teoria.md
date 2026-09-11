# Optimización Avanzada, Arquitectura y Rendimiento en Google Cloud BigQuery

> **Audiencia Objetivo:** Principal Data Architects, Analytics Engineers, Database Administrators (DBAs), FinOps Practitioners y Data Platform Leads.
>
> **Alcance:** Desentrañar la arquitectura interna de BigQuery (Dremel, Capacitor, Colossus, Jupiter, Borg), los fundamentos físicos de diseño (Particionamiento, Clustering, Search Indexes), el análisis del plan de ejecución distribuido (Slots, Shuffle, Skew), patrones y antipatrones de SQL de alto rendimiento, y gobernanza financiera (FinOps).
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 02 — BigQuery: Particionamiento, Clustering y Optimización de SQL](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo y Marco Conceptual](#1-resumen-ejecutivo-y-marco-conceptual)
2. [Arquitectura de Desacoplamiento Extremo de BigQuery](#2-arquitectura-de-desacoplamiento-extremo-de-bigquery)
   - [2.1. Dremel: Motor de Ejecución Masivamente Paralelo (MPP)](#21-dremel-motor-de-ejecución-masivamente-paralelo-mpp)
   - [2.2. Colossus y Formato Capacitor: Persistencia Columnar Nativa](#22-colossus-y-formato-capacitor-persistencia-columnar-nativa)
   - [2.3. Red Jupiter: Desacoplamiento Petabit sin Cuellos de Botella](#23-red-jupiter-desacoplamiento-petabit-sin-cuellos-de-botella)
   - [2.4. Borg: Orquestación Elástica y Dinámica de Slots](#24-borg-orquestación-elástica-y-dinámica-de-slots)
3. [Diseño Físico de Almacenamiento: Particionamiento y Clustering](#3-diseño-físico-de-almacenamiento-particionamiento-y-clustering)
   - [3.1. Particionamiento (`PARTITION BY`): Podado de Metadatos](#31-particionamiento-partition-by-podado-de-metadatos)
   - [3.2. Clustering (`CLUSTER BY`): Co-ubicación y Podado de Bloques](#32-clustering-cluster-by-co-ubicación-y-podado-de-bloques)
   - [3.3. Matriz de Combinación: ¿Cuándo usar Partición, Cluster o Ambos?](#33-matriz-de-combinación-cuándo-usar-partición-cluster-o-ambos)
   - [3.4. Guardrails de Protección: `require_partition_filter`](#34-guardrails-de-protección-require_partition_filter)
4. [Anatomía del Plan de Ejecución de Consultas (Query Plan)](#4-anatomía-del-plan-de-ejecución-de-consultas-query-plan)
   - [4.1. Unidades de Medida: Slots, Slot Time y Elapsed Time](#41-unidades-de-medida-slots-slot-time-y-elapsed-time)
   - [4.2. Etapas de Ejecución (*Query Stages*): Input, Compute y Output](#42-etapas-de-ejecución-query-stages-input-compute-y-output)
   - [4.3. El Shuffle Distribuido en Memoria: Corazón de los JOINs y Agregaciones](#43-el-shuffle-distribuido-en-memoria-corazón-de-los-joins-y-agregaciones)
   - [4.4. Diagnóstico de Síntomas Críticos: Data Skew y Spill-to-Disk](#44-diagnóstico-de-síntomas-críticos-data-skew-y-spill-to-disk)
5. [Catálogo Maestro de Optimización de SQL y Antipatrones](#5-catálogo-maestro-de-optimización-de-sql-y-antipatrones)
   - [5.1. Antipatrón 1: Proyección Descontrolada (`SELECT *`)](#51-antipatrón-1-proyección-descontrolada-select-)
   - [5.2. Antipatrón 2: Filtrado Tardío post-JOIN](#52-antipatrón-2-filtrado-tardío-post-join)
   - [5.3. Antipatrón 3: Orden de JOINs y Mecánica Broadcast vs. Hash](#53-antipatrón-3-orden-de-joins-y-mecánica-broadcast-vs-hash)
   - [5.4. Antipatrón 4: Transformaciones Escalares sobre Claves de Filtro](#54-antipatrón-4-transformaciones-escalares-sobre-claves-de-filtro)
   - [5.5. Antipatrón 5: Cardinalidad Exacta vs. HyperLogLog++ (`APPROX_COUNT_DISTINCT`)](#55-antipatrón-5-cardinalidad-exacta-vs-hyperloglog-approx_count_distinct)
   - [5.6. Antipatrón 6: Ordenamiento Global sin Límite (`ORDER BY` Huérfano)](#56-antipatrón-6-ordenamiento-global-sin-límite-order-by-huérfano)
   - [5.7. Antipatrón 7: `UNION DISTINCT` Innecesario frente a `UNION ALL`](#57-antipatrón-7-union-distinct-innecesario-frente-a-union-all)
   - [5.8. Antipatrón 8: Subconsultas Correlacionadas vs. Window Functions y `QUALIFY`](#58-antipatrón-8-subconsultas-correlacionadas-vs-window-functions-y-qualify)
6. [FinOps Estratégico y Modelos de Facturación](#6-finops-estratégico-y-modelos-de-facturación)
   - [6.1. On-Demand vs. BigQuery Editions (Standard, Enterprise, Enterprise Plus)](#61-on-demand-vs-bigquery-editions-standard-enterprise-enterprise-plus)
   - [6.2. Estimación Previa y Validación de Costos a Cero Dólares (`dry_run`)](#62-estimación-previa-y-validación-de-costos-a-cero-dólares-dry_run)
   - [6.3. Observabilidad con `INFORMATION_SCHEMA.JOBS_BY_*`](#63-observabilidad-con-information_schemajobs_by_)
   - [6.4. Políticas de Guardrail: `maximum_bytes_billed` y Presupuestos](#64-políticas-de-guardrail-maximum_bytes_billed-y-presupuestos)
7. [Árbol de Decisión y Diagnóstico de Rendimiento](#7-árbol-de-decisión-y-diagnóstico-de-rendimiento)
8. [Referencias Técnicas y Bibliografía Fundacional](#8-referencias-técnicas-y-bibliografía-fundacional)

---

## 1. Resumen Ejecutivo y Marco Conceptual

En almacenes de datos analíticos modernos (Cloud Data Warehouses) con motores masivamente paralelos (MPP) como **Google Cloud BigQuery**, el paradigma clásico de ajuste (*tuning*) de bases de datos relacionales tradicionales (índices B-Tree, locks de fila, tamaño de bloque de disco, `VACUUM` y parámetros de memoria de conexión) es completamente irrelevante.

BigQuery opera bajo un modelo **totalmente serverless y con desacoplamiento extremo entre cómputo y almacenamiento**. En este entorno, el rendimiento y el costo no son variables separadas: **están intrínsecamente sincronizadas**. Una consulta que escanea terabytes innecesarios no solo tarda más en responder, sino que cuesta múltiplos de dinero en modelos de facturación On-Demand o satura la cuota de *slots* en compromisos de Editions (*Capacity/Reservations*), afectando los SLAs de toda la organización.

```
EL CICLO VIRTUOSO DE LA OPTIMIZACIÓN EN BIGQUERY

             Diseño Físico Eficiente
             (Partición + Clustering)
                   ▲         │
                   │         ▼
   Gobernanza FinOps        Menor I/O en Disco
   y Control de Costos      (Menos Bytes Escaneados)
                   ▲         │
                   │         ▼
             Consultas SQL  Menor Presión en Shuffle
             Optimizadas  ◄ y Reducción de Slot Time
```

Optimizar en BigQuery exige comprender con precisión matemática **cómo Dremel descompone una consulta SQL en árboles de ejecución**, cómo **Capacitor descarta bloques de datos en disco antes de leerlos**, cómo la **red Jupiter transporta gigabytes de datos en el shuffle tier**, y cómo escribir SQL que minimice la transferencia de bytes entre etapas.

---

## 2. Arquitectura de Desacoplamiento Extremo de BigQuery

BigQuery no es una máquina virtual con un motor SQL instalado; es una orquestación distribuida de **cuatro sistemas planetarios independientes de Google Cloud**:

```
ARQUITECTURA DE CUATRO PILARES DE BIGQUERY
┌────────────────────────────────────────────────────────────────────────┐
│ 1. CÓMPUTO DISTRIBUIDO: Dremel Execution Engine                        │
│    Árbol multinivel de slots dinámicos (Root, Mixers, Leaf Nodes)      │
├────────────────────────────────────────────────────────────────────────┤
│ 2. RED PETABIT: Jupiter Network Fabric                                 │
│    1 Petabit/segundo de ancho de banda biseccional sin colisiones      │
├────────────────────────────────────────────────────────────────────────┤
│ 3. SHUFFLE EN MEMORIA: Distributed Shuffle Tier                        │
│    Intercambio ultra-rápido de estados intermedios en RAM distribuida  │
├────────────────────────────────────────────────────────────────────────┤
│ 4. ALMACENAMIENTO COLUMNAR: Colossus File System + Formato Capacitor  │
│    Compresión avanzada (RLE, Bit-Packing) y metadatos de min/max       │
└────────────────────────────────────────────────────────────────────────┘
```

### 2.1. Dremel: Motor de Ejecución Masivamente Paralelo (MPP)

**Dremel** es el motor de procesamiento de consultas creado por Google en 2006 (y formalizado en su paper seminal de 2010 y 2020). Dremel organiza la ejecución de una consulta mediante una jerarquía distribuida en árbol:

```
JERARQUÍA DE EJECUCIÓN DREMEL
                   ┌──────────────┐
                   │  Root Server │ ◄── Recibe el SQL y devuelve el resultado final
                   └──────┬───────┘
                          │
             ┌────────────┴────────────┐
             ▼                         ▼
      ┌──────────────┐          ┌──────────────┐
      │  Mixer Node  │          │  Mixer Node  │ ◄── Agregaciones intermedias
      └──────┬───────┘          └──────┬───────┘
             │                         │
     ┌───────┴───────┐         ┌───────┴───────┐
     ▼               ▼         ▼               ▼
┌─────────┐     ┌─────────┐ ┌─────────┐     ┌─────────┐
│Leaf Slot│     │Leaf Slot│ │Leaf Slot│     │Leaf Slot│ ◄── Leen Capacitor vía red
└─────────┘     └─────────┘ └─────────┘     └─────────┘
```

1. **Root Server:** Recibe la consulta SQL, parsea el texto, consulta el catálogo de metadatos, optimiza el árbol lógico y genera el plan de ejecución físico (*Stages*).
2. **Mixer Nodes:** Capas intermedias que coordinan el procesamiento paralelo y fusionan las agregaciones parciales producidas por las capas inferiores.
3. **Leaf Nodes (Slots):** Nodos de cómputo en la base del árbol. Se comunican directamente con Colossus a través de la red Jupiter para leer únicamente las columnas necesarias, aplicar filtros locales y proyectar resultados preliminares.

### 2.2. Colossus y Formato Capacitor: Persistencia Columnar Nativa

Los datos de BigQuery no se guardan en el mismo disco de los servidores de cómputo; residen en **Colossus**, el sistema de archivos distribuido de última generación de Google (sucesor de GFS).

Dentro de Colossus, las tablas se serializan en el formato columnar propietario **Capacitor** (el equivalente interno de Google para Parquet/ORC):
- **Orientación a columnas:** Cada columna se comprime de forma aislada utilizando el algoritmo más adecuado para su distribución estadística (Run-Length Encoding - RLE, Bit-Packing, Dictionary Encoding, Snappy, ZSTD).
- **Estadísticas embebidas:** Cada bloque de archivo (*stripe/chunk*) en Capacitor almacena los valores mínimos y máximos (`min/max`), conteos de nulos y diccionarios de valores. Esto permite al motor realizar **Block Pruning** (descarte de bloques completos de disco sin leerlos si el predicado `WHERE` está fuera del rango).

### 2.3. Red Jupiter: Desacoplamiento Petabit sin Cuellos de Botella

Tradicionalmente, en bases de datos distribuidas se buscaba la "co-ubicación de datos y cómputo" (*data locality*) porque mover datos por la red era prohibitivamente lento. 

Google resolvió este problema con **Jupiter**, una infraestructura de red de centro de datos basada en topología Clos de tres capas:
- Proporciona **más de 1 Petabit por segundo** de ancho de banda de bisección total.
- Permite que miles de slots de Dremel lean cientos de terabytes de almacenamiento en Colossus a la misma velocidad que si estuvieran en un bus PCI local, desacoplando completamente el escalado del cómputo del almacenamiento.

### 2.4. Borg: Orquestación Elástica y Dinámica de Slots

BigQuery no mantiene máquinas virtuales encendidas esperando consultas. Las tareas de cómputo se ejecutan como contenedores efímeros administrados por **Borg** (el predecesor interno de Kubernetes en Google). Cuando una consulta arranca, Borg asigna instantáneamente cientos o miles de **Slots** (unidades de cómputo equivalentes a CPU + RAM) y los libera en el instante en que la etapa finaliza.

---

## 3. Diseño Físico de Almacenamiento: Particionamiento y Clustering

El diseño físico en BigQuery tiene un objetivo unívoco: **evitar que los Leaf Slots lean bytes innecesarios de Colossus**.

```
MECÁNICA FÍSICA DE PODADO (PRUNING)

 CONSULTA: WHERE fecha = '2026-03-01' AND ciudad = 'Madrid'

 ┌─────────────────────────────────────────────────────────────┐
 │ TABLA COMPLETA: 100 GB (100% de los datos)                  │
 └──────────────────────────────┬──────────────────────────────┘
                                │
               1. PARTITION BY fecha (Podado de Partición)
               ► Descarta todas las fechas salvo 2026-03-01
                                │
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ PARTICIÓN 2026-03-01: 5 GB (Descarta 95 GB a nivel metadatos)│
 └──────────────────────────────┬──────────────────────────────┘
                                │
               2. CLUSTER BY ciudad (Podado de Bloques Capacitor)
               ► Descarta bloques donde min/max no incluye 'Madrid'
                                │
                                ▼
 ┌─────────────────────────────────────────────────────────────┐
 │ BYTES REALMENTE LEÍDOS DE DISCO: 350 MB (Ahorro del 99.65%) │
 └─────────────────────────────────────────────────────────────┘
```

### 3.1. Particionamiento (`PARTITION BY`): Podado de Metadatos

El particionamiento segmenta físicamente la tabla en particiones lógicas discretas basadas en una columna clave.

#### Tipos de Particionamiento Soportados:
1. **Por columna de fecha/hora (`TIMESTAMP`, `DATE`, `DATETIME`):**
   - Granularidad: Por hora (`HOUR`), día (`DAY`), mes (`MONTH`) o año (`YEAR`).
   ```sql
   CREATE TABLE taxi_lab.trips_partitioned
   PARTITION BY DATE(pickup_datetime) AS ...
   ```
2. **Por tiempo de ingestión (`_PARTITIONTIME`):**
   - La tabla se particiona automáticamente según la fecha/hora en que BigQuery recibió los datos.
3. **Por rango de enteros (`RANGE_BUCKET`):**
   - Útil para IDs numéricos continuos (ej. códigos postales, IDs de cliente).
   ```sql
   PARTITION BY RANGE_BUCKET(customer_id, GENERATE_ARRAY(0, 1000000, 10000))
   ```

#### Límites Arquitectónicos Críticos:
- **Límite de 4.000 particiones por tabla:** Si particionas por día, 4.000 particiones equivalen a ~10.9 años de datos. Particionar por hora solo cubre ~166 días antes de exceder el límite.
- Si los datos contienen marcas temporales anómalas (ej. fechas en el año `1970` o en el `2099`), BigQuery las asigna a dos particiones especiales: `__NULL__` y `__UNPARTITIONED__`.

### 3.2. Clustering (`CLUSTER BY`): Co-ubicación y Podado de Bloques

El clustering ordena los datos dentro de cada partición (o dentro de toda la tabla si no está particionada) en función del contenido de **hasta cuatro columnas**.

#### Mecánica Interna:
- BigQuery agrupa las filas con valores similares en los mismos bloques físicos de Capacitor.
- Al ejecutar una consulta con filtros `WHERE` o agregaciones `GROUP BY` sobre las columnas clusterizadas, Dremel utiliza las estadísticas `min/max` de cada bloque para saltarse los bloques que no contienen el valor buscado (*Block Pruning*).
- **Auto-Reclustering Automático:** A diferencia de bases de datos tradicionales que requieren comandos manuales como `OPTIMIZE` o `VACUUM`, BigQuery reclusteriza continuamente los datos en segundo plano sin costo de cómputo adicional para el usuario y sin bloquear lecturas ni escrituras.

### 3.3. Matriz de Combinación: ¿Cuándo usar Partición, Cluster o Ambos?

```
ÁRBOL DE DECISIÓN DE DISEÑO FÍSICO

¿La tabla supera 1 GB o acumula millones de filas?
  ├── NO  ──► Tabla Plana Estándar (La sobrecarga de metadatos supera el beneficio).
  └── SÍ  ──► ¿Las consultas filtran principalmente por un rango de fechas/horas continuo?
                ├── SÍ ──► Aplica PARTITION BY fecha
                │            │
                │            └── ¿Además filtran frecuentemente por columnas de alta cardinalidad
                │                (IDs, regiones, tipos, categorías)?
                │                  ├── SÍ ──► AGREGA CLUSTER BY (col1, col2, col3)
                │                  └── NO ──► Solo PARTITION BY
                │
                └── NO ──► ¿Las consultas filtran por IDs o categorías sin eje temporal claro?
                             └── SÍ ──► Solo CLUSTER BY (hasta 4 columnas en orden de frecuencia)
```

| Criterio | Particionamiento (`PARTITION BY`) | Clustering (`CLUSTER BY`) |
|---|---|---|
| **Límite de Columnas** | Exactamente 1 columna | De 1 a 4 columnas ordenadas jerárquicamente |
| **Cardinalidad de la Columna** | **Baja a Media:** Máximo 4.000 valores únicos | **Media a Muy Alta:** Sin límite de cardinalidad |
| **Garantía de Costo Previo** | **Estricta:** El validador y `dry_run` conocen los bytes exactos antes de ejecutar. | **Dinámica:** La reducción se mide durante la ejecución al descartar bloques. |
| **Operaciones Beneficiadas** | Filtros `WHERE` directos por fecha/rango | Filtros `WHERE`, operadores de igualdad (`=`), `IN`, `LIKE 'ABC%'`, `GROUP BY` y `JOIN`. |

### 3.4. Guardrails de Protección: `require_partition_filter`

En entornos empresariales donde analistas de datos o herramientas de BI (Looker, Tableau) conectan directamente a BigQuery, un usuario descuidado que ejecute `SELECT * FROM ventas` sobre una tabla particionada de 50 TB escaneará la tabla entera, incurriendo en un gasto de cientos de dólares en un solo segundo.

Para prevenir esto, BigQuery ofrece la opción **`require_partition_filter = true`**:

```sql
ALTER TABLE taxi_lab.trips_optimized
SET OPTIONS (require_partition_filter = true);
```

Si una consulta intenta leer la tabla sin un predicado explícito sobre la columna particionada en el `WHERE`, **el motor rechaza la consulta instantáneamente en milisegundos con costo $0**.

---

## 4. Anatomía del Plan de Ejecución de Consultas (Query Plan)

Para diagnosticar una consulta lenta o costosa, es indispensable inspeccionar el **Grafo de Ejecución (Execution Graph)** disponible en BigQuery Studio o a través de la API en `INFORMATION_SCHEMA`.

### 4.1. Unidades de Medida: Slots, Slot Time y Elapsed Time

$$\text{Slot Time (Tiempo de Slot)} = \sum_{i=1}^{M} \text{tiempo consumido por el slot } i$$
$$\text{Elapsed Time (Tiempo Real de Reloj)} \approx \frac{\text{Slot Time}}{\text{Número Promedio de Slots en Paralelo}}$$

- **Elapsed Time:** El tiempo real transcurrido que el usuario espera en su pantalla (ej. `5.2 segundos`).
- **Slot Time (CPU Time):** El trabajo computacional total sumado de todos los procesadores asignados (ej. `3 minutos y 45 segundos de CPU`).
- Si una consulta tiene un `Slot Time` altísimo pero un `Elapsed Time` corto, significa que BigQuery paralizó el trabajo eficientemente en cientos de slots.
- Si el `Elapsed Time` es alto pero el `Slot Time` es bajo, el problema no es cómputo, sino contención de I/O, latencia de red o dependencias lineales bloqueantes.

### 4.2. Etapas de Ejecución (*Query Stages*): Input, Compute y Output

Cada consulta se compila en un Grafo Acíclico Dirigido (DAG) dividido en etapas discretas denominadas **Stages** (`S00`, `S01`, `S02`...):

```
ESTRUCTURA TÍPICA DE UN STAGE EN DREMEL
┌────────────────────────────────────────────────────────────────────────┐
│ STAGE S01: Input                                                       │
├────────────────────────────────────────────────────────────────────────┤
│ READ:  Lee columnas id, timestamp, amount desde Colossus (con pruning) │
│ COMPUTE: Evalúa WHERE timestamp >= '2026-01-01'                        │
│ AGGREGATE: Agrupación local parcial SUM(amount) por id                 │
│ WRITE: Escribe registros intermedios en el Distributed Shuffle Tier   │
└────────────────────────────────────────────────────────────────────────┘
```

1. **Input:** Tiempo que los slots dedican a leer datos desde Colossus o desde el Shuffle de la etapa anterior.
2. **Compute:** Tiempo activo de procesamiento de CPU evaluando funciones, expresiones matemáticas, hashes y filtros.
3. **Output:** Tiempo consumido escribiendo los resultados calculados hacia el siguiente nivel de Shuffle o hacia el resultado final.

### 4.3. El Shuffle Distribuido en Memoria: Corazón de los JOINs y Agregaciones

El **Distributed Shuffle Tier** es una de las mayores ventajas arquitectónicas de BigQuery:
- Cuando una consulta ejecuta un `JOIN` o un `GROUP BY`, los datos con la misma clave deben viajar al mismo slot físico para consolidarse (*repartitioning*).
- En tecnologías tradicionales (como Apache Spark o Hadoop), el shuffle escribe pesados archivos intermedios en el disco local del nodo trabajador, generando cuellos de botella de I/O.
- BigQuery ejecuta el shuffle en un **clúster masivo de memoria RAM dedicada desacoplada**, permitiendo intercambiar terabytes de estados intermedios en cuestión de segundos.

### 4.4. Diagnóstico de Síntomas Críticos: Data Skew y Spill-to-Disk

Al inspeccionar un Stage en la interfaz gráfica, presta especial atención a la métrica de tiempo de los slots:
- **Average Time (Tiempo promedio):** Tiempo medio de todos los slots asignados a esa etapa.
- **Max Time (Tiempo máximo):** Tiempo consumido por el slot más lento (*tail latency*).

```
DIAGNÓSTICO VISUAL DE DATA SKEW (SESGO DE DATOS)
┌─────────────────────────────────────────────────────────────┐
│ Slot 1: [████] (0.8s)                                       │
│ Slot 2: [█████] (1.1s)                                      │
│ Slot 3: [████] (0.9s)                                       │
│ Slot 4: [████████████████████████████████████████████] (45s)│ ◄── DATA SKEW!
└─────────────────────────────────────────────────────────────┘
```

#### ¿Qué significa este patrón?
Si `Max Time` es 10x o 50x mayor que `Average Time`, existe **Data Skew (Sesgo de Datos)**. Un solo valor de la clave de unión o de agrupación (por ejemplo, registros con `id IS NULL` o transacciones de un cliente masivo tipo corporativo) cayó en un único slot, obligándolo a procesar millones de filas mientras todos los demás slots permanecen ociosos esperando.

---

## 5. Catálogo Maestro de Optimización de SQL y Antipatrones

### 5.1. Antipatrón 1: Proyección Descontrolada (`SELECT *`)

```sql
-- ❌ PÉSIMA PRÁCTICA: Escanea el 100% de los bytes de la tabla
SELECT *
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
WHERE pickup_datetime >= '2018-06-01' AND pickup_datetime < '2018-06-02';

-- ✅ BUENA PRÁCTICA: Proyección quirúrgica
SELECT vendor_id, pickup_datetime, trip_distance, total_amount
FROM `bigquery-public-data.new_york_taxi_trips.tlc_yellow_trips_2018`
WHERE pickup_datetime >= '2018-06-01' AND pickup_datetime < '2018-06-02';
```

#### Por qué importa:
BigQuery es estrictamente columnar. Si la tabla tiene 50 columnas y solo utilizas 4, `SELECT *` factura y procesa **12.5 veces más datos de forma totalmente inútil**.

> ⚠️ **Mito común:** Usar `LIMIT 10` con `SELECT *` **NO reduce los bytes escaneados**. El motor debe escanear los bloques completos de todas las columnas antes de aplicar el límite.

### 5.2. Antipatrón 2: Filtrado Tardío post-JOIN

```sql
-- ❌ PÉSIMA PRÁCTICA: El motor ejecuta el JOIN de millones de filas antes de filtrar
SELECT o.order_id, c.customer_name
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.order_date = '2026-03-01';

-- ✅ BUENA PRÁCTICA: Filtrar la tabla de hechos antes o dentro de la unión
SELECT o.order_id, c.customer_name
FROM (
  SELECT order_id, customer_id
  FROM orders
  WHERE order_date = '2026-03-01'
) o
JOIN customers c ON o.customer_id = c.id;
```

Aunque el optimizador moderno de BigQuery intenta empujar predicados hacia abajo (*Filter Pushdown*), en consultas complejas con funciones de ventana o agrupaciones intermedias el optimizador puede fallar. Filtrar explícitamente en subconsultas o CTEs garantiza el mínimo volumen en el Shuffle.

### 5.3. Antipatrón 3: Orden de JOINs y Mecánica Broadcast vs. Hash

BigQuery implementa dos estrategias físicas principales para resolver `JOIN`:

1. **Broadcast Join:** Si una de las tablas del `JOIN` es pequeña (generalmente < 100 MB), BigQuery envía una copia completa de la tabla pequeña a todos los slots que procesan la tabla grande. **No requiere shuffle de la tabla grande**, resultando en una velocidad máxima.
2. **Hash Distributed Join:** Si ambas tablas son grandes, BigQuery rehashea y envía ambas tablas a través de la red Jupiter hacia el Distributed Shuffle Tier, consumiendo considerables recursos de red y slots.

#### Regla de Oro:
**Coloca siempre la tabla de mayor volumen a la izquierda** (en la cláusula `FROM`) y las tablas de menor volumen a la derecha (en la cláusula `JOIN`):

```sql
-- ✅ Tabla masiva a la izquierda, lookup a la derecha
FROM trips_raw t                        -- 112 millones de filas
JOIN taxi_zone_geom z                   -- 263 filas (Broadcast Join automático)
  ON t.pickup_location_id = z.zone_id
```

### 5.4. Antipatrón 4: Transformaciones Escalares sobre Claves de Filtro

```sql
-- ❌ PÉSIMA PRÁCTICA: Rompe el podado de partición (Full Table Scan)
SELECT count(*)
FROM taxi_lab.trips_partitioned
WHERE TIMESTAMP_TRUNC(pickup_datetime, DAY) = TIMESTAMP('2018-06-01');

-- ✅ BUENA PRÁCTICA: Expresar el filtro con rango estricto sobre el tipo nativo
SELECT count(*)
FROM taxi_lab.trips_partitioned
WHERE pickup_datetime >= '2018-06-01 00:00:00'
  AND pickup_datetime < '2018-06-02 00:00:00';
```

Al envolver la columna de partición en una función escalar arbitraria, el optimizador no puede resolver en fase de metadatos qué particiones podar y se ve forzado a leer todas las particiones de la tabla.

### 5.5. Antipatrón 5: Cardinalidad Exacta vs. HyperLogLog++ (`APPROX_COUNT_DISTINCT`)

Calcular un conteo distintivo exacto (`COUNT(DISTINCT user_id)`) sobre miles de millones de filas requiere que BigQuery envíe todos los IDs únicos al Shuffle Tier para deduplicar valores en un único punto.

```sql
-- ❌ Exacto: Alto costo de CPU, memoria y riesgo de Spill-to-Disk
SELECT COUNT(DISTINCT user_id) FROM web_events;

-- ✅ Aproximado: Utiliza HyperLogLog++ (Error estadístico típico < 1%, 10x más rápido)
SELECT APPROX_COUNT_DISTINCT(user_id) FROM web_events;
```

Para paneles de control ejecutivos y análisis exploratorio, el algoritmo probabilístico **HyperLogLog++** ahorra más del 90% de slot time con un error típicamente inferior al 1%.

### 5.6. Antipatrón 6: Ordenamiento Global sin Límite (`ORDER BY` Huérfano)

```sql
-- ❌ PÉSIMA PRÁCTICA: Serializa el trabajo de cientos de slots en un único worker
SELECT * FROM gran_tabla ORDER BY importe DESC;

-- ✅ BUENA PRÁCTICA: Limitar el resultado
SELECT * FROM gran_tabla ORDER BY importe DESC LIMIT 100;
```

Un `ORDER BY` sin `LIMIT` exige que todos los datos calculados en paralelo se envíen a un **único nodo final** para ordenarse en memoria secuencial, generando el clásico error:
`Resources exceeded during query execution: The query could not be executed in the allocated memory`.

### 5.7. Antipatrón 7: `UNION DISTINCT` Innecesario frente a `UNION ALL`

- `UNION DISTINCT` (o simplemente `UNION`): Ejecuta una etapa oculta de hash y deduplicación en el Shuffle para eliminar filas idénticas.
- `UNION ALL`: Simplemente concatena los flujos de datos sin costo de deduplicación.

> 💡 **Regla:** Si sabes de antemano que los conjuntos de datos no se solapan (ej. unir datos de 2024 con datos de 2025), usa siempre `UNION ALL`.

### 5.8. Antipatrón 8: Subconsultas Correlacionadas vs. Window Functions y `QUALIFY`

Para filtrar el registro más reciente por entidad (patrón deduplicación / última versión):

```sql
-- ❌ PÉSIMA PRÁCTICA: Doble scan y JOIN correlacionado
SELECT t.*
FROM transactions t
JOIN (
  SELECT user_id, MAX(trans_time) AS max_time
  FROM transactions GROUP BY user_id
) m ON t.user_id = m.user_id AND t.trans_time = m.max_time;

-- ✅ BUENA PRÁCTICA: Window Function con la cláusula nativa QUALIFY
SELECT *
FROM transactions
QUALIFY ROW_NUMBER() OVER(PARTITION BY user_id ORDER BY trans_time DESC) = 1;
```

La cláusula `QUALIFY` ejecuta la ventana y el filtro en una sola pasada de lectura (*single pass*), reduciendo el consumo de I/O y slots a la mitad.

---

## 6. FinOps Estratégico y Modelos de Facturación

### 6.1. On-Demand vs. BigQuery Editions (Standard, Enterprise, Enterprise Plus)

| Modelo de Precios | Métrica de Facturación | Caso de Uso Ideal | Consideración FinOps Clave |
|---|---|---|---|
| **On-Demand** | **Bytes leídos** ($6.25 / TiB en US, primer TiB/mes gratis) | Cargas de trabajo esporádicas, análisis ad-hoc, desarrollo. | Cada byte cuenta: el particionamiento reduce el costo en dólares directamente. |
| **BigQuery Editions (Slots)** | **Slot-horas consumidas** (Autoscaling o compromisos de capacidad) | Cargas predecibles enterprise, grandes corporaciones, BI continuo. | Los bytes escaneados no alteran el precio directo, pero reducir slot time evita escalar más slots. |

### 6.2. Estimación Previa y Validación de Costos a Cero Dólares (`dry_run`)

Antes de lanzar una consulta pesada en producción o en un pipeline de CI/CD, puedes verificar cuántos bytes va a escanear **sin ejecutarla y sin pagar un solo centavo**:

#### Desde BigQuery CLI:
```bash
bq query --use_legacy_sql=false --dry_run \
  "SELECT pickup_location_id, AVG(fare_amount) FROM \`TU_PROYECTO.taxi_lab.trips_optimized\` WHERE pickup_datetime >= '2018-06-01' AND pickup_datetime < '2018-06-02' GROUP BY 1;"
```

*Salida:*
`Query successfully validated. Assuming the tables are not modified, running this query will process 42.1 MB of data.`

### 6.3. Observabilidad con `INFORMATION_SCHEMA.JOBS_BY_*`

BigQuery expone toda la telemetría histórica de consultas mediante vistas del sistema en tiempo real:

```sql
-- TOP 10 CONSULTAS MÁS COSTOSAS DE LOS ÚLTIMOS 7 DÍAS
SELECT
    project_id,
    user_email,
    job_id,
    creation_time,
    total_bytes_billed / POW(10, 12) AS terabytes_billed,
    (total_bytes_billed / POW(10, 12)) * 6.25 AS estimated_cost_usd,
    total_slot_ms / 1000 AS total_slot_seconds,
    query
FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
WHERE creation_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
  AND job_type = 'QUERY'
  AND state = 'DONE'
ORDER BY total_bytes_billed DESC
LIMIT 10;
```

### 6.4. Políticas de Guardrail: `maximum_bytes_billed` y Presupuestos

Para proteger el presupuesto ante consultas erróneas en clientes programáticos (Python, dbt, Airflow):
- Configura en el cliente API el parámetro `maximum_bytes_billed` (por ejemplo: `107374182400` para 100 GB).
- Si la consulta intentara escanear 101 GB, BigQuery la aborta antes de comenzar a ejecutarla, garantizando costo $0.

---

## 7. Árbol de Decisión y Diagnóstico de Rendimiento

```
DIAGNÓSTICO CUANDO UNA CONSULTA FALLA O SUPERA EL SLA

¿La consulta devuelve "Resources Exceeded"?
 ├── SÍ ──► Revisa el plan de ejecución:
 │           ├── ¿Hay ORDER BY sin LIMIT? ──► Agrega LIMIT o remueve el ordenamiento.
 │           ├── ¿Hay un CROSS JOIN accidental? ──► Revisa las condiciones ON del JOIN.
 │           └── ¿Hay Data Skew masivo? ──► Aisla los valores nulos o claves dominantes.
 │
 └── NO  ──► ¿La consulta es demasiado lenta (alto Elapsed Time)?
              ├── ¿El slot time es bajo pero elapsed time alto?
              │     └── Hay contención por cuota de slots compartida. Considera reservar slots o usar Editions.
              └── ¿El slot time es altísimo?
                    ├── ¿La tabla está particionada? ──► Aplica PARTITION BY fecha y exige filtro.
                    ├── ¿Filtra por columnas de alta cardinalidad? ──► Aplica CLUSTER BY.
                    └── ¿Usa SELECT *? ──► Proyecta exclusivamente las columnas necesarias.
```

---

## 8. Referencias Técnicas y Bibliografía Fundacional

1. **Melnik, S., et al.** (2010). *Dremel: Interactive Analysis of Web-Scale Datasets*. Proceedings of the VLDB Endowment (PVLDB), 3(1-2), 330-339.
2. **Chatziantoniou, D., et al.** (2020). *Dremel: A Decade of Interactive SQL Analysis at Web Scale*. Proceedings of the VLDB Endowment, 13(12), 3461-3472.
3. **Google Cloud Whitepapers:** *BigQuery Under the Hood: The architecture of a modern cloud data warehouse*.
4. **Lakshmanan, V., & Tigani, S.** (2019). *Google BigQuery: The Definitive Guide: Data Warehousing, Analytics, and Machine Learning at Scale*. O'Reilly Media.
5. **Google Cloud Documentation:** [BigQuery Query Plan Explanation](https://cloud.google.com/bigquery/docs/query-plan-explanation) y [Optimizing query performance](https://cloud.google.com/bigquery/docs/best-practices-performance-overview).
