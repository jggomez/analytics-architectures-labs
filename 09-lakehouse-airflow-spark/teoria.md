# Lakehouse en Producción: CDC, Iceberg, Spark y Airflow como lo Operan las Empresas

> **Audiencia Objetivo:** Data Engineers y Data Architects que ya construyeron un pipeline Medallion "de taller" (Módulos 01 y 05) y quieren saber **qué cambia cuando ese pipeline tiene que correr solo, todos los días, con datos sucios, sin perder ni duplicar nada y sin publicar números que nadie validó.**
>
> **Alcance:** No repite CDC ni Medallion ([Módulo 01](../01-patrones-y-modelado/teoria.md)), formatos e Iceberg ([Módulo 03](../03-formatos-y-lakehouse/teoria.md)) ni Datastream hacia archivos ([Módulo 05](../05-elt-dataflow-iceberg/teoria.md)). Se enfoca en las decisiones de un pipeline de producción: catálogos Iceberg, idempotencia, borrados, cuarentena, SCD2, tipos de hechos, orquestación por datos, Write-Audit-Publish y mantenimiento.
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 09 — Lakehouse en Producción: Datastream → Iceberg → Spark → BigQuery, orquestado con Airflow](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo: del Taller al Sistema Real](#1-resumen-ejecutivo-del-taller-al-sistema-real)
2. [Los Nombres de 2026](#2-los-nombres-de-2026)
3. [Arquitectura de Referencia](#3-arquitectura-de-referencia)
4. [Bronze: el Registro Inmutable](#4-bronze-el-registro-inmutable)
5. [Catálogos Iceberg en Google Cloud](#5-catálogos-iceberg-en-google-cloud)
6. [Silver: de Eventos a Estado](#6-silver-de-eventos-a-estado)
7. [Gold: Modelado Dimensional que Aguanta el Mundo Real](#7-gold-modelado-dimensional-que-aguanta-el-mundo-real)
8. [Orquestación con Airflow 3](#8-orquestación-con-airflow-3)
9. [Calidad de Datos en Dos Niveles y Write-Audit-Publish](#9-calidad-de-datos-en-dos-niveles-y-write-audit-publish)
10. [Mantenimiento de Tablas Iceberg](#10-mantenimiento-de-tablas-iceberg)
11. [Operación: Observabilidad, Seguridad y Lab vs. Producción](#11-operación-observabilidad-seguridad-y-lab-vs-producción)
12. [Costos](#12-costos)
13. [Referencias Técnicas](#13-referencias-técnicas)

---

## 1. Resumen Ejecutivo: del Taller al Sistema Real

En un taller, un pipeline está "listo" cuando produce la tabla correcta **una vez**. En una empresa está listo cuando produce la tabla correcta **todas las veces**: cuando el job se cae a la mitad, cuando alguien lo corre dos veces, cuando un evento llega tarde, cuando el origen borra una fila, cuando entra un dato malo y cuando un despliegue introduce un bug.

| Situación | Pipeline de taller | Pipeline de producción |
|---|---|---|
| El job se cae a la mitad | Se corre de nuevo desde cero y se duplican filas | Se reintenta y el resultado es idéntico (**idempotencia**) |
| Hay que procesar lo nuevo | Relee todo en cada corrida | Lee desde un **watermark**, con margen para lo tardío |
| El origen borra una fila | El borrado se pierde o se ignora | Se marca como **borrado lógico** y se propaga |
| Llega un email inválido | El job falla o el dato entra sucio | La fila va a **cuarentena** y el pipeline sigue |
| Un join multiplica filas | Nadie se entera | Una **conciliación** detiene el job |
| Falta una tasa de cambio | El tablero muestra ventas en $0 | La **puerta de calidad** bloquea la publicación (**WAP**) |
| Un despliegue corrompe una tabla | Se restaura un backup (si hay) | **Rollback** al snapshot anterior en segundos |
| Hay que encadenar pasos | Una persona corre scripts en orden | Un **orquestador** encadena por datos, reintenta y alerta |

El lab implementa cada fila de la columna derecha. Esta guía explica el porqué de cada una.

---

## 2. Los Nombres de 2026

Entre abril y octubre de 2026, Google renombró buena parte de su plataforma de datos. Los nombres de **APIs, comandos `gcloud`, librerías y roles de IAM no cambiaron**, así que en la documentación y en el código vas a ver ambos.

| Nombre anterior | Nombre actual | Fecha | Sigue apareciendo como |
|---|---|---|---|
| Cloud Composer | **Managed Service for Apache Airflow** | abr-2026 | `gcloud composer`, `roles/composer.*` |
| Dataproc (clusters) + Serverless for Apache Spark | **Managed Service for Apache Spark** (despliegue con clúster o serverless) | abr-2026 | `gcloud dataproc`, `DataprocCreateBatchOperator` |
| BigLake | **Lakehouse** (Lakehouse for Apache Iceberg) | 20-abr-2026 | `gcloud biglake`, `roles/biglake.*`, `biglake.googleapis.com` |
| BigLake metastore | **Lakehouse runtime catalog** | 20-abr-2026 | `bl://projects/…/catalogs/…` |
| Dataplex Universal Catalog | **Knowledge Catalog** | 10-abr-2026 | `gcloud dataplex`, `DataplexRunDataQualityScanOperator` |

Cambios funcionales relevantes para este módulo:

- **Airflow 3 es GA** en la generación 3 del servicio de Airflow (abril de 2026). La imagen **por defecto sigue siendo Airflow 2.11**, así que Airflow 3 hay que pedirlo explícitamente. La migración de 2 a 3 es lado a lado (un entorno nuevo), no se hace en el mismo entorno.
- **Modo desarrollo e hibernación** de entornos de Airflow son GA desde el **13-oct-2026**. La hibernación reduce casi a cero el costo de un entorno que no se usa.
- **Datastream escribe tablas Apache Iceberg** desde junio de 2025 (destino BigQuery, solo modo *append-only*).
- **Iceberg V2 es GA** en el Lakehouse runtime catalog. **V3 está en Preview**, y V1 no se soporta.

---

## 3. Arquitectura de Referencia

```
                    ┌──────────────── PLANO DE CONTROL ────────────────┐
                    │ Managed Service for Apache Airflow (Airflow 3)   │
                    │ shop_silver ──Asset──► shop_gold    maintenance  │
                    └───────┬────────────────────┬──────────────┬──────┘
                            │ batch              │ batch + scan │ batch
 ┌──────────────┐  CDC   ┌──▼────────────────┐   │              │
 │ PostgreSQL   ├───────►│ BRONZE            │   │              │
 │ (Cloud SQL)  │Datastr.│ Iceberg · catálogo│   │              │
 └──────────────┘ append │ BigQuery          │   │              │
                   only  └──┬────────────────┘   │              │
                            │ Spark serverless   │              │
                         ┌──▼────────────────────▼──────────────▼──┐
                         │ SILVER · Iceberg · Lakehouse runtime     │
                         │ catalog (REST) · actual + historia +     │
                         │ cuarentena + ops (watermarks, runs)      │
                         └──┬───────────────────────────────────────┘
                            │ Spark serverless
                         ┌──▼──────────────┐   ┌──────────────────┐   ┌──────────┐
                         │ GOLD_STAGING    ├──►│ Knowledge Catalog├──►│ GOLD     │──► BI
                         │ BigQuery nativo │   │ (auditoría)      │   │ (publish)│
                         └─────────────────┘   └──────────────────┘   └──────────┘
```

| Capa | Formato | Catálogo | Escribe | Contrato con el consumidor |
|---|---|---|---|---|
| Bronze | Iceberg (Parquet) | BigQuery | Datastream | "Todo lo que pasó en el origen, sin tocar" |
| Silver | Iceberg (Parquet) | Lakehouse runtime catalog (REST) | Spark | "El estado actual, limpio, una fila por entidad" |
| Gold staging | BigQuery nativo | BigQuery | Spark | Ninguno: es interno |
| Gold | BigQuery nativo | BigQuery | Copy job de BigQuery | "Validado. Esto es lo que ve negocio." |

¿Por qué Gold es **BigQuery nativo** y no Iceberg? Porque Gold es la capa de consumo: la consultan herramientas de BI cientos de veces al día y necesita clustering, caché, BI Engine y seguridad por fila y por columna. Las tablas del Lakehouse runtime catalog todavía no soportan clustering ni seguridad por fila o columna. Silver sí es Iceberg, porque es la capa que se **comparte entre motores** (Spark hoy, Trino o Flink mañana) y donde importan el time travel y la evolución de esquema.

---

## 4. Bronze: el Registro Inmutable

### 4.1. Datastream directo a Iceberg vs. hacia archivos Avro

Datastream tiene dos caminos para alimentar un lakehouse:

| | Destino Cloud Storage (Módulo 05) | Destino BigQuery → "Apache Iceberg tables" (este módulo) |
|---|---|---|
| Qué escribe | Archivos **Avro** o JSON en `esquema_tabla/aaaa/mm/dd/hh/mm/` | Una **tabla Iceberg** por tabla de origen, con datos en **Parquet** en tu bucket |
| Modo de escritura | Siempre eventos | Solo **append-only** |
| Paso extra para tener una tabla | Un job (Dataflow/Spark) que lea Avro y escriba Iceberg | Ninguno |
| Metadatos CDC | Anidados en cada evento (`source_metadata`) | Columna `datastream_metadata` (struct) |
| Quién puede escribir la tabla después | Quien tú decidas | Solo BigQuery |

En una empresa, el destino Iceberg elimina un job entero y su mantenimiento. El destino Cloud Storage sigue teniendo sentido cuando necesitas los archivos crudos para otro sistema, o cuando el lakehouse no está en Google Cloud.

**¿Por qué append-only en Bronze?** Porque Bronze es el **seguro de todo el sistema**. Si cada evento queda guardado, cualquier error de Silver o de Gold se corrige reprocesando. Además, la historia completa permite construir un SCD2 o auditar quién cambió qué y cuándo. Un Bronze en modo *merge* (una fila por clave) es más cómodo de consultar, pero pierde esa capacidad para siempre.

### 4.2. Dónde está el Avro

Es común pensar que "un lakehouse con Datastream guarda Avro". Depende de la capa que mires:

```
TABLA ICEBERG (cualquiera, Bronze o Silver)

  metadata/v3.metadata.json      ← JSON: esquema, particiones, lista de snapshots
  metadata/snap-…-1-….avro       ← AVRO: manifest list (qué manifests tiene cada snapshot)
  metadata/…-m0.avro             ← AVRO: manifest (qué archivos de datos y sus estadísticas)
  data/…-00001.parquet           ← PARQUET: los datos, columnares
```

En Iceberg, **los datos son Parquet y los metadatos son Avro**. Avro se usa en los manifests porque se escriben fila a fila (un registro por archivo de datos) y se leen completos, que es justo el caso de uso de un formato orientado a filas (ver [Módulo 03](../03-formatos-y-lakehouse/teoria.md)).

En las tablas Iceberg **administradas por BigQuery** (Bronze), BigQuery guarda los metadatos en su propio almacenamiento y los escribe en GCS cuando se lo pides con `EXPORT TABLE METADATA`. En las tablas del **Lakehouse runtime catalog** (Silver), cada commit de Spark escribe los metadatos directamente.

### 4.3. Anatomía de `datastream_metadata` y el problema de los borrados

| Campo | Tipo | Para qué sirve en Silver |
|---|---|---|
| `uuid` | STRING | Identificador único del evento (útil para deduplicar reentregas) |
| `source_timestamp` | INTEGER (ms epoch) | **Cuándo ocurrió el cambio en el origen**. Es la base del orden y del watermark. |
| `change_sequence_number` | STRING | Desempata eventos del mismo milisegundo |
| `change_type` | STRING | `INSERT`, `UPDATE-INSERT`, `UPDATE-DELETE`, `DELETE` |
| `sort_keys` | ARRAY<STRING> | Claves de orden que Datastream recomienda para ordenar eventos |

Dos detalles que separan un pipeline que funciona de uno que corrompe datos en silencio:

1. **Un `DELETE` de PostgreSQL solo trae la clave primaria.** Con la *replica identity* por defecto, el resto de las columnas llega en `NULL`. Si Silver aplicara ese evento con un `UPDATE SET *`, borraría los datos de la fila en vez de marcarla como eliminada. Por eso el `MERGE` del lab tiene una rama aparte para los borrados, y por eso las reglas de calidad no se aplican a los `DELETE`. Si necesitas la imagen completa de la fila borrada, la opción es `ALTER TABLE … REPLICA IDENTITY FULL` en el origen (más WAL y más tráfico).
2. **`UPDATE-DELETE`** aparece cuando un `UPDATE` cambia la clave primaria: la clave vieja "se borra" y la nueva "se inserta". Por eso Silver lo trata igual que un `DELETE`.

---

## 5. Catálogos Iceberg en Google Cloud

Una tabla Iceberg es un conjunto de archivos más un **puntero al metadato vigente**. El catálogo es quien guarda ese puntero y garantiza que dos escritores no lo pisen a la vez. Por eso, **elegir el catálogo es elegir quién puede leer y escribir la tabla.**

| Tipo de tabla Iceberg en GCP | Catálogo | Escribe | Lee | Estado (oct-2026) | Cuándo usarla |
|---|---|---|---|---|---|
| **Iceberg administrada por BigQuery** | BigQuery | BigQuery (DML, carga, Storage Write API; también Datastream) | BigQuery; Spark vía conector de BigQuery | GA | El dato lo produce y lo consume sobre todo BigQuery, o lo escribe Datastream |
| **Iceberg en el Lakehouse runtime catalog** (endpoint REST) | Lakehouse runtime catalog | Spark, Flink, Trino… (GA); BigQuery DML (Preview) | Cualquier motor Iceberg + BigQuery (`proyecto.catálogo.namespace.tabla`) | V2 GA, V3 Preview | **Lakehouse abierto**: varios motores sobre los mismos datos. Es la opción recomendada por Google. |
| Iceberg externa en BigQuery | El que tengas (Glue, Hive…) | Otro motor | BigQuery (solo lectura) | GA | Leer desde BigQuery un lakehouse que vive en otro catálogo |
| Custom Iceberg catalog for BigQuery | BigQuery metastore | Spark | Spark, BigQuery | Para usuarios existentes | Mantener tablas que ya creaste así; para tablas nuevas, el endpoint REST |

**Por qué el lab usa dos catálogos.** Datastream solo sabe escribir Iceberg administrado por BigQuery, así que Bronze no tiene alternativa. Silver usa el catálogo REST porque es la capa que debe quedar **abierta a cualquier motor**. Spark puede leer Bronze (con el conector de BigQuery), pero no escribirlo. Eso no es un problema: en Bronze el único escritor legítimo es Datastream.

**Credenciales: `end-user` vs. *credential vending*.** Con `end-user`, cada motor accede a GCS con su propia identidad, así que la cuenta de Spark necesita permisos sobre el bucket. Con *credential vending*, el catálogo entrega al motor credenciales temporales limitadas a los archivos de la tabla: el motor no necesita permisos sobre el bucket y el control de acceso queda centralizado en el catálogo. Es la opción más segura cuando hay muchos motores o muchos equipos. El lab usa `end-user` por simplicidad.

**Limitaciones a conocer** del Lakehouse runtime catalog: no permite renombrar tablas, no tiene vistas de Iceberg ni de metastore, y las tablas no se pueden clusterizar ni tener seguridad por fila o columna. Por eso, las reglas de acceso finas viven en Gold (BigQuery).

---

## 6. Silver: de Eventos a Estado

Silver convierte una **secuencia de eventos** en **estado actual**, y lo hace de forma incremental y repetible.

### 6.1. Carga incremental: watermark + lookback

Releer todo Bronze en cada corrida funciona con 1.000 filas y es inviable con 1.000 millones. La técnica estándar es el **watermark**: guardar el último `source_timestamp` procesado y leer solo lo posterior.

El problema es que **los eventos no llegan en orden**: un cambio ocurrido a las 10:00 puede llegar a Bronze a las 10:07, cuando el watermark ya avanzó a 10:05. Si filtras con `> watermark`, ese evento se pierde para siempre. La solución es un **lookback**: leer desde `watermark - margen`.

```
eventos en Bronze ──────────────────────────────────────────────► tiempo de origen
                      │◄─ lookback (30 min) ─►│
                   desde = wm - 30m          wm (última corrida)
                      └──── se relee ──────────┴──── nuevo ────►
```

Releer el margen es seguro solo si el paso siguiente es **idempotente** (6.3). Elegir el tamaño del margen es una decisión empírica: tiene que superar la latencia máxima de Datastream que hayas observado, con holgura.

El watermark se guarda **al final** de la corrida, después del `MERGE`. Si el job falla a la mitad, el watermark no avanza y la próxima corrida reprocesa. Nunca hay huecos.

### 6.2. Deduplicar: el último evento por clave

Dentro del lote leído, una misma clave puede tener varios eventos (`INSERT`, luego dos `UPDATE`). Se queda el último según `source_timestamp` y, para desempatar, `change_sequence_number`. Fíjate que **no se usa la hora en que el evento llegó a Bronze**: esa refleja la latencia de la replicación, no el orden de los hechos.

### 6.3. `MERGE` idempotente y borrados lógicos

```sql
MERGE INTO silver.orders t USING lote s ON t.order_id = s.order_id
WHEN MATCHED AND s._is_deleted AND s._source_ts >= t._source_ts THEN UPDATE SET t._is_deleted = true, …
WHEN MATCHED AND NOT s._is_deleted AND s._source_ts >= t._source_ts THEN UPDATE SET *
WHEN NOT MATCHED AND NOT s._is_deleted THEN INSERT *
```

- **`s._source_ts >= t._source_ts`** impide que un evento viejo, releído por el lookback o llegado tarde, pise uno más nuevo. Es lo que vuelve seguro reintentar.
- **`>=` en vez de `>`** es deliberado: permite reprocesar a propósito para aplicar una regla de limpieza nueva sobre datos ya cargados. El costo es reescribir filas que no cambiaron.
- **Borrado lógico (`_is_deleted`)** en vez de `DELETE`. Gold filtra las filas borradas, pero la fila sigue existiendo: se puede auditar y un hecho histórico no pierde su dimensión.
- **`WHEN NOT MATCHED AND NOT s._is_deleted`**: si lo primero que se ve de una clave es su borrado, no se inserta.

**Copy-on-write vs. merge-on-read.** Por defecto, Iceberg ejecuta el `MERGE` en modo *copy-on-write*: reescribe los archivos que contienen filas modificadas. Las lecturas son rápidas, pero las escrituras son más caras. En modo *merge-on-read* escribe *delete files* pequeños y los combina al leer: las escrituras son baratas y las lecturas más lentas hasta la próxima compactación. En tablas con muchos cambios pequeños y frecuentes (CDC cada pocos minutos), *merge-on-read* + compactación periódica suele ser la mejor opción. El lab usa el valor por defecto.

### 6.4. Limpieza vs. enriquecimiento

| Limpieza (corrige lo que vino) | Enriquecimiento (agrega lo que no vino) |
|---|---|
| `trim`, mayúsculas y minúsculas, tipos | Dominio del email y si es gratuito |
| Ciudades canónicas (`bogota` → `Bogotá`) | Región comercial a partir del país |
| Teléfonos solo con dígitos | Banda de precio del producto |
| Zona horaria única (UTC) | Horas en tránsito, importe de línea |

Una regla práctica: el **enriquecimiento por fila** (derivable de la propia fila) va en Silver. El **enriquecimiento con datos de referencia que pueden corregirse** (tasas de cambio, mapeos que mantiene un área de negocio) conviene unirlo **lo más tarde posible**, en Gold. Así, cuando alguien corrige la referencia, la corrección se aplica en la siguiente corrida sin reprocesar Silver. El incidente del lab demuestra exactamente esto.

### 6.5. Cuarentena

Una fila que no cumple una regla **no detiene el pipeline**: se guarda en `silver.quarantine` con la carga original (`payload` en JSON) y la lista de reglas que falló. Tres razones:

1. **Disponibilidad:** un email mal escrito no debe atrasar el tablero de ventas de toda la empresa.
2. **Evidencia:** el equipo dueño del origen recibe *qué* fila, *qué* regla y *cuándo*, no un "el pipeline falló".
3. **Recuperación:** cuando el origen corrige el dato, el evento corregido entra por CDC y reemplaza el estado anterior. Lo que quedó en cuarentena es el registro histórico del problema.

La cuarentena se escribe con un `MERGE` (no con un `append`) para que el lookback no duplique filas. **También tiene que ser idempotente.**

### 6.6. Estado actual + historia

Para la mayoría de las tablas, Silver guarda solo el estado actual. Para las entidades cuyo cambio importa al negocio (aquí, `customers`), guarda además una tabla `_history` con **cada versión limpia**. Así, el SCD2 de Gold se puede reconstruir completo en cada corrida, sin depender de haber "visto" cada cambio en el momento justo.

---

## 7. Gold: Modelado Dimensional que Aguanta el Mundo Real

### 7.1. Los tres tipos de tabla de hechos

Kimball define tres tipos de tabla de hechos, y un datamart real suele necesitar los tres:

| Tipo | Grano | Se actualiza | Pregunta que responde | En el lab |
|---|---|---|---|---|
| **Transaccional** | Un evento (una línea de pedido) | Nunca: solo se agregan filas | "¿Cuánto vendimos por categoría?" | `fact_order_lines` |
| **Accumulating snapshot** | Un proceso con etapas (un pedido) | Sí: cada vez que avanza de etapa | "¿Cuánto tarda un pedido en llegar? ¿Dónde se atasca?" | `fact_order_fulfillment` |
| **Periodic snapshot** | Entidad × período (producto × día) | No: una foto nueva por período | "¿Cómo evolucionó el inventario?" | `fact_inventory_daily` |

El *accumulating snapshot* es el que mejor aprovecha el CDC: cada `UPDATE` de estado del pedido, cada pago y cada envío llena una fecha más de la misma fila. El *periodic snapshot* es **idempotente por fecha**: re-correr el mismo día reemplaza la foto de ese día. Una advertencia: no se puede reconstruir la foto de un día pasado desde Silver, porque Silver solo tiene el estado actual. Si se necesitara, habría que reconstruirla desde Bronze, que tiene cada evento. Es otro argumento a favor de un Bronze append-only.

### 7.2. SCD Tipo 1 y Tipo 2 en la misma dimensión

No todos los atributos de una dimensión merecen historia. En `dim_customer`:

- **`segment` es SCD2.** Si un cliente pasa de `NEW` a `VIP`, sus compras anteriores deben seguir atribuyéndose a `NEW`. Si no, las ventas de "VIP" se inflan retroactivamente.
- **`email`, `full_name` y la ciudad son SCD1.** Se sobrescriben: nadie analiza ventas "por el email que tenía el cliente en ese momento".

Esta combinación (a veces llamada **SCD tipo 6** o híbrida) es la norma en la práctica. Decidir qué atributo es tipo 1 o tipo 2 es una conversación con negocio, no una decisión técnica.

Detalles de implementación que evitan errores clásicos:

- **La primera versión arranca en `1900-01-01`**, no en el primer evento visto. El backfill de Datastream trae el estado actual con la fecha del backfill, y los pedidos anteriores a ese momento no encontrarían ninguna versión vigente.
- **`valid_to` exclusivo** (`order_ts >= valid_from AND order_ts < valid_to`), para que un instante pertenezca a exactamente una versión.
- **Se colapsan los eventos que no cambian el atributo rastreado**: si un cliente cambia de email cinco veces, no se abren cinco versiones.

### 7.3. Join point-in-time

Para asignar a cada hecho la versión correcta de la dimensión, el join no es por `customer_id` sino por `customer_id` **y** la fecha del hecho dentro de la vigencia de la versión. Si las vigencias se solapan, cada pedido se cuenta dos veces: por eso la conciliación (11.1) y la regla de calidad `una-version-vigente` (9.2) existen.

### 7.4. Claves sustitutas deterministas

Si Gold se reconstruye en cada corrida, una clave generada con `ROW_NUMBER()` o una secuencia **cambia de valor entre corridas**. Cualquier extracto, caché o tablero que la haya guardado queda apuntando a otra fila. Con un **hash** de la clave natural y el inicio de vigencia (`xxhash64(customer_id, valid_from)`), la misma versión obtiene siempre la misma clave. El costo es una probabilidad de colisión mínima, aceptable en dimensiones de millones de filas con un hash de 64 bits.

### 7.5. Miembro desconocido y dimensiones que llegan tarde

Datastream replica las tablas en paralelo, así que un pedido puede llegar a Silver antes que su cliente. También puede pasar que el cliente esté en cuarentena. Si el join fuera `INNER`, ese pedido desaparecería de las ventas. La solución de Kimball es una fila **"Desconocido" con clave `-1`**: el hecho se conserva y se asigna a `-1`. Cuando el cliente llega, la siguiente reconstrucción de Gold lo reasigna. La regla de calidad `clientes-desconocidos-max-5pct` vigila que esto sea la excepción y no la norma.

---

## 8. Orquestación con Airflow 3

### 8.1. Principios de diseño de DAGs

| Principio | Cómo se aplica en el lab |
|---|---|
| **El DAG orquesta, no transforma** | Cero lógica de datos en Airflow. Los jobs de Spark se prueban solos. |
| **Tareas idempotentes** | Re-ejecutar cualquier tarea deja el mismo resultado (MERGE, overwrite, copy con `WRITE_TRUNCATE`). |
| **Tareas atómicas** | Una tarea = una unidad que se puede reintentar: un batch, un escaneo, una copia. |
| **Configuración fuera del código** | Proyecto, bucket y cuentas de servicio vienen de variables de entorno del entorno de Airflow. |
| **Reintentos donde ayudan, no donde estorban** | Los batches reintentan (un fallo de infraestructura puede ser transitorio). La auditoría de calidad **no** (un dato malo no se arregla reintentando). |
| **Un fallo se ve** | `on_failure_callback` deja una línea `PIPELINE_ALERT` que Cloud Monitoring convierte en alerta. |

### 8.2. Data-aware scheduling con Assets

Con horarios fijos, si Silver corre a las :00 y Gold a las :10, el día que Silver tarde 12 minutos Gold procesa datos viejos (o a medio escribir). Con **Assets** (en Airflow 2 se llamaban *Datasets*), la tarea de Silver declara `outlets=[SILVER_READY]` y el DAG de Gold declara `schedule=[SILVER_READY]`: Gold corre **cuando hay datos nuevos**, no a una hora. Es la diferencia entre encadenar por tiempo y encadenar por datos.

En Airflow 3, una ejecución disparada por un Asset **no tiene `logical_date`**. Por eso el lab calcula la fecha del snapshot con `dag_run.logical_date or dag_run.run_after`.

### 8.3. Dynamic task mapping

`.partial(…).expand(data_scan_id=[…])` crea una tarea por elemento de la lista **en tiempo de ejecución**. Cada una se reintenta, se ve y falla de forma independiente, y agregar una tabla a publicar es agregar un elemento a una lista. El trade-off aparece cuando cada tarea es cara de arrancar. Un batch serverless de Spark tarda ~1 minuto en arrancar y usa 12 vCPU, por eso el lab usa un batch por capa y no uno por tabla (ver Reto 3 del lab).

### 8.4. Backfill en un pipeline CDC

En un pipeline particionado por fecha, *backfill* significa "re-correr los días X a Y". En un pipeline CDC incremental, el equivalente es **ampliar el lookback**: el parámetro `lookback_minutes` del DAG de Silver permite reprocesar la última hora, día o semana con el mismo código, gracias al `MERGE` idempotente.

### 8.5. ¿Por qué Airflow y no otra cosa?

| Orquestador | Fortaleza | Cuándo elegirlo |
|---|---|---|
| **Managed Service for Apache Airflow** | Estándar de la industria, cientos de operadores, encadena cualquier servicio | Pipelines que cruzan varios servicios (como este) |
| Dataform / dbt (con su scheduler) | SQL declarativo con dependencias, tests y linaje integrados | Todo el pipeline es SQL dentro de BigQuery (Módulo 01) |
| Workflows | Serverless y barato, ideal para encadenar APIs | Flujos simples, sin necesidad de UI de operación ni reintentos ricos |
| Pipelines de Kubeflow / Vertex | Pensado para ML, con linaje de artefactos | Entrenamiento y despliegue de modelos (Módulo 07) |

### 8.6. Modo desarrollo e hibernación

Un entorno de Airflow es infraestructura **siempre encendida**, y por eso suele ser el costo fijo más alto de un pipeline pequeño. El **modo desarrollo** (un solo nodo, preset Small, solo Airflow 3) y la **hibernación** (pausar el entorno y reanudarlo después) existen para los entornos de desarrollo, prueba y aprendizaje. No sirven para producción: un entorno hibernado no programa ni ejecuta DAGs.

---

## 9. Calidad de Datos en Dos Niveles y Write-Audit-Publish

### 9.1. Dos preguntas distintas

| | Nivel fila (Silver) | Nivel tabla (Gold) |
|---|---|---|
| Pregunta | "¿Esta fila es válida?" | "¿Este conjunto de datos es publicable?" |
| Ejemplos | Email con formato válido, cantidad > 0, estado conocido | Sin duplicados, ≤ 5% de clientes desconocidos, una versión vigente por cliente, montos en USD completos |
| Si falla | La fila va a cuarentena, **el resto sigue** | **Se bloquea la publicación** |
| Herramienta en el lab | Reglas en PySpark (`TABLES[…]["rules"]`) | Escaneos de calidad de Knowledge Catalog |

Algunos errores solo existen a nivel tabla: una línea duplicada es válida fila por fila, y un monto `NULL` en USD puede ser legítimo en una fila y un desastre en el 4% de la tabla.

### 9.2. ¿Dataform, Spark o Knowledge Catalog?

| Herramienta | Dónde corre | Fortaleza | Limitación |
|---|---|---|---|
| **Assertions de Dataform** ([Módulo 01, Paso 4.5](../01-patrones-y-modelado/lab.md#45-capa-de-control-assertions)) | BigQuery (SQL) | Viven junto al modelo SQL y bloquean dependencias | Solo si el pipeline es Dataform/SQL |
| **Reglas en Spark** (PySpark puro, Great Expectations, Soda, PyDeequ) | Dentro del job | Validan **por fila** antes de escribir y desvían a cuarentena | La lógica queda dentro del código del job |
| **Knowledge Catalog (auto data quality)** | Servicio administrado | Independiente del motor, con historial, reglas en YAML, exporta resultados a BigQuery y se integra con el catálogo y el linaje. Soporta tablas de BigQuery **e Iceberg del catálogo REST** | Valida tablas completas o muestras, no desvía filas |

En una empresa, lo habitual es combinar validación por fila dentro del motor con una puerta a nivel tabla independiente del motor. Esto responde a la pregunta de si "se puede validar calidad con Spark como con Dataform": sí, y conviene complementarlo con una puerta externa.

### 9.3. Write-Audit-Publish (WAP)

```
WRITE   → el job escribe en un lugar que nadie consume (gold_staging)
AUDIT   → se valida ese lugar (Knowledge Catalog)
PUBLISH → solo si pasa, se mueve a donde sí se consume (gold)
```

La propiedad clave: **si la auditoría falla, lo publicado no cambia.** Los consumidores siguen viendo la última versión buena, levemente desactualizada pero correcta, en vez de una versión fresca e incorrecta. Para BI casi siempre es la elección correcta.

**Variantes de WAP:**

| Variante | Cómo publica | Atomicidad multi-tabla |
|---|---|---|
| Staging + copia (este lab) | Copy job de BigQuery por tabla | No: cada tabla se publica por separado |
| Ramas de Iceberg | `fast_forward` de `main` a la rama validada | No: las ramas son por tabla |
| Catálogos con versionado (Nessie, lakeFS) | *Merge* de una rama de todo el catálogo | Sí |

Si la publicación falla a mitad de camino (tres tablas copiadas y tres no), Gold queda inconsistente hasta la siguiente corrida. Las mitigaciones habituales son publicar primero los hechos y después las dimensiones (o al revés, según qué tolere el consumidor), exponer Gold a través de vistas que se cambian juntas, o aceptar la ventana y documentarla.

---

## 10. Mantenimiento de Tablas Iceberg

Cada commit crea un snapshot nuevo y, con `MERGE` frecuentes, archivos pequeños. Sin mantenimiento, una tabla Iceberg que recibe CDC cada 30 minutos acumula miles de archivos y snapshots en semanas.

| Operación | Procedimiento de Spark | Qué resuelve | Qué cuesta |
|---|---|---|---|
| **Compactación** | `rewrite_data_files` | Muchos archivos pequeños → pocos grandes: menos aperturas de archivo y lecturas más rápidas | Cómputo de reescritura |
| **Reescritura de manifests** | `rewrite_manifests` | Menos metadatos que leer al planificar cada consulta | Poco |
| **Expirar snapshots** | `expire_snapshots` | Borra los snapshots viejos y los archivos que ya nadie referencia; libera almacenamiento | **Acorta la ventana de time travel** |
| **Archivos huérfanos** | `remove_orphan_files` | Archivos que dejó un job caído y que ningún snapshot referencia | Listado del bucket |

**La retención de snapshots es una decisión de negocio.** Con 72 horas, un error detectado el lunes por un despliegue del viernes todavía se revierte con un rollback. Con 7 días, aguanta un fin de semana largo, a costa de más almacenamiento. Hay que fijarla junto con quien define los tiempos de recuperación (RPO y RTO).

**Rollback** (`rollback_to_snapshot` / `rollback_to_timestamp`) cambia a qué snapshot apunta la tabla, sin copiar datos. Es por tabla: un error que afecta a varias tablas exige revertir cada una al mismo instante.

**Gestión automática.** El Lakehouse runtime catalog ofrece *table management* automático (compactación y recolección de basura) en **Preview**. Cuando sea GA, buena parte del DAG de mantenimiento podrá delegarse al servicio. Hasta entonces, el DAG explícito es lo prudente.

---

## 11. Operación: Observabilidad, Seguridad y Lab vs. Producción

### 11.1. Observabilidad mínima de un pipeline de datos

| Señal | En el lab | Pregunta que responde |
|---|---|---|
| **Volumen** | `ops.pipeline_runs` (leídas, válidas, en cuarentena, claves) | "¿Procesamos lo de siempre o algo raro pasó?" |
| **Conciliación** | `reconcile()` en Gold | "¿El modelo perdió o duplicó filas?" |
| **Calidad** | `quality.dq_results` | "¿Qué reglas fallaron, cuándo y cuánto?" |
| **Frescura** | `MAX(order_ts)` vs. ahora | "¿Qué tan atrasado está lo que ve negocio?" |
| **Fallos** | `PIPELINE_ALERT` en Cloud Logging | "¿Alguien se enteró?" |

La frescura es la métrica que más importa a negocio y la que más se olvida. Un pipeline que "no falla" pero lleva 6 horas sin datos nuevos (porque el stream de Datastream se pausó) es un incidente silencioso.

### 11.2. Seguridad

- **Una identidad por componente**: Spark, Airflow, la conexión de BigQuery y el agente de Knowledge Catalog tienen cada uno solo lo que necesitan.
- **Secretos en Secret Manager**, nunca en variables de entorno ni en el código.
- **Conectividad privada** entre Datastream y la base de datos (Private Service Connect), sin IP pública.
- **Seguridad por columna en Gold** (*policy tags* y enmascaramiento, [Módulo 06](../06-knowledge-catalog-mesh-fabric/teoria.md)) sobre el email y el teléfono.

### 11.3. Simplificaciones del lab vs. producción

| Aspecto | En el lab | En producción |
|---|---|---|
| Red de Cloud SQL | IP pública + `0.0.0.0/0` | Private Service Connect, sin IP pública |
| Contraseñas | En texto plano | Secret Manager |
| Roles de IAM | A nivel proyecto | A nivel bucket y dataset |
| Entorno de Airflow | Modo desarrollo, 1 nodo | Preset Medium/Large, alta resiliencia, entornos separados de dev y prod |
| Despliegue de DAGs y jobs | `gcloud … import` a mano | CI/CD desde git con tests (Spark local + Iceberg `hadoop`) |
| Gold | Reconstrucción completa | Incremental por partición cuando el volumen lo exige |
| Credenciales de Iceberg | `end-user` | *Credential vending* |
| Alertas | Línea en Cloud Logging | Alerta basada en logs → PagerDuty, Slack o correo |
| Retención de snapshots | 72 h | Definida con negocio según el RPO |

---

## 12. Costos

Los costos de este pipeline se dividen en **fijos** (existen aunque no proceses nada) y **variables** (escalan con el volumen):

| Componente | Tipo | Cómo se cobra | Palanca de ahorro |
|---|---|---|---|
| Entorno de Airflow | **Fijo** | DCU-hora mientras existe | Modo desarrollo + hibernación fuera de horario (solo dev) |
| Cloud SQL | **Fijo** | Hora de instancia | Tamaño adecuado; es el origen, no se apaga en producción |
| Datastream | Variable | GB procesados | Replicar solo las tablas y columnas necesarias |
| Spark serverless | Variable | DCU-hora + shuffle | Un batch por capa (no por tabla); tamaño mínimo de executors; *merge-on-read* |
| Knowledge Catalog (calidad) | Variable | DCU por escaneo | Muestreo (`samplingPercent`) y filtros (`rowFilter`) en tablas grandes |
| BigQuery | Variable | Lecturas de Storage Read API, consultas, almacenamiento | Gold particionado y clusterizado; las copias (*publish*) son gratis |
| Cloud Storage | Variable | GB-mes | Compactación + expiración de snapshots |

En un pipeline pequeño, **los costos fijos dominan**: el entorno de Airflow puede costar más que todo el procesamiento. Por eso, para pocos pipelines simples, a veces se elige Workflows o el scheduler de Dataform en lugar de Airflow. Airflow empieza a justificarse cuando hay decenas de pipelines o dependencias entre servicios.

---

## 13. Referencias Técnicas

- [Datastream — Apache Iceberg tables destination](https://docs.cloud.google.com/datastream/docs/destination-blmt)
- [Datastream — BigQuery destination](https://docs.cloud.google.com/datastream/docs/destination-bigquery)
- [Datastream — Release notes](https://docs.cloud.google.com/datastream/docs/release-notes)
- [Lakehouse — About the Lakehouse runtime catalog](https://docs.cloud.google.com/lakehouse/docs/about-lakehouse-catalogs)
- [Lakehouse — About the Apache Iceberg REST catalog endpoint](https://docs.cloud.google.com/lakehouse/docs/about-iceberg-rest-catalog-endpoint)
- [Lakehouse — Compare table types](https://docs.cloud.google.com/lakehouse/docs/lakehouse-tables)
- [Lakehouse — Use the Lakehouse runtime catalog with tables in BigQuery](https://docs.cloud.google.com/lakehouse/docs/lakehouse-catalog-tables)
- [Managed Service for Apache Spark (antes Dataproc)](https://cloud.google.com/products/managed-service-for-apache-spark)
- [Managed Service for Apache Spark — Runtime 3.0](https://docs.cloud.google.com/managed-spark/docs/concepts/versions/spark-runtime-3.0)
- [Managed Service for Apache Airflow — Release notes](https://docs.cloud.google.com/composer/docs/release-notes)
- [Managed Service for Apache Airflow — Hibernate development mode environments](https://docs.cloud.google.com/composer/docs/composer-3/hibernate-environments)
- [Knowledge Catalog — Auto data quality overview](https://docs.cloud.google.com/knowledge-catalog/docs/auto-data-quality-overview)
- [Apache Airflow — Assets y data-aware scheduling](https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/asset-scheduling.html)
- [Apache Iceberg — Spark procedures (mantenimiento y rollback)](https://iceberg.apache.org/docs/latest/spark-procedures/)
- [Apache Iceberg — Branching and tagging](https://iceberg.apache.org/docs/latest/branching/)
- Kimball, R. & Ross, M. — *The Data Warehouse Toolkit*, 3.ª ed. (cap. 4: tipos de tablas de hechos; cap. 5: técnicas de SCD)
