# analytics-architectures-labs

Laboratorios prácticos de **arquitecturas de analítica de datos en Google Cloud**.

Cada lab es un *codelab* autónomo en un único archivo Markdown: se lee de arriba abajo y
se van ejecutando los bloques de código (`bash`, `sql`) en tu propio proyecto de GCP.
El objetivo no es solo “que funcione”, sino entender **por qué** se diseña así y **medir**
el resultado (costo, latencia, bytes escaneados).

---

## Contenido

| Lab | Tema | Servicios GCP | Qué construyes |
|---|---|---|---|
| **[Lab 01](lab01-Kimball.md)** | Modelado dimensional (Kimball) + arquitectura Medallion con Dataform | Cloud SQL (PostgreSQL), Firestore, Datastream, BigQuery, Dataform, Looker Studio | Pipeline CDC en tiempo real desde fuentes transaccionales hacia BigQuery, y transformación por capas Bronze → Silver → Gold (staging con deduplicación CDC, dimensiones SCD Tipo 2, tablas de hechos, data marts) |
| **[Lab 02](lab02-BigQuery.md)** | Particionamiento, clustering y optimización de SQL en BigQuery | BigQuery (+ datasets públicos) | Tres versiones de la misma tabla (sin optimizar / particionada / particionada+clusterizada) a partir de datos públicos de taxis de NYC, y una batería de consultas para medir costo y rendimiento **antes y después** de aplicar buenas prácticas |
| **[Lab 03](lab03-Formatos.md)** | Formatos de archivo (Parquet, Avro) y formatos de tabla abiertos (Delta Lake, Iceberg, Hudi) | BigQuery, Cloud Storage, BigLake connections | Exportar datos a CSV/Avro/Parquet y comparar tamaño y lectura selectiva; inspeccionar esquemas y estadísticas por dentro; crear una tabla **Iceberg gestionada en BigQuery** con DML, time travel y `EXPORT TABLE METADATA`; escribir **Delta** con `delta-rs` (sin Spark) y leerla desde BigQuery; **Hudi** conceptual; árbol de decisión de cuándo usar cada formato. Comandos verificados end-to-end |

---

## Estructura del repositorio

```
analytics-architectures-labs/
├── README.md            ← este archivo
├── lab01-Kimball.md     ← Lab 01: CDC + Medallion + Dataform
├── lab02-BigQuery.md    ← Lab 02: particionamiento, clustering y tuning de SQL
└── lab03-Formatos.md    ← Lab 03: Parquet/Avro y formatos de tabla abiertos (Delta, Iceberg, Hudi)
```

No hay código fuente ni build: los labs son documentos. Todo el código vive en bloques
dentro de cada `.md` y se ejecuta en la consola web de GCP, en Cloud Shell o con las CLIs
locales.

---

## Prerrequisitos generales

- Una **cuenta de Google Cloud con facturación habilitada** y un proyecto propio.
- **`gcloud` CLI** instalada y autenticada (`gcloud auth login`, `gcloud config set project TU_PROYECTO`),
  o acceso a **Cloud Shell**.
- Permisos de administración/edición sobre los servicios de cada lab (ver la sección
  *Prerrequisitos* dentro de cada archivo).
- Conocimientos básicos de SQL y de modelado de datos.

Requisitos específicos (APIs a habilitar, roles IAM, herramientas adicionales como
`bq` o el conector de Dataform) están indicados **al inicio de cada lab**.

---

## Cómo usar los labs

1. Abre el archivo del lab (`lab0X-*.md`).
2. Lee la sección de **objetivos, prerrequisitos y costos**.
3. Reemplaza los marcadores tipo `TU_PROYECTO` por tus valores reales.
4. Ejecuta los bloques en orden. Los bloques ```sql``` van en el editor de BigQuery
   (o Dataform); los ```bash``` en Cloud Shell / terminal.
5. Compara tus resultados con lo que el lab dice que deberías observar.
6. **Ejecuta el paso de limpieza final** para no dejar recursos facturando.

Los bloques marcados con **`📸`** en el texto son marcadores de captura de pantalla:
puedes ignorarlos o reemplazarlos con tus propias capturas si adaptas el material.

---

## Costos

Los labs están diseñados para caer dentro de la **capa gratuita** siempre que se ejecute
el paso de limpieza:

- **BigQuery consultas (on-demand):** primer 1 TiB/mes gratis; después ~$6.25/TiB (US).
- **BigQuery almacenamiento:** primeros 10 GiB/mes gratis; después ~$0.02/GiB.
- **Cloud SQL / Datastream (Lab 01):** usan instancias mínimas (`db-f1-micro`), pero
  **no son gratis mientras están encendidas** — apágalas o elimínalas al terminar.

Cada lab incluye una estimación de costo y un paso de *cleanup* al final. Revisa
**Billing → Reports** después de cada sesión.

---

## Convenciones

- **Región:** los Labs 02 y 03 usan la multi-región `US` (obligatorio para leer
  `bigquery-public-data`). El Lab 01 usa `us-central1`. No mezcles regiones entre dataset
  origen y destino en una misma operación.
- **Placeholders:** `TU_PROYECTO` (Lab 02) y valores como IPs, contraseñas y nombres de
  instancia en el Lab 01 son de ejemplo — **cámbialos** y no reutilices contraseñas
  reales en texto plano.
- **Idioma:** los labs están en español.
- **Formato:** un lab = un archivo Markdown; encabezados `##` para pasos principales,
  bloques de código con lenguaje explícito, diagramas en ASCII.

---

## Contribuir

Para añadir un lab nuevo:

1. Crea `lab0N-<Tema>.md` siguiendo la estructura de los existentes
   (objetivos → prerrequisitos → costos → pasos numerados → limpieza → retos → referencias).
2. Añádelo a la tabla **Contenido** y al árbol de **Estructura del repositorio** de este README.
3. Verifica que cada bloque de código es ejecutable tal cual (salvo placeholders) y que
   el lab incluye su propio paso de limpieza.
