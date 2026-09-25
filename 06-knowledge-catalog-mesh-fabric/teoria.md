# Data Mesh, Data Fabric y Knowledge Catalog: Gobierno de Datos Federado en GCP

> **Audiencia Objetivo:** Data Architects, Data Governance Leads, Analytics Engineers y Chief Data Officers que necesitan decidir **cómo escalar el gobierno de datos** cuando una sola tabla, un solo equipo central o un solo catálogo ya no alcanzan.
>
> **Alcance:** Este módulo es independiente de los Módulos 01-05 (no requiere haberlos completado). Cubre dos paradigmas arquitectónicos vendor-neutral — **Data Mesh** y **Data Fabric** — y cómo **Knowledge Catalog**, el catálogo de metadatos de Google Cloud (antes *Dataplex Universal Catalog*, antes *Data Catalog*), les da forma concreta.
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 06 — Gobierno Federado con Knowledge Catalog](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo: El Problema que Resuelven Mesh y Fabric](#1-resumen-ejecutivo-el-problema-que-resuelven-mesh-y-fabric)
2. [Data Mesh: Descentralización Organizacional](#2-data-mesh-descentralización-organizacional)
3. [Data Fabric: Automatización Tecnológica vía Metadata Activa](#3-data-fabric-automatización-tecnológica-vía-metadata-activa)
4. [Mesh vs. Fabric: No Son lo Mismo, No Son Excluyentes](#4-mesh-vs-fabric-no-son-lo-mismo-no-son-excluyentes)
5. [Knowledge Catalog: la Pieza Técnica de Google Cloud](#5-knowledge-catalog-la-pieza-técnica-de-google-cloud)
6. [De Conceptos a Recursos: Cómo Knowledge Catalog Implementa Mesh y Fabric](#6-de-conceptos-a-recursos-cómo-knowledge-catalog-implementa-mesh-y-fabric)
7. [Marcos de Gobernanza: Catálogo, Diccionario y Linaje](#7-marcos-de-gobernanza-catálogo-diccionario-y-linaje)
8. [Seguridad: Cifrado y Enmascaramiento de Datos](#8-seguridad-cifrado-y-enmascaramiento-de-datos)
9. [Matriz de Decisión](#9-matriz-de-decisión)
10. [Costos y Límites](#10-costos-y-límites)
11. [Referencias Técnicas](#11-referencias-técnicas)

---

## 1. Resumen Ejecutivo: El Problema que Resuelven Mesh y Fabric

Durante la última década, la respuesta por defecto a "¿cómo organizamos nuestros datos?" fue **centralizar**: un solo data warehouse, un solo equipo de datos, un solo pipeline ETL que todos los demás equipos esperan en la fila para usar. Este modelo funciona mientras la organización es pequeña. Cuando crecen los dominios de negocio (Ventas, Marketing, Logística, Finanzas...), el equipo central se convierte en un **cuello de botella**: cada nueva fuente de datos, cada nueva tabla, cada nueva regla de calidad pasa por el mismo grupo de 5-10 personas que ya no dan abasto.

Dos respuestas, complementarias pero distintas, emergieron para este problema:

```
DOS RESPUESTAS AL MISMO SÍNTOMA (EL EQUIPO CENTRAL COMO CUELLO DE BOTELLA)

DATA MESH                                    DATA FABRIC
"El problema es ORGANIZACIONAL"              "El problema es TECNOLÓGICO"
Redistribuye la PROPIEDAD del dato           Automatiza el DESCUBRIMIENTO e
a los dominios que lo generan.               INTEGRACIÓN vía metadata activa,
                                              sin importar quién es dueño de qué.
   ¿QUIÉN decide y es responsable?              ¿CÓMO se conecta todo
                                                  automáticamente?
```

Ninguno reemplaza al otro. De hecho, en la práctica **Data Fabric es la plataforma técnica que hace viable a Data Mesh**: sin un catálogo que descubra, etiquete y conecte automáticamente los datos de cada dominio, "cada dominio es dueño de su dato" degenera rápido en silos incomunicados.

---

## 2. Data Mesh: Descentralización Organizacional

**Data Mesh** (término acuñado por Zhamak Dehghani en 2019) es un **paradigma organizacional y arquitectónico**, no un producto que se compra. Se apoya en 4 principios:

```
LOS 4 PRINCIPIOS DE DATA MESH

1. PROPIEDAD DESCENTRALIZADA POR DOMINIO
   El equipo de Ventas es dueño de sus datos de ventas y los modela.
   El equipo de Marketing es dueño de los suyos. Nadie más los "posee".

2. DATOS COMO PRODUCTO ("Data as a Product")
   Cada dominio publica sus datos con los mismos estándares que un
   producto de software: descubrible, direccionable, con dueño,
   documentado, con SLA de calidad y frescura.

3. PLATAFORMA DE AUTOSERVICIO ("Self-Serve Data Platform")
   Un equipo de plataforma (no de datos) provee herramienta genérica
   -catalogación, control de acceso, pipelines- para que CADA dominio
   pueda publicar y consumir datos sin depender de un equipo central
   para cada tarea.

4. GOBIERNO COMPUTACIONAL FEDERADO
   Reglas globales (privacidad, seguridad, estándares de nombres,
   interoperabilidad) se codifican y se aplican automáticamente en
   toda la malla -no las revisa un comité central a mano.
```

### 2.1. Lo que Data Mesh NO es

- **No es "cada equipo con su propia base de datos sin reglas".** Eso es simplemente un silo — el fracaso que Data Mesh intenta evitar con el principio #4 (gobierno federado).
- **No es un producto de GCP, AWS o Databricks que se instala.** Es una decisión organizacional; las nubes ofrecen *herramientas* (como Knowledge Catalog) que la hacen operacionalmente posible.
- **No sustituye a un Data Warehouse o Lakehouse.** Un dominio puede seguir usando BigQuery, un Lakehouse Iceberg (ver [Módulo 03](../03-formatos-y-lakehouse/teoria.md)) o cualquier motor — Mesh define *quién* es dueño y *cómo se publica*, no *dónde* vive físicamente.

---

## 3. Data Fabric: Automatización Tecnológica vía Metadata Activa

**Data Fabric** es un patrón de **arquitectura técnica** (popularizado por Gartner) que despliega una capa de **metadata activa** — impulsada cada vez más por IA/ML — sobre un ecosistema de datos heterogéneo y distribuido (multi-nube, on-prem, distintos formatos). Su objetivo: **descubrir, clasificar, conectar y gobernar automáticamente** los datos sin que un humano tenga que mapear manualmente cada fuente.

```
CAPAS DE UN DATA FABRIC

┌──────────────────────────────────────────────────────────────┐
│ 4. CONSUMO: Búsqueda unificada, agentes de IA, self-service   │
├──────────────────────────────────────────────────────────────┤
│ 3. GOBIERNO: Políticas de acceso, linaje, calidad, glosario   │
├──────────────────────────────────────────────────────────────┤
│ 2. METADATA ACTIVA: Grafo de contexto generado y mantenido    │
│    automáticamente (IA clasifica, etiqueta, detecta PII)      │
├──────────────────────────────────────────────────────────────┤
│ 1. FUENTES HETEROGÉNEAS: BigQuery, GCS, Cloud SQL, on-prem,   │
│    otras nubes -sin importar el formato ni quién es dueño     │
└──────────────────────────────────────────────────────────────┘
```

La palabra clave es **"activa"**: a diferencia de un catálogo tradicional donde alguien documenta las tablas a mano (y ese trabajo queda desactualizado en semanas), un Data Fabric **escanea continuamente** las fuentes, infiere relaciones, sugiere clasificaciones de sensibilidad y mantiene el grafo de metadata sincronizado con la realidad — típicamente con asistencia de modelos de lenguaje.

---

## 4. Mesh vs. Fabric: No Son lo Mismo, No Son Excluyentes

| Dimensión | Data Mesh | Data Fabric |
|---|---|---|
| **Naturaleza** | Paradigma organizacional/socio-técnico | Patrón de arquitectura tecnológica |
| **Pregunta que responde** | ¿Quién es dueño y quién decide? | ¿Cómo se conecta y descubre todo automáticamente? |
| **Unidad central** | El *dominio* y el *data product* | El *grafo de metadata activa* |
| **Dónde vive el poder de decisión** | Descentralizado en los dominios | Centralizado en la capa de metadata (pero el dato físico puede estar distribuido) |
| **Rol de la IA** | No es un requisito del paradigma | Frecuentemente central (clasificación automática, generación de contexto) |
| **Puede existir sin el otro** | Sí, pero sin buena tooling de descubrimiento se vuelve caótico | Sí, sobre una organización centralizada tradicional |
| **Cómo se combinan** | El Fabric es la **plataforma de autoservicio** (principio #3) que hace operable al Mesh | El Mesh es una forma posible de **organizar** lo que el Fabric conecta |

> [!NOTE]
> En la práctica de 2026, la mayoría de las implementaciones de Data Mesh en la nube **se construyen sobre** una capa de Data Fabric (catálogo + metadata activa + IA), porque manualmente mantener el principio #4 (gobierno federado) sin automatización es insostenible a escala. Google Cloud posiciona explícitamente a Knowledge Catalog como esa capa de Fabric — y le agrega el recurso `Data Product` para que sirva directamente al modelo Mesh también.

---

## 5. Knowledge Catalog: la Pieza Técnica de Google Cloud

El catálogo de metadatos de Google Cloud ha cambiado de nombre varias veces — importante tenerlo claro porque bastante documentación y tutoriales en internet todavía usan nombres viejos:

```
LÍNEA DE TIEMPO DEL PRODUCTO

Data Catalog  ──►  Dataplex  ──►  Dataplex Universal Catalog  ──►  Knowledge Catalog
(original)         (~2021)         (~2023-2024)                    (abril 2026, nombre actual)

⚠️ Data Catalog (el nombre original) está DEPRECADO y se descontinúa el 1 de junio de 2026.
   La API, el cliente, el CLI y los nombres IAM siguen usando "dataplex"/"datacatalog"
   internamente aunque el producto ahora se llame "Knowledge Catalog" en la consola.
```

Knowledge Catalog es, en términos de la arquitectura del §3, la capa de **metadata activa impulsada por IA**: extrae semántica de datos estructurados y no estructurados automáticamente, construye un **grafo de contexto** dinámico, y lo usa tanto para búsqueda humana como para "aterrizar en la verdad de la empresa" a agentes de IA (reduciendo alucinaciones al consultar datos corporativos).

---

## 6. De Conceptos a Recursos: Cómo Knowledge Catalog Implementa Mesh y Fabric

| Concepto de Mesh/Fabric | Recurso concreto en Knowledge Catalog |
|---|---|
| Datos como Producto (Mesh #2) | **Data Product**: recurso de primera clase — nombre, dueño, descripción, activos de BigQuery adjuntos, contrato de refresco, se publica y los consumidores solicitan acceso |
| Plataforma de autoservicio (Mesh #3) | **Grupos de acceso** dentro de un Data Product + roles IAM (`dataplex.dataProductsConsumer`) — un consumidor descubre y solicita acceso sin pasar por el equipo central |
| Gobierno computacional federado (Mesh #4) | **Aspect Types**: plantillas de metadata custom (ej. "Dueño del Dominio", "SLA de Frescura", "Estado") que cada dominio llena sobre sus propios activos, buscables y auditables centralmente |
| Vocabulario compartido entre dominios (Mesh #4 + Fabric) | **Business Glossary**: términos de negocio (ej. "Customer", "Revenue") vinculados a columnas de tablas de *distintos* dominios/sistemas |
| Metadata activa (Fabric capa 2) | Metadata técnica **auto-ingerida** desde BigQuery, GCS, Cloud SQL, etc. — sin trabajo manual |
| Descubrimiento unificado (Fabric capa 4) | **Búsqueda** (por palabra clave o lenguaje natural) que cruza activos de cualquier dominio/sistema conectado |

---

## 7. Marcos de Gobernanza: Catálogo, Diccionario y Linaje

Un marco de gobierno de datos completo se apoya en tres piezas de metadata, no solo el catálogo:

```
LAS 3 PIEZAS DE UN MARCO DE GOBERNANZA

1. CATÁLOGO          ¿Qué activos existen y dónde están?
   (§5-6 de este documento: Knowledge Catalog, Data Products, búsqueda)

2. DICCIONARIO        ¿Qué SIGNIFICA cada campo, en lenguaje de negocio?
   (§1 del Lab: Business Glossary — "Customer", "Revenue"...)

3. LINAJE              ¿De DÓNDE vino este dato y A DÓNDE va?
   (Nuevo en este módulo: qué tablas/queries alimentaron a cuáles)
```

Las dos primeras ya las construiste en el Lab 06 (Pasos 0-5). El **linaje** es la pieza que responde "si esta métrica está mal, ¿qué tablas río arriba pudieron causarlo?" o "si cambio esta columna, ¿qué se rompe río abajo?" — información que ningún glosario ni catálogo estático puede darte por sí solo.

### 7.1. El estándar abierto: OpenLineage

**OpenLineage** es una especificación abierta (proyecto **Graduado** de la Linux Foundation AI & Data, no un producto de un solo vendor) para capturar eventos de linaje de forma consistente entre herramientas distintas: Airflow, Spark, dbt, Flink y otras emiten eventos OpenLineage cuando corren un job, y cualquier backend compatible los puede recibir. **Marquez** es el backend de referencia (también LF AI & Data) que los almacena y visualiza.

**Por qué importa incluso si usas Knowledge Catalog:** si tu organización es multi-nube o tiene herramientas de datos que no son de Google (Airflow on-prem, Spark en Databricks, dbt), OpenLineage es el mecanismo para que el linaje de *esas* herramientas también termine en un solo lugar, en vez de tener un grafo de linaje por cada nube/herramienta.

### 7.2. Catálogos open-source: Amundsen y DataHub

> [!WARNING]
> **Amundsen** (el catálogo de datos open-source creado por Lyft en 2019, donado a Linux Foundation AI en 2020) **fue archivado en septiembre de 2026 por inactividad** — su repositorio quedó en solo lectura, sin mantenimiento activo. Lo cubrimos aquí porque es un referente histórico importante y todavía aparece mencionado en mucha literatura de arquitectura de datos, pero **no lo recomendamos para un proyecto nuevo en 2026**.

| | Amundsen (archivado, 2026) | DataHub (activo) | Knowledge Catalog (GCP nativo) |
|---|---|---|---|
| **Origen** | Lyft (2019) | LinkedIn (2019) | Google Cloud |
| **Estado (2026)** | Archivado, solo lectura | Activo, mantenido | Activo, GA |
| **Infraestructura propia** | Sí (Neo4j/Elasticsearch) | Sí (varios backends) | No — es SaaS gestionado |
| **Multi-nube / vendor-neutral** | Sí | Sí | No — solo GCP |
| **Costo de operación** | Auto-hospedado (cómputo + mantenimiento) | Auto-hospedado (cómputo + mantenimiento) | Incluido en Google Cloud, sin cargo por catalogación |

**Por qué el Lab 06 no monta Amundsen/DataHub:** requieren su propia infraestructura (mínimo un grafo tipo Neo4j + un motor de búsqueda tipo Elasticsearch, típicamente en GKE o Compute Engine) — eso es, en sí mismo, un proyecto de varias horas, no algo que quepa en un taller de $0 y menos de 2 horas. Si tu organización necesita un catálogo **multi-nube** (no solo GCP), DataHub es hoy la opción open-source activa a evaluar; si vives 100% en Google Cloud, Knowledge Catalog te da lo mismo sin operar infraestructura propia.

---

## 8. Seguridad: Cifrado y Enmascaramiento de Datos

### 8.1. Cifrado en reposo y en tránsito (la parte que casi nunca tienes que configurar)

En Google Cloud, **el cifrado en reposo y en tránsito es automático por defecto** en BigQuery, Cloud Storage y el resto de servicios usados en este repositorio — no hay un interruptor que prender ni un paso de laboratorio que hacer. Google gestiona las claves de cifrado (*Google-managed encryption keys*) de forma transparente. La única decisión real que toma un arquitecto aquí es si usar las claves gestionadas por Google (por defecto, $0 adicional) o migrar a **CMEK** (*Customer-Managed Encryption Keys*, vía Cloud KMS) cuando hay un requisito de cumplimiento que exige control explícito y rotación propia de las claves — una decisión de gobierno corporativo, no algo que se resuelva con un ejercicio de 15 minutos, por eso no tiene un paso dedicado en el lab.

### 8.2. Enmascaramiento dinámico: la seguridad que SÍ es hands-on

Donde la seguridad de datos se vuelve una decisión de **diseño de gobernanza** (no solo de infraestructura) es en el control de acceso **a nivel de columna** — la pregunta de "¿quién puede ver el valor real de esta columna sensible, y quién debería ver una versión enmascarada?"

En BigQuery esto se construye con dos piezas, ambas gestionadas dentro de Knowledge Catalog:

```
DE COLUMNA SENSIBLE A COLUMNA ENMASCARADA

1. TAXONOMÍA + POLICY TAG          Clasificas la columna: "esto es dato
   (Knowledge Catalog)              financiero confidencial"

2. REGLA DE ENMASCARAMIENTO         Defines QUÉ ve alguien sin el permiso
   (Data Policy, vinculada           de "lectura sin máscara": NULL, un
   al Policy Tag)                    valor por defecto, un hash SHA-256...

3. ROLES IAM (sobre el Policy Tag, NO sobre la tabla)
   - Data Catalog Fine-Grained Reader → ve el valor REAL
   - BigQuery Masked Reader           → ve el valor ENMASCARADO
   - Ninguno de los dos               → la query falla en esa columna
```

**Reglas de enmascaramiento predefinidas disponibles:** `Nullify` (NULL), `Default masking value` (valor por defecto del tipo, ej. `0` o `""`), `Hash (SHA-256)`, `Random hash` (hash con sal aleatoria por consulta), `Email mask` (oculta el nombre de usuario, conserva el dominio), `First/Last four characters` (muestra solo 4 caracteres), `Date year mask` (trunca una fecha a solo el año), o una **rutina custom** (una función SQL propia).

> [!IMPORTANT]
> Hay un detalle fácil de pasar por alto: **crear la taxonomía y el policy tag no activa nada por sí solo** — son solo etiquetas hasta que haces clic explícitamente en **"Enforce access control"** sobre la taxonomía. Antes de eso, cualquiera con acceso normal a la tabla ve el dato sin ninguna restricción, aunque la columna ya tenga un policy tag asignado. Esto lo verás en el Paso 6 del [Lab 06](lab.md).

Esta es, en la práctica, la forma más concreta de "gobierno computacional federado" (Data Mesh, principio #4) aplicado a seguridad: la regla de enmascaramiento vive pegada al dato (vía el Policy Tag), no en un documento de políticas separado que alguien tiene que recordar aplicar manualmente en cada consulta.

### 8.3. Lo que viene después de Policy Tags: IAM Data Governance Tags

Desde mediados de 2026, Google Cloud tiene en **Preview** un mecanismo más nuevo llamado **IAM Data Governance Tags** — tags de Resource Manager (los mismos que usarías para condiciones de IAM u organization policies) marcados con `purpose=DATA_GOVERNANCE`. A diferencia de los Policy Tags clásicos (regionales, taxonomía plana, acoplados a BigQuery), estos tags son **globales, jerárquicos (hasta 5 niveles) y desacoplados** (etiquetas primero, se aplican las reglas de acceso después) — pensados para organizaciones multi-región con necesidades de *disaster recovery* del propio esquema de gobierno.

**Por qué el Lab 06 sigue usando Policy Tags clásicos:** siguen siendo GA, completamente soportados, y son la opción correcta para el caso de este lab — un solo proyecto, una sola región. IAM Data Governance Tags es la dirección a la que se mueve el producto para escenarios más grandes, pero al ser Preview no es la elección adecuada para un taller que además apunta a mantenerse en $0 y con la menor superficie de cosas que puedan cambiar antes de que llegue a GA.

---

## 9. Matriz de Decisión

```
¿Tienes más de un equipo generando y siendo responsable de datos de negocio?
  ├── NO ──► Un catálogo centralizado tradicional basta. Mesh es sobre-ingeniería.
  └── SÍ ──► ¿El equipo central de datos es hoy un cuello de botella para publicar
              o descubrir datos nuevos?
               ├── NO ──► Aún no necesitas Mesh; invierte en Fabric (catalogación
               │           automática) para que no se vuelva cuello de botella.
               └── SÍ ──► ¿Los dominios tienen capacidad técnica para ser dueños
                            de sus propios datos (modelarlos, documentarlos)?
                             ├── NO ──► Invierte primero en una plataforma de
                             │           autoservicio (Fabric) antes de descentralizar.
                             └── SÍ ──► Implementa Data Mesh sobre Knowledge Catalog:
                                          Data Products + Aspect Types + Glosario federado.
```

---

## 10. Costos y Límites

> [!IMPORTANT]
> **Gratis:** organización de datos (lakes/zonas/activos), Data Products, Aspect Types, Business Glossary, propagación de políticas de seguridad IAM, metadata técnica auto-ingerida, **búsqueda del catálogo** (incluida la búsqueda en lenguaje natural), **taxonomías, policy tags y enmascaramiento dinámico** (GA, sin cargo de licencia separado — solo pagas el procesamiento normal de BigQuery sobre esos datos), y el **linaje automático de BigQuery** (metadata de dependencias entre jobs, igual que el resto de la metadata técnica auto-ingerida).
>
> **De pago:** **"Data Insights"** — la función que usa Gemini para *generar* automáticamente descripciones, relaciones y consultas de ejemplo sobre tus datos — se factura por *data tokens* (entrada/salida) a partir del **27 de octubre de 2026**. También generan costo los *discovery scans* y conectores administrados (disparan jobs de Dataflow/Spark por detrás). El [Lab 06](lab.md) evita deliberadamente ambas funciones — todo lo que hace el laboratorio cae en la columna gratuita.

Límites publicados (nivel proyecto): hasta 5.000 términos de glosario, 200 Data Products por proyecto, resultados de búsqueda limitados a 500.

---

## 11. Referencias Técnicas

1. **Dehghani, Z. (2019-2022).** *How to Move Beyond a Monolithic Data Lake to a Distributed Data Mesh.* MartinFowler.com / *Data Mesh: Delivering Data-Driven Value at Scale.* O'Reilly Media.
2. **Gartner.** *Data Fabric Architecture* — definición de referencia del patrón (activa metadata, integración automatizada).
3. **Google Cloud — Knowledge Catalog overview.** [docs.cloud.google.com/dataplex/docs/introduction](https://docs.cloud.google.com/dataplex/docs/introduction).
4. **Google Cloud — Transition from Data Catalog to Knowledge Catalog.** [docs.cloud.google.com/dataplex/docs/transition-to-dataplex-catalog](https://docs.cloud.google.com/dataplex/docs/transition-to-dataplex-catalog).
5. **Google Cloud — Create data products.** [docs.cloud.google.com/dataplex/docs/create-data-products](https://docs.cloud.google.com/dataplex/docs/create-data-products).
6. **Google Cloud — Build foundational data governance.** [docs.cloud.google.com/dataplex/docs/build-foundational-data-governance](https://docs.cloud.google.com/dataplex/docs/build-foundational-data-governance).
7. **Google Cloud — What is data as a product (DaaP)?** [cloud.google.com/discover/what-is-data-as-a-product](https://cloud.google.com/discover/what-is-data-as-a-product).
8. **Google Cloud — Knowledge Catalog pricing.** [cloud.google.com/products/knowledge-catalog/pricing](https://cloud.google.com/products/knowledge-catalog/pricing).
9. **OpenLineage.** [openlineage.io](https://openlineage.io/) — especificación abierta (LF AI & Data, proyecto Graduado) para eventos de linaje.
10. **Lyft Engineering — Open Sourcing Amundsen.** [eng.lyft.com/open-sourcing-amundsen](https://eng.lyft.com/open-sourcing-amundsen-a-data-discovery-and-metadata-platform-2282bb436234) — contexto histórico; repositorio archivado en septiembre de 2026.
11. **DataHub.** [datahub.com](https://datahub.com/) — catálogo open-source activo (originado en LinkedIn), alternativa multi-nube a evaluar si Amundsen ya no es viable.
12. **Google Cloud — Restrict access with column-level access control.** [docs.cloud.google.com/bigquery/docs/column-level-security](https://docs.cloud.google.com/bigquery/docs/column-level-security).
13. **Google Cloud — Introduction to data masking.** [docs.cloud.google.com/bigquery/docs/column-data-masking-intro](https://docs.cloud.google.com/bigquery/docs/column-data-masking-intro).
14. **Google Cloud — View data lineage for Google Cloud systems.** [docs.cloud.google.com/dataplex/docs/use-lineage](https://docs.cloud.google.com/dataplex/docs/use-lineage).
