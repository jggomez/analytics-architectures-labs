# Lab 06 — Gobierno Federado con Knowledge Catalog: Data Mesh y Data Fabric en la Práctica

> 📖 **Marco Teórico:** Consulta la [Guía de Data Mesh, Data Fabric y Knowledge Catalog](teoria.md) para entender los 4 principios de Data Mesh, qué es un Data Fabric, y cómo Google Cloud los materializa.
>
> Este lab es independiente — no requiere haber completado los Módulos 01-05.

---

## Codelab — Taller de 1 hora: "RetailCo, Dos Dominios"

**RetailCo** tiene dos equipos que generan datos de forma totalmente independiente: **Ventas** (dueño de `sales.orders`) y **Marketing** (dueño de `marketing.campaigns`). Nadie más los edita. Ambos comparten un concepto de negocio — `customer_id` — pero hasta hoy no hay ningún vocabulario común ni forma de descubrir qué existe en el otro dominio sin preguntar por Slack.

En este taller vas a usar **Knowledge Catalog** para resolver exactamente eso: un glosario de negocio compartido, un "contrato" de gobernanza que cada dominio se autoaplica, un Data Product publicado y descubrible, y una búsqueda que cruza ambos dominios.

```
ARQUITECTURA DEL LABORATORIO — RETAILCO

┌───────────────────────────┐        ┌───────────────────────────┐
│      DOMINIO VENTAS       │        │     DOMINIO MARKETING      │
│  BigQuery: sales.orders   │        │ BigQuery: marketing.campaigns │
│  (dueño: equipo Ventas)   │        │   (dueño: equipo Marketing) │
└─────────────┬─────────────┘        └─────────────┬───────────────┘
              │                                    │
              │      customer_id (concepto compartido)
              └────────────────┬───────────────────┘
                                ▼
                 ┌───────────────────────────────┐
                 │        KNOWLEDGE CATALOG       │
                 │ • Glosario de negocio (federado)│
                 │ • Aspect Type "Domain Contract" │
                 │   (gobierno federado)           │
                 │ • Data Product publicado        │
                 │ • Búsqueda cruzando dominios    │
                 └───────────────────────────────┘
```

> [!NOTE]
> Este es un taller casi 100% de **consola** (Knowledge Catalog no tiene comandos `gcloud` dedicados para glosarios/aspectos/data products todavía — se gestionan por Console o API REST). Usarás Cloud Shell solo para crear las 2 tablas de BigQuery.

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. Explicar la diferencia entre **Data Mesh** (organizacional) y **Data Fabric** (tecnológico) con un ejemplo concreto.
2. Crear un **Business Glossary** federado que le da un vocabulario común a dominios que no comparten equipo ni base de datos.
3. Definir un **Aspect Type** custom y usarlo como "contrato de dominio" — la forma computacional de aplicar gobierno federado sin un comité central.
4. Empaquetar una tabla como **Data Product**: nombre, dueño, activos, y publicarlo para que sea descubrible.
5. Vincular columnas de tablas de *distintos* dominios al mismo término de glosario.
6. Buscar en el catálogo cruzando dominios por término de negocio y por campo de aspecto.

### Prerrequisitos

- Proyecto de Google Cloud con facturación habilitada.
- Cloud Shell (para las 2 tablas de BigQuery) + acceso a la consola web de Google Cloud.
- Rol de Owner o Editor en el proyecto (para crear Data Products necesitas, como mínimo, `roles/dataplex.dataProductsAdmin`; Owner ya lo incluye).
- Conocimientos básicos de SQL. No requiere haber hecho otros módulos de este repositorio.

### Costo Estimado (FinOps)

| Concepto | Recurso en el Lab | ¿Cubierto por capa gratuita? | Estimado |
|---|---|---|---|
| **Business Glossary, Aspect Types, Data Product** | Metadata de gobierno | ✅ Sí — sin costo (ver [teoría §8](teoria.md#8-costos-y-límites)) | **$0.00** |
| **Búsqueda del catálogo** (incluida en lenguaje natural) | Discovery/search | ✅ Sí — explícitamente sin cargo | **$0.00** |
| **BigQuery** (2 tablas mínimas) | Storage + queries de creación | ✅ Sí (10 GiB / 1 TiB gratis al mes) | **$0.00** |

> [!WARNING]
> **No hagas clic en "Generate insights" / "Data Insights"** en ningún panel de Knowledge Catalog durante este lab. Esa función usa Gemini y se factura por tokens desde el 27 de octubre de 2026 — no la necesitamos para nada de lo que hacemos aquí. Tampoco actives *discovery scans* automáticos (disparan jobs de Dataflow/Spark facturables). Siguiendo los pasos tal cual, el costo de este laboratorio es **$0.00**.

---

### Mapa del Laboratorio (Taller de 60 minutos)

```
Paso 0  (5 min)   Aprovisionar: 2 tablas BigQuery (sales.orders, marketing.campaigns)
Paso 1  (10 min)  Glosario de negocio compartido (Customer, Revenue, Marketing Spend)
Paso 2  (10 min)  Aspect Type "Domain Contract" -gobierno federado por dominio
Paso 3  (10 min)  Vincular columnas de AMBOS dominios al mismo glosario
Paso 4  (10 min)  Crear y publicar un Data Product (sales.orders)
Paso 5  (10 min)  Búsqueda cruzando dominios
Paso 6  (5 min)   Limpieza + Retos Opcionales
```

---

## Paso 0 — Aprovisionar los Dos Dominios (5 min)

```bash
export PROJECT_ID=$(gcloud config get-value project)
export REGION=us-central1

gcloud services enable \
  dataplex.googleapis.com \
  datacatalog.googleapis.com \
  bigquery.googleapis.com

bq --location="$REGION" mk --dataset --description "RetailCo - Dominio Ventas" "${PROJECT_ID}:sales"
bq --location="$REGION" mk --dataset --description "RetailCo - Dominio Marketing" "${PROJECT_ID}:marketing"

bq query --use_legacy_sql=false <<'EOF'
CREATE OR REPLACE TABLE `sales.orders` AS
SELECT * FROM UNNEST([
  STRUCT('ORD-001' AS order_id, 'CUST-01' AS customer_id, 120.50 AS amount, DATE('2026-09-01') AS order_date),
  STRUCT('ORD-002', 'CUST-02', 899.00, DATE('2026-09-03')),
  STRUCT('ORD-003', 'CUST-01', 45.00, DATE('2026-09-10'))
]);
EOF

bq query --use_legacy_sql=false <<'EOF'
CREATE OR REPLACE TABLE `marketing.campaigns` AS
SELECT * FROM UNNEST([
  STRUCT('CMP-01' AS campaign_id, 'CUST-01' AS customer_id, 'Email' AS channel, 15.00 AS spend),
  STRUCT('CMP-02', 'CUST-02', 'Social', 42.50),
  STRUCT('CMP-03', 'CUST-01', 'Search', 8.75)
]);
EOF
```

> [!NOTE]
> Nota que **nadie coordinó los esquemas**: `sales.orders` y `marketing.campaigns` viven en datasets distintos, los creó (hipotéticamente) gente distinta, y solo comparten el concepto `customer_id` — sin que exista todavía ningún acuerdo formal de qué significa. Ese es exactamente el punto de partida real de un Data Mesh: dominios autónomos, sin vocabulario ni gobierno compartido *todavía*.

Knowledge Catalog **auto-ingiere** la metadata técnica de estas 2 tablas sin que hagas nada más — esa es la capa de Data Fabric operando por detrás (ver [teoría §3](teoria.md#3-data-fabric-automatización-tecnológica-vía-metadata-activa)). Puedes confirmarlo yendo a la consola de Google Cloud → **Knowledge Catalog → Search** y buscando `orders` o `campaigns`: ya aparecen, sin que las hayas registrado a mano.

---

## Paso 1 — Glosario de Negocio Compartido (10 min)

Esta es la pieza de **vocabulario federado** (principio #4 de Data Mesh + capa de metadata activa de Data Fabric): un significado de negocio único, que ambos dominios van a referenciar sin tener que ponerse de acuerdo cada vez que alguien crea una tabla nueva.

1. En la consola de Google Cloud, ve a **Knowledge Catalog → Glossaries**.
2. Clic en **Create Business Glossary**.
   - **Display name:** `RetailCo Business Glossary`
   - **Location:** `us-central1`
   - Clic en **Create**.
3. Dentro del glosario, clic en **Create Category**:
   - **Nombre:** `Core Entities`
4. Selecciona la categoría `Core Entities` → **Add term**:
   - **Nombre:** `Customer`
   - Abre el término y agrega una descripción: *"Persona identificada por customer_id, compartida por todos los dominios de RetailCo."*
5. Crea una segunda categoría, `Finance Metrics`, y dentro de ella dos términos:
   - **`Revenue`** — *"Monto bruto facturado por una orden de venta."*
   - **`Marketing Spend`** — *"Costo de una campaña de marketing atribuible a un cliente."*

> [!NOTE]
> Fíjate que este glosario **no vive dentro de `sales` ni de `marketing`** — es un recurso independiente de Knowledge Catalog. Cualquier dominio nuevo que se sume a RetailCo en el futuro reutiliza estos mismos términos en vez de inventar los suyos (evita el clásico problema de Mesh de "cada dominio con su propio significado de 'cliente'").

---

## Paso 2 — Aspect Type "Domain Contract": Gobierno Federado (10 min)

Un **Aspect Type** es una plantilla de metadata custom. Aquí la usamos como el "contrato" que cada dominio se autoaplica — nadie de un equipo central llena esto por ellos, pero el formato es el mismo para todos, así que se puede auditar y buscar de forma consistente en toda la malla.

1. Ve a **Knowledge Catalog → Metadata types → Aspect types** (pestaña **Custom**).
2. Clic en **Create**.
   - **Display name:** `Domain Contract`
   - **Location:** `us-central1`
3. En **Template**, agrega estos campos con **Add field**:
   - `Domain Owner` — tipo **Text**, marca **Is Required**.
   - `Product Status` — tipo **Enum**, valores: `Draft`, `Published`. Marca **Is Required**.
   - `Refresh SLA` — tipo **Text** (opcional), ej. libre como `"Diario antes de las 09:00"`.
4. **Save**.

### 2.1. Adjuntar el contrato a las tablas de cada dominio

1. Ve a **Knowledge Catalog → Search**, busca `orders`, abre la entrada de `sales.orders`.
2. En la pestaña **Details**, junto a **Optional aspects**, clic en **Add**.
3. Selecciona el Aspect Type `Domain Contract` y llena:
   - `Domain Owner`: `equipo-ventas@retailco.example`
   - `Product Status`: `Published`
   - `Refresh SLA`: `Diario antes de las 09:00`
4. **Save**.
5. Repite exactamente lo mismo para `marketing.campaigns`, con `Domain Owner: equipo-marketing@retailco.example` y `Product Status: Draft` (a propósito — así el Paso 5 tiene algo interesante que filtrar).

> [!IMPORTANT]
> Esto **es** gobierno computacional federado en la práctica: no hay un formulario en Confluence ni un Google Sheet que alguien tiene que mantener actualizado a mano — el "contrato" vive pegado al activo técnico, en el mismo sistema donde se descubre y se consulta, y es **buscable** (Paso 5).

---

## Paso 3 — Vincular Columnas de Ambos Dominios al Mismo Glosario (10 min)

Ahora conectamos el vocabulario compartido del Paso 1 con las columnas reales — de **dos dominios distintos** — usando exactamente el mismo término.

1. Abre la entrada de `sales.orders` en Knowledge Catalog → pestaña **Schema**.
2. Marca el checkbox de la columna `customer_id` → clic en **Add business term** → selecciona `Customer`.
3. Marca el checkbox de la columna `amount` → **Add business term** → selecciona `Revenue`.
4. Ve a la entrada de `marketing.campaigns` → pestaña **Schema**.
5. Vincula `customer_id` → `Customer` (el **mismo** término que usaste en `sales.orders`).
6. Vincula `spend` → `Marketing Spend`.

> [!NOTE]
> Acabas de crear el puente semántico entre dos tablas que ni siquiera están en el mismo dataset: `sales.orders.customer_id` y `marketing.campaigns.customer_id` ahora apuntan al **mismo concepto de negocio** en el catálogo, aunque cada dominio siga siendo dueño exclusivo de su tabla. Esto es lo que la [teoría §6](teoria.md#6-de-conceptos-a-recursos-cómo-knowledge-catalog-implementa-mesh-y-fabric) llama "el Fabric conecta lo que el Mesh mantiene separado".

---

## Paso 4 — Crear y Publicar un Data Product (10 min)

Este es el recurso que hace literal el principio "**datos como producto**" de Data Mesh.

1. Ve a **Knowledge Catalog → Data products** → clic en **Create**.
   - **Data product name:** `Sales Orders`
   - **Data product ID:** `sales-orders-product` (minúsculas y guiones — formato obligatorio)
   - **Project / Region:** tu proyecto / `us-central1`
   - **Description:** *"Órdenes de venta de RetailCo, propiedad del dominio Ventas."*
   - **Contacts → Data product owner:** tu propio correo (o `equipo-ventas@retailco.example`)
   - Clic en **Create data product**.
2. En **Add assets**, clic en **+Add**, busca y selecciona la tabla `sales.orders`. (No necesitas un Lake ni una Zone de Dataplex — el Data Product se puede armar directo sobre tablas de BigQuery.)
3. (Opcional, si quieres ver el flujo completo) En **Access groups**, clic en **Add access group**:
   - **Nombre:** `Analistas`
   - **Access group identifier:** un Google Group que tengas a mano, o tu propio correo si no tienes uno.
   - En **Configure permissions** del asset, asigna el rol `BigQuery Metadata Viewer` a ese grupo.
4. En **Add aspect**, adjunta el `Domain Contract` que ya llenaste en el Paso 2 (o crea uno nuevo si prefieres mantenerlos separados).
5. Publica el Data Product.

> [!NOTE]
> En una organización real, un consumidor de otro dominio ahora puede **buscar `Sales Orders` en el catálogo, ver quién es el dueño, y solicitar acceso** sin escribirle a nadie del equipo de Ventas directamente — eso es autoservicio (principio #3 de Mesh). El dueño del producto aprueba o rechaza la solicitud desde la misma consola. Como este lab es de una sola persona, no vas a completar el ciclo solicitud→aprobación con una segunda cuenta, pero el botón **Request access** que ves en la vista de un Data Product publicado es exactamente ese flujo.

---

## Paso 5 — Búsqueda Cruzando Dominios (10 min)

Ahora, el momento de verdad: ¿puedes descubrir y filtrar activos de **ambos** dominios sin saber de antemano en qué dataset vive cada uno?

1. Ve a **Knowledge Catalog → Search**.
2. Busca por término de negocio: escribe `Customer` — deberías ver **columnas de `sales.orders` y de `marketing.campaigns`** en los resultados, aunque son tablas de dominios distintos.
3. Filtra por aspecto con la sintaxis estructurada de búsqueda: `aspect:<id-del-aspect-type>.<id-del-campo>=Published`. El `<id-del-aspect-type>` y el `<id-del-campo>` son slugs en minúsculas y guiones que Knowledge Catalog generó automáticamente a partir de los nombres que pusiste en el Paso 2 (`Domain Contract` → algo como `domain-contract`; `Product Status` → algo como `product-status`) — **verifica el id exacto** abriendo el Aspect Type en **Metadata types → Aspect types** (se muestra junto al nombre del campo) antes de escribir la búsqueda, en vez de asumir el slug de memoria. Con el id correcto, algo como:
   ```
   aspect:domain-contract.product-status=Published
   ```
   Debería devolver solo `sales.orders` (recuerda: le pusiste `Published`, y a `marketing.campaigns` le pusiste `Draft`).
4. Prueba también con búsqueda en lenguaje natural (gratis, no es "Data Insights"): escribe algo como *"tablas con Revenue publicadas"* y observa qué te devuelve.

> [!IMPORTANT]
> Esta búsqueda cruzada — sin que tú sepas de antemano si el dato está en `sales`, `marketing`, o un tercer dominio que se sume mañana — es el valor central de la capa de Data Fabric (teoría §3, capa 4 "Consumo"). Es lo que hace que Data Mesh **escale**: puedes tener 50 dominios autónomos y aun así descubrir todo desde un solo lugar.

---

## Paso 6 — Limpieza (5 min)

```bash
bq rm --recursive --force "${PROJECT_ID}:sales"
bq rm --recursive --force "${PROJECT_ID}:marketing"
```

Luego, en la consola de Google Cloud:
1. **Knowledge Catalog → Data products** → abre `Sales Orders` → elimínalo.
2. **Knowledge Catalog → Glossaries** → abre `RetailCo Business Glossary` → elimínalo (borra también sus categorías/términos).
3. **Knowledge Catalog → Metadata types → Aspect types (Custom)** → elimina `Domain Contract`.

> [!NOTE]
> A diferencia de otros módulos de este repositorio, aquí no hay ninguna instancia de cómputo (Cloud SQL, Dataflow, Datastream) corriendo en segundo plano — todo lo creado en este lab es metadata gratuita, así que la limpieza es por prolijidad, no por costo.

---

## Retos Opcionales (Extensión)

### Reto 1: Un tercer dominio reutilizando el mismo glosario

Crea un dataset `logistics` con una tabla `shipments` (`shipment_id`, `customer_id`, `status`). Vincula su columna `customer_id` al mismo término `Customer` del Paso 1 — sin crear un término nuevo. Confirma con una búsqueda que ahora aparecen **3 dominios** distintos bajo el mismo concepto.

<details>
<summary>👀 Ver Pista de Solución Reto 1</summary>

El punto del reto es que el Paso 1 (crear el glosario) **no se repite** — un dominio nuevo solo necesita el paso de "Add business term" sobre su propia columna, reutilizando el término existente. Eso es literalmente lo que hace que el glosario escale: el costo de sumar un dominio nuevo es un clic, no una reunión de alineación de nomenclatura.

</details>

### Reto 2: Empaqueta también `marketing.campaigns` como Data Product

Repite el Paso 4 para el dominio Marketing, con `Product Status: Draft` reflejado en su `Domain Contract`. Busca (con el id de campo real que veas en tu Aspect Type, ver Paso 5) el equivalente a `aspect:domain-contract.product-status=Draft` y confirma que solo aparece el de Marketing.

<details>
<summary>👀 Ver Pista de Solución Reto 2</summary>

Es la misma secuencia del Paso 4, cambiando la tabla origen y el dueño de contacto. Vale la pena notar cómo el campo `Product Status` del Aspect Type — no algo hardcodeado en el producto de la plataforma — es lo que determina si algo se considera "listo para consumo" o no; eso es gobierno federado, no centralizado.

</details>

### Reto 3: Compara esto contra un catálogo centralizado manual

Imagina que en vez de Knowledge Catalog, RetailCo mantiene un Google Sheet compartido donde cada equipo anota sus tablas. Escribe (en un párrafo) qué se rompe primero cuando la empresa pasa de 2 a 20 dominios, y cuál de los 4 principios de Data Mesh (ver [teoría §2](teoria.md#2-data-mesh-descentralización-organizacional)) está resolviendo cada pieza que reemplazaste en este lab.

<details>
<summary>👀 Ver Pista de Solución Reto 3</summary>

Lo primero que se rompe con un Sheet manual es el principio #4 (gobierno federado): nadie audita ni fuerza que el Sheet esté actualizado, y a los 20 dominios el documento es ruido no confiable. El Aspect Type resuelve eso al vivir pegado al activo técnico. El Glosario resuelve la falta de vocabulario común (parte del mismo principio #4). El Data Product resuelve el principio #2 (datos como producto) y el botón de "Request access" resuelve el principio #3 (autoservicio) — ninguno de los tres existe de forma confiable en un Sheet.

</details>

---

## Resumen de lo Aprendido

- **Data Mesh es una decisión organizacional** (quién es dueño, cómo se publica) — **Data Fabric es la tecnología** (cómo se descubre y conecta automáticamente). Knowledge Catalog es la pieza de Fabric de Google Cloud, con el recurso `Data Product` diseñado específicamente para operacionalizar Mesh encima.
- **El vocabulario compartido (Glosario) y el gobierno federado (Aspect Types) no requieren centralizar el dato** — cada dominio sigue siendo dueño de su tabla, pero ambos hablan el mismo idioma y se auditan con el mismo formato.
- **La búsqueda cruzando dominios es lo que hace que Mesh escale** — sin ella, cada dominio nuevo es un silo más que hay que descubrir preguntando, no buscando.
- **El producto se llama Knowledge Catalog desde abril de 2026** — si ves tutoriales que hablan de "Data Catalog" (el original, deprecado el 1-jun-2026) o "Dataplex Universal Catalog", son la misma familia de producto con nombres anteriores.

---

## Referencias

- [Knowledge Catalog overview](https://docs.cloud.google.com/dataplex/docs/introduction)
- [Build foundational data governance](https://docs.cloud.google.com/dataplex/docs/build-foundational-data-governance)
- [Establish foundational data context with Knowledge Catalog](https://docs.cloud.google.com/dataplex/docs/establish-foundational-data-context)
- [Create data products](https://docs.cloud.google.com/dataplex/docs/create-data-products)
- [Manage a business glossary](https://docs.cloud.google.com/dataplex/docs/manage-glossaries)
- [Knowledge Catalog pricing](https://cloud.google.com/products/knowledge-catalog/pricing)
- [Codelab oficial: Foundational Governance with Knowledge Catalog](https://codelabs.developers.google.com/dataplex-foundational-governance)
