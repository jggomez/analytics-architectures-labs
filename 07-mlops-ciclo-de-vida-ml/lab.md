# Lab 07 — MLOps en GCP: Feature Store, Model Registry, Endpoints y Reentrenamiento por Drift

> 📖 **Marco Teórico:** Consulta la [Guía de MLOps y Ciclo de Vida de ML](teoria.md) para entender qué es MLOps y sus fases, qué problema resuelve cada componente (Feature Store, Model Registry, serving, pipelines, monitoreo), data drift vs. concept drift, los trade-offs, y cómo se mapea cada concepto a GCP, AWS, Azure, Databricks y open source.
>
> Este lab es independiente: genera sus propios datos y no requiere haber completado los Módulos 01-06. Usa la narrativa de **FinTechCo** (solicitudes de crédito) del [Módulo 01](../01-patrones-y-modelado/lab.md).

---

## Codelab — Taller de ~2 horas: "FinTechCo, del dato Silver al modelo que se cuida solo"

**FinTechCo** quiere predecir qué solicitudes de crédito van a caer en impago. En este taller vas a llevar ese modelo **de la capa Silver a producción**, y después vas a construir el mecanismo que lo mantiene sano solo: un pipeline que detecta cuándo cambiaron los datos (*drift*), reentrena, compara el modelo nuevo contra el que está en producción, y **solo despliega si el nuevo es mejor**.

```
ARQUITECTURA DEL LABORATORIO — FINTECHCO MLOPS

  BigQuery: fintech_silver.solicitudes_historicas
                 │
                 ├──► Feature Store (Feature Group, offline)
                 │      registro y descubrimiento de features
                 ▼
  BigQuery ML: CREATE MODEL ... model_registry='VERTEX_AI'
                 │
                 ▼
  Model Registry: fintech_credit_risk  (v1, v2, ...)
                 │
                 ▼
  Endpoint: fintech-credit-endpoint  ◄──── predicciones online
                 ▲
                 │ despliega solo si el challenger gana
  ┌──────────────┴─────────────────────────────────────────┐
  │ PIPELINE DE REENTRENAMIENTO (Kubeflow Pipelines)        │
  │  0. leer champion actual (alias + historial)            │
  │  1. PSI (drift) ─► ¿> 0.2? ─► 2. reentrenar (BQML)       │
  │  3. AUC challenger vs. champion ─► ¿gana? ─► 4. deploy   │
  │     y el challenger pasa a ser el nuevo champion         │
  │  Ejecución manual  o  programada con cron                │
  └─────────────────────────────────────────────────────────┘
```

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. Registrar features de una tabla Silver de BigQuery en el **Feature Store** (offline) para que sean descubribles y reutilizables.
2. Probar varios modelos candidatos, registrar cada intento en un **experimento** y compararlos para elegir el mejor.
3. Entrenar el modelo elegido con **BigQuery ML** y registrarlo automáticamente en el **Model Registry** con versiones.
4. Desplegar el modelo en un **Endpoint** y obtener predicciones online.
5. Medir **data drift** con el *Population Stability Index* (PSI) usando SQL.
6. Construir un **pipeline de Kubeflow (KFP)** con *quality gates*: reentrena solo si hay drift y despliega solo si el modelo nuevo supera al actual.
7. Ejecutar ese pipeline **manualmente** y **programarlo con cron**.

### Prerrequisitos

- Proyecto de Google Cloud con facturación habilitada (idealmente un proyecto nuevo para este lab).
- **Todo se ejecuta en Google Cloud Shell**, más la consola web para observar el Model Registry, el endpoint y el pipeline.
- Rol de **Owner** en el proyecto. Es lo más simple para el taller, porque vas a otorgar roles a una cuenta de servicio.
- Conocimientos básicos de SQL y Python. No necesitas experiencia previa en ML: el modelo es una regresión logística entrenada con SQL.

### Costo Estimado (FinOps)

| Concepto | Recurso en el Lab | ¿Cubierto por capa gratuita? | Estimado |
|---|---|---|---|
| **BigQuery ML** (entrenamientos) | 6-7 `CREATE MODEL` sobre unos miles de filas (4 candidatos, v1 y los reentrenamientos) | ✅ Sí (el free tier incluye 10 GiB/mes de `CREATE MODEL`) | **~$0.00** |
| **BigQuery** (datos y consultas) | Tablas de pocos MB, consultas de PSI | ✅ Sí | **~$0.00** |
| **Feature Store** (solo offline) | Feature Group sobre una tabla BigQuery, sin online store | Sin nodos de serving (los datos viven en BigQuery) | **~$0.00** |
| **Endpoint** (predicción online) | 1 nodo `n1-standard-2` desplegado ~1-1.5 h | ❌ No: se factura **por hora mientras el modelo esté desplegado** | **~$0.15–0.30** |
| **Pipelines** | 2-3 ejecuciones del pipeline | ❌ No: cargo pequeño por ejecución más el cómputo de cada componente | **~$0.10–0.30** |
| **Experimentos** | Un experimento con 4 runs (parámetros y métricas), sin TensorBoard | Metadatos de pocos KB | **~$0.00** |
| **Cloud Storage** | Artefactos del pipeline | ✅ Sí | **~$0.00** |

> [!WARNING]
> **El endpoint es el recurso caro de este lab, y sigue facturando aunque nadie le envíe predicciones.** Un endpoint con un modelo desplegado tiene al menos un nodo encendido 24/7. El Paso 9 (Limpieza) no es opcional. Revisa los precios vigentes de predicción online en la página de precios de Agent Platform antes de empezar, porque cambian según el tipo de máquina y la región.

---

### Mapa del Laboratorio (~135 minutos)

```
Paso 0  (10 min)  Entorno: variables, APIs, bucket, datasets, permisos, Python
Paso 1  (10 min)  Datos: generar la capa Silver sintética (historia de 5000 solicitudes)
Paso 2  (10 min)  Feature Store: registrar las features (offline)
Paso 3  (25 min)  Experimentos: probar 4 candidatos y compararlos; entrenar v1 y registrarla
Paso 4  (20 min)  Endpoint: desplegar v1 y predecir online
Paso 5  (10 min)  Drift: generar un lote "sano" y uno "con drift" y compararlos
Paso 6  (15 min)  Pipeline: construirlo y correrlo con el lote sano (no debe reentrenar)
Paso 7  (25 min)  Pipeline con el lote con drift: reentrena, compara y despliega v2
Paso 8  (5 min)   Programar el pipeline con cron
Paso 9  (10 min)  Limpieza + Retos Opcionales
```

---

## Paso 0 — Preparar el Entorno (10 min)

### 0.1. Variables, APIs, bucket y datasets

```bash
export PROJECT_ID=$(gcloud config get-value project)
export PROJECT_NUMBER=$(gcloud projects describe "$PROJECT_ID" --format="value(projectNumber)")
export REGION=us-central1
export BUCKET="gs://${PROJECT_ID}-lab07"

gcloud services enable \
  aiplatform.googleapis.com \
  bigquery.googleapis.com \
  bigquerystorage.googleapis.com \
  cloudresourcemanager.googleapis.com \
  compute.googleapis.com \
  storage.googleapis.com

gcloud storage buckets create "$BUCKET" --location="$REGION"

bq --location="$REGION" mk --dataset --description "FinTechCo - Capa Silver (features)" "${PROJECT_ID}:fintech_silver"
bq --location="$REGION" mk --dataset --description "FinTechCo - Modelos BigQuery ML" "${PROJECT_ID}:fintech_ml"
```

### 0.2. Permisos para la cuenta de servicio que ejecuta el pipeline

Los pipelines corren con la **cuenta de servicio de Compute Engine por defecto**, salvo que indiques otra. Esa cuenta va a entrenar modelos en BigQuery, registrarlos en el Model Registry y desplegarlos en el endpoint, así que necesita permisos explícitos:

```bash
SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

for ROLE in roles/aiplatform.admin roles/bigquery.admin roles/storage.objectAdmin; do
  gcloud projects add-iam-policy-binding "$PROJECT_ID" \
    --member="serviceAccount:${SA}" --role="$ROLE" --condition=None --quiet > /dev/null
  echo "Otorgado $ROLE a $SA"
done
```

> [!NOTE]
> En proyectos creados desde 2024, la cuenta de servicio de Compute Engine por defecto **ya no recibe el rol Editor automáticamente**. Si no le das estos roles a mano, el pipeline falla a mitad de camino con errores de permisos. `roles/aiplatform.admin` es el rol que la documentación de BigQuery ML exige para registrar modelos en el Model Registry. En producción usarías una cuenta de servicio dedicada con permisos mínimos. Aquí priorizamos que el taller funcione.

### 0.3. Entorno Python

Igual que en el [Módulo 05](../05-elt-dataflow-iceberg/lab.md), el entorno virtual va en `/tmp` para no agotar los 5 GB del `$HOME` de Cloud Shell:

```bash
python3 -m venv /tmp/lab07-venv
source /tmp/lab07-venv/bin/activate
pip install --quiet --no-cache-dir \
  "google-cloud-aiplatform==2.3.0" \
  "google-cloud-bigquery==3.46.0" \
  "kfp==2.17.0"

mkdir -p ~/lab07 && cd ~/lab07
```

> [!IMPORTANT]
> Todos los pasos siguientes asumen que estás en `~/lab07`, con el entorno virtual activo y las variables del Paso 0.1 exportadas. Si se reinicia Cloud Shell, vuelve a correr los `export` del 0.1 y `source /tmp/lab07-venv/bin/activate`. Si la VM de Cloud Shell cambió y `/tmp/lab07-venv` ya no existe, repite el 0.3.

---

## Paso 1 — Datos: la Capa Silver (10 min)

En un caso real, esta tabla saldría del pipeline Silver de los Módulos 01 o 05. Aquí la **generamos sintéticamente** por una razón pedagógica: necesitamos poder **inyectar drift a propósito** en el Paso 5. En un taller de 2 horas no podemos esperar meses a que los datos cambien solos.

El generador tiene tres perfiles: `historico` (los datos de entrenamiento), `sano` (un lote nuevo con la misma distribución) y `drift` (un lote nuevo donde cambiaron tanto los datos como la relación entre los datos y el impago).

```bash
cat <<'EOF' > generar_lote.py
"""Genera un lote sintético de solicitudes de crédito en fintech_silver.

Perfiles:
  historico -> 5000 filas, distribución "normal" (datos de entrenamiento de v1)
  sano      -> 2000 filas, misma distribución (no debería disparar drift)
  drift     -> 3000 filas: ingresos 35% más bajos, más endeudamiento (data drift)
               y el plazo pasa a pesar más que el score en el impago (concept drift)
"""
import argparse
import os

from google.cloud import bigquery

PERFILES = {
    "historico": dict(tabla="solicitudes_historicas", n=5000, factor_ingreso=1.0, delta_ratio=0.0,
                      intercepto=-3.0, coef_score=-0.006, coef_plazo=0.01,
                      ts="TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL CAST(365 * RAND() AS INT64) DAY)"),
    "sano": dict(tabla="solicitudes_lote_sano", n=2000, factor_ingreso=1.0, delta_ratio=0.0,
                 intercepto=-3.0, coef_score=-0.006, coef_plazo=0.01, ts="CURRENT_TIMESTAMP()"),
    "drift": dict(tabla="solicitudes_lote_drift", n=3000, factor_ingreso=0.65, delta_ratio=0.15,
                  intercepto=-5.0, coef_score=-0.001, coef_plazo=0.08, ts="CURRENT_TIMESTAMP()"),
}

SQL = """
CREATE OR REPLACE TABLE `{project}.fintech_silver.{tabla}` AS
WITH base AS (
  SELECT
    FORMAT('{prefijo}-%06d', n) AS solicitud_id,
    ROUND((2.0 + 6.0 * RAND() + 2.0 * RAND()) * {factor_ingreso}, 2) AS ingreso_mensual_m,
    ROUND(LEAST(0.95, 0.05 + 0.6 * RAND() + {delta_ratio}), 3) AS ratio_deuda_ingreso,
    CAST(450 + 400 * RAND() AS INT64) AS score_crediticio,
    12 * CAST(1 + FLOOR(4 * RAND()) AS INT64) AS plazo_meses,
    {ts} AS feature_timestamp,
    RAND() AS u
  FROM UNNEST(GENERATE_ARRAY(1, {n})) AS n
)
SELECT
  * EXCEPT (u),
  -- "Verdad oculta" del generador: probabilidad logística de impago
  u < 1 / (1 + EXP(-({intercepto}
        + 4.0 * ratio_deuda_ingreso
        + {coef_score} * (score_crediticio - 650)
        - 0.15 * (ingreso_mensual_m - 6)
        + {coef_plazo} * plazo_meses))) AS incumplio
FROM base
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--perfil", choices=PERFILES, required=True)
    args = parser.parse_args()

    p = PERFILES[args.perfil]
    project = os.environ["PROJECT_ID"]
    client = bigquery.Client(project=project, location=os.environ["REGION"])
    client.query(SQL.format(project=project, prefijo=args.perfil.upper(), **p)).result()

    resumen = list(client.query(f"""
      SELECT COUNT(*) AS filas, ROUND(AVG(ingreso_mensual_m), 2) AS ingreso_prom,
             ROUND(AVG(ratio_deuda_ingreso), 3) AS ratio_prom,
             ROUND(AVG(CAST(incumplio AS INT64)), 3) AS tasa_impago
      FROM `{project}.fintech_silver.{p['tabla']}`""").result())[0]
    print(f"{p['tabla']}: {dict(resumen.items())}")
EOF

python3 generar_lote.py --perfil historico
```

Deberías ver unas 5000 filas, ingreso promedio ~6 (millones), y una tasa de impago cercana al 25%.

> [!NOTE]
> La columna `incumplio` es la **etiqueta** (*label*): lo que el modelo aprende a predecir. En la vida real esta etiqueta llega **con retraso**: sabes si alguien cayó en impago meses después de aprobarle el crédito. Esta es la razón de fondo por la que el *concept drift* es más difícil de detectar que el *data drift* (ver [teoría §6](teoria.md#6-drift-por-qué-los-modelos-se-degradan-solos)).

---

## Paso 2 — Feature Store: Registrar las Features (10 min)

El **Feature Store** de la plataforma no copia tus datos a otro lado. Registra una tabla o vista de BigQuery como **Feature Group** y declara cuáles de sus columnas son features. El *offline store* **es** BigQuery. Lo que ganas es un catálogo central de features con dueño y descripción, del que entrenamiento y serving leen la **misma definición** (ver [teoría §4.1](teoria.md#41-feature-store-una-sola-definición-para-entrenar-y-servir)).

```bash
cat <<'EOF' > registrar_features.py
"""Registra la tabla Silver como Feature Group (Feature Store, solo offline)."""
import os

from google.cloud import aiplatform
from vertexai.resources.preview import feature_store

PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["REGION"]
aiplatform.init(project=PROJECT_ID, location=REGION)

grupo = feature_store.FeatureGroup.create(
    name="fintech_solicitudes",
    source=feature_store.FeatureGroupBigQuerySource(
        uri=f"bq://{PROJECT_ID}.fintech_silver.solicitudes_historicas",
        entity_id_columns=["solicitud_id"],
    ),
    description="Features de riesgo de crédito de FinTechCo (capa Silver)",
)

FEATURES = {
    "ingreso_mensual_m": "Ingreso mensual del solicitante, en millones",
    "ratio_deuda_ingreso": "Deuda total / ingreso mensual",
    "score_crediticio": "Score de buró (450-850)",
    "plazo_meses": "Plazo solicitado en meses",
}
for nombre, descripcion in FEATURES.items():
    grupo.create_feature(name=nombre, description=descripcion)
    print(f"Feature registrada: {nombre}")

print(f"Feature Group listo: {grupo.resource_name}")
EOF

python3 registrar_features.py
```

Verifícalo en la consola: **Agent Platform → Models → Feature Store → Feature Registry**. Deberías ver el grupo `fintech_solicitudes` con sus 4 features, la tabla de BigQuery que les sirve de fuente y la llave `solicitud_id`.

> [!IMPORTANT]
> **Este paso no copia ni un solo dato.** Si buscas "dónde quedaron las filas", siguen en `fintech_silver.solicitudes_historicas`. El Feature Group es un **registro de metadatos**: qué tabla es la fuente, cuál es la llave de la entidad y qué columnas son features, con su descripción. Por eso:
>
> - **Para entrenar o reentrenar, se lee directo de BigQuery** con SQL, como hacen el Paso 3 y el pipeline. El registry no es otro camino para leer los datos: te dice **cuál es la definición oficial** de cada feature, para que todos los modelos usen la misma.
> - **El Feature Store solo "se llena" si creas un online store.** Un *online store* con una *feature view* sí copia (sincroniza) los valores más recientes desde BigQuery a un almacenamiento optimizado para buscar por llave en milisegundos. Hace falta cuando, al momento de predecir, el que pide la predicción **no tiene** las features. Por ejemplo, la app solo conoce el `cliente_id`, y features como "pagos atrasados en los últimos 12 meses" están calculadas en la plataforma de datos y hay que buscarlas rápido. Consultar BigQuery tarda segundos; el online store, milisegundos.
> - **En este lab no hace falta online store:** las 4 features vienen en la propia solicitud (ingreso, deuda, score y plazo los llena el cliente en el formulario), así que `predecir.py` se las manda directo al endpoint. Además, el online store cobra por hora, como un endpoint.
>
> Más detalle en [teoría §4.1](teoria.md#41-feature-store-una-sola-definición-para-entrenar-y-servir).

> [!NOTE]
> Fíjate en el import: `from vertexai.resources.preview import feature_store`. Esta API del SDK está en el módulo `preview`, lo que significa que puede cambiar entre versiones del SDK. Por eso el Paso 0.3 fija `google-cloud-aiplatform==2.3.0`, la versión con la que se verificó este lab.

---

## Paso 3 — Experimentos, Entrenar v1 y Registrarla (25 min)

### 3.1. Experimentos: probar candidatos, registrarlos y compararlos

Antes de decidir qué modelo va a producción, un equipo de ciencia de datos prueba varias alternativas: otro algoritmo, otras features, otra regularización. El **seguimiento de experimentos** registra cada intento como un *run*, con sus **parámetros** (qué se probó) y sus **métricas** (cómo le fue), para compararlos con evidencia en vez de memoria (ver [teoría §4.2](teoria.md#42-seguimiento-de-experimentos)).

Vamos a probar 4 candidatos, todos entrenados con el mismo 80% de los datos y evaluados sobre **el mismo 20% que ninguno vio**:

| Run | Qué prueba |
|---|---|
| `logistica-base` | Regresión logística con las 4 features |
| `logistica-l2` | La misma, con regularización L2 (penaliza coeficientes grandes) |
| `logistica-sin-plazo` | Sin `plazo_meses`: ¿esa feature aporta algo? |
| `arbol-boosted` | Otro algoritmo: árboles de decisión con *boosting* (XGBoost) |

```bash
cat <<'EOF' > experimentos.py
"""Entrena 4 modelos candidatos con BigQuery ML, registra cada uno como un run del
experimento (parámetros + métricas) y los compara. Los candidatos NO van al Model
Registry: son pruebas. Solo el elegido se registra, en el Paso 3.2."""
import os
import time

from google.cloud import aiplatform, bigquery

PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["REGION"]
EXPERIMENTO = "fintech-credit-experimentos"
TODAS = ["ingreso_mensual_m", "ratio_deuda_ingreso", "score_crediticio", "plazo_meses"]

CANDIDATOS = {
    "logistica-base": dict(model_type="LOGISTIC_REG", features=TODAS, extra={}),
    "logistica-l2": dict(model_type="LOGISTIC_REG", features=TODAS, extra={"l2_reg": 1.0}),
    "logistica-sin-plazo": dict(model_type="LOGISTIC_REG", features=TODAS[:3], extra={}),
    "arbol-boosted": dict(model_type="BOOSTED_TREE_CLASSIFIER", features=TODAS,
                          extra={"max_iterations": 20}),
}
# Mismo corte que usa el pipeline: el 80% entrena, el 20% (mod 5 = 0) solo evalúa
FILTRO_TRAIN = "MOD(ABS(FARM_FINGERPRINT(solicitud_id)), 5) != 0"
FILTRO_HOLDOUT = "MOD(ABS(FARM_FINGERPRINT(solicitud_id)), 5) = 0"

bq = bigquery.Client(project=PROJECT_ID, location=REGION)
# experiment_tensorboard=False: sin esto, la plataforma crea una instancia de TensorBoard
# (que cobra almacenamiento) para métricas de series de tiempo que aquí no usamos
aiplatform.init(project=PROJECT_ID, location=REGION, experiment=EXPERIMENTO,
                experiment_description="Candidatos para el modelo de riesgo de FinTechCo",
                experiment_tensorboard=False)

for nombre, c in CANDIDATOS.items():
    columnas = ", ".join(c["features"])
    modelo = f"{PROJECT_ID}.fintech_ml.exp_{nombre.replace('-', '_')}"
    opciones = "".join(f"{k} = {v}, " for k, v in c["extra"].items())

    # El sufijo de tiempo hace único cada run: volver a correr el script agrega runs nuevos
    with aiplatform.start_run(f"{nombre}-{int(time.time())}"):
        aiplatform.log_params({"model_type": c["model_type"], "features": columnas,
                               "n_features": len(c["features"]),
                               **{k: str(v) for k, v in c["extra"].items()}})
        print(f"Entrenando {nombre}...")
        bq.query(f"""
        CREATE OR REPLACE MODEL `{modelo}`
        OPTIONS ({opciones}model_type = '{c["model_type"]}', input_label_cols = ['incumplio'])
        AS SELECT {columnas}, incumplio
        FROM `{PROJECT_ID}.fintech_silver.solicitudes_historicas` WHERE {FILTRO_TRAIN}
        """).result()

        fila = list(bq.query(f"""
        SELECT roc_auc, precision, recall, f1_score, log_loss
        FROM ML.EVALUATE(MODEL `{modelo}`, (
          SELECT {columnas}, incumplio
          FROM `{PROJECT_ID}.fintech_silver.solicitudes_historicas` WHERE {FILTRO_HOLDOUT}))
        """).result())[0]
        aiplatform.log_metrics({k: round(float(v), 4) for k, v in fila.items()})

print("\nComparación de runs del experimento (mejor AUC primero):")
print(f"{'run':<36}{'modelo':<26}{'feat':>5}{'roc_auc':>9}{'log_loss':>10}")
runs = aiplatform.ExperimentRun.list(experiment=EXPERIMENTO)
for run in sorted(runs, key=lambda r: r.get_metrics().get("roc_auc", 0), reverse=True):
    p, m = run.get_params(), run.get_metrics()
    print(f"{run.name:<36}{p.get('model_type', ''):<26}{p.get('n_features', ''):>5}"
          f"{m.get('roc_auc', 0):>9.4f}{m.get('log_loss', 0):>10.4f}")
EOF

python3 experimentos.py
```

Entrenar los 4 candidatos tarda unos minutos; el árbol es el más lento. Al final verás la tabla de runs ordenada por AUC. Compárala también en la consola: en **Agent Platform**, abre **Experiments** → `fintech-credit-experimentos`, selecciona los runs y pulsa **Compare** para ver parámetros y métricas lado a lado.

Cómo leer la comparación:
- **Las tres logísticas deberían quedar muy parejas.** Si `logistica-sin-plazo` saca casi el mismo AUC, el plazo aporta poco *hoy*. Guárdalo en mente: en el lote con drift del Paso 5, el plazo pasa a pesar mucho, y un modelo que lo hubiera descartado sufriría más.
- **El árbol puede ganar o perder por poco.** Los datos sintéticos siguen una relación logística, así que un modelo más complejo no tiene mucho que descubrir.
- **Elegir no es solo mirar el AUC más alto.** Con diferencias de milésimas, gana el modelo más simple de explicar. En crédito eso pesa: un regulador puede pedir que se justifique por qué se rechazó a alguien, y los coeficientes de una regresión logística se explican solos.

Con estos datos, lo esperable es que **`logistica-base`** quede arriba o empatada con las mejores. Si es así, la elegimos: usa todas las features y es la más fácil de explicar. (Si en tu corrida otro candidato gana con claridad, es una buena discusión para el grupo: ¿lo cambiarías, sabiendo lo que pierdes en explicabilidad?) Esa decisión, con la evidencia de los runs, es la que llevamos a producción en el 3.2.

> [!NOTE]
> **Experimento ≠ Model Registry.** Los 4 candidatos quedan en el experimento, pero **ninguno** se registra en el Model Registry: el registry es para los modelos que aspiran a producción, no para cada prueba. Por eso estos `CREATE MODEL` no llevan `model_registry = 'VERTEX_AI'`. El experimento responde *"¿qué probamos y por qué elegimos esto?"*; el registry, *"¿qué versión está en producción?"*.

### 3.2. Entrenar v1 y registrarla

Ahora entrenamos el candidato elegido con **todos** los datos históricos y lo registramos como v1. Ejecuta en el editor de BigQuery (o con `bq query --use_legacy_sql=false`):

```sql
CREATE OR REPLACE MODEL `fintech_ml.credit_model_v1`
OPTIONS (
  model_type = 'LOGISTIC_REG',
  input_label_cols = ['incumplio'],
  model_registry = 'VERTEX_AI',
  vertex_ai_model_id = 'fintech_credit_risk',
  vertex_ai_model_version_aliases = ['v1', 'champion']
) AS
SELECT ingreso_mensual_m, ratio_deuda_ingreso, score_crediticio, plazo_meses, incumplio
FROM `fintech_silver.solicitudes_historicas`;
```

Tres opciones hacen el trabajo de MLOps:
- `model_registry = 'VERTEX_AI'` registra el modelo en el **Model Registry** automáticamente al terminar el entrenamiento.
- `vertex_ai_model_id = 'fintech_credit_risk'` es el **nombre del modelo en el registry**. Todos los modelos futuros que usen este mismo ID (con un nombre BigQuery ML distinto) se registran como **versiones nuevas** del mismo modelo. Así va a crear el pipeline la v2.
- `vertex_ai_model_version_aliases = ['v1', 'champion']` le pone dos alias a esta versión: `v1`, que usaremos para desplegarla, y `champion`, que marca **cuál es el modelo en producción**. Cuando el pipeline promueva una versión nueva, le moverá el alias `champion`.

Registra también a la v1 como champion en una tabla de historial. El pipeline la consulta para saber **contra qué modelo comparar** y **contra qué datos medir el drift**:

```sql
CREATE OR REPLACE TABLE `fintech_ml.champion_historial` AS
SELECT
  '1' AS version_id,
  CONCAT(@@project_id, '.fintech_ml.credit_model_v1') AS modelo_bqml,
  CONCAT(@@project_id, '.fintech_silver.solicitudes_historicas') AS tabla_entrenamiento,
  CURRENT_TIMESTAMP() AS fecha;
```

> [!NOTE]
> ¿Por qué una tabla si ya existe el alias `champion` en el registry? El alias dice **cuál versión** es la champion, pero `ML.EVALUATE` necesita el **nombre del modelo en BigQuery ML** (`credit_model_v1`), y el PSI necesita saber **con qué datos se entrenó**. Esa tabla guarda los tres datos y, de paso, deja un historial auditable de cada promoción.

> [!WARNING]
> Si vuelves a ejecutar `CREATE OR REPLACE MODEL` **con el mismo nombre BigQuery ML** (`credit_model_v1`), BigQuery **reemplaza** la versión existente en el registry en vez de crear una nueva. Para tener v1, v2, v3... cada versión necesita un nombre BigQuery ML distinto con el mismo `vertex_ai_model_id`. El pipeline del Paso 6 lo hace usando el ID de la ejecución en el nombre.

Evalúa el modelo:

```sql
SELECT precision, recall, accuracy, roc_auc
FROM ML.EVALUATE(MODEL `fintech_ml.credit_model_v1`);
```

Verifícalo en la consola: **Agent Platform → Models → Model Registry** → `fintech_credit_risk` → versión 1 con los alias `v1` y `champion`.

---

## Paso 4 — Endpoint: Desplegar v1 y Predecir (20 min)

```bash
cat <<'EOF' > desplegar_v1.py
"""Crea (o reutiliza) el endpoint y despliega la versión v1 del modelo (alias 'v1')."""
import os

from google.cloud import aiplatform
from google.cloud.aiplatform_v1 import EndpointServiceClient
from google.cloud.aiplatform_v1.types import DedicatedResources, DeployedModel, MachineSpec

PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["REGION"]
aiplatform.init(project=PROJECT_ID, location=REGION)

existentes = aiplatform.Endpoint.list(filter='display_name="fintech-credit-endpoint"')
endpoint = existentes[0] if existentes else aiplatform.Endpoint.create(
    display_name="fintech-credit-endpoint")
modelo = aiplatform.Model(model_name="fintech_credit_risk@v1")

# Se usa la API de bajo nivel porque el SDK de alto nivel (modelo.deploy) no permite
# desactivar las explicaciones, y con explicaciones activas el despliegue de modelos
# de BigQuery ML falla (bug conocido; ver la nota debajo de este bloque).
cliente = EndpointServiceClient(client_options={"api_endpoint": f"{REGION}-aiplatform.googleapis.com"})
operacion = cliente.deploy_model(
    endpoint=endpoint.resource_name,
    deployed_model=DeployedModel(
        model=modelo.versioned_resource_name,
        display_name="fintech-credit-v1",
        disable_explanations=True,
        dedicated_resources=DedicatedResources(
            machine_spec=MachineSpec(machine_type="n1-standard-2"),
            min_replica_count=1,
            max_replica_count=1,
        ),
    ),
    traffic_split={"0": 100},  # "0" = el modelo que se está desplegando: recibe el 100% del tráfico
)
print("Desplegando (tarda 10-20 minutos)...")
operacion.result(timeout=3600)
print(f"Endpoint listo: {endpoint.resource_name}")
EOF

python3 desplegar_v1.py
```

> [!NOTE]
> **El despliegue tarda entre 10 y 20 minutos**: la plataforma aprovisiona la máquina y carga el modelo. Mientras esperas, lee la [teoría §4](teoria.md#4-los-componentes-de-una-plataforma-de-mlops) o adelanta el Paso 5 en otra pestaña de Cloud Shell. Un modelo de BigQuery ML registrado se despliega **sin contenedor propio**: la plataforma se encarga de servirlo.

> [!WARNING]
> **Por qué `disable_explanations=True` y la API de bajo nivel.** Si despliegas un modelo de BigQuery ML con el SDK de alto nivel (`modelo.deploy(...)`), el despliegue falla con:
> `400 Error occurred in Explanation preprocessing ... NodeDef mentions attr 'debug_name' not in Op<name=VarHandleOp ...>`.
> Es un **bug conocido de la plataforma** desde 2024 ([issue #2723 en vertex-ai-samples](https://github.com/GoogleCloudPlatform/vertex-ai-samples/issues/2723), [issue tracker 337998773](https://issuetracker.google.com/issues/337998773)). Al desplegar con explicaciones activas (*explainable AI*), la plataforma intenta leer el modelo exportado por BigQuery ML con una versión de TensorFlow más vieja que la que lo generó. Desplegar **desde la consola** funciona, porque no activa las explicaciones. Por eso el script usa la API de bajo nivel con `disable_explanations=True`: es lo mismo que hace la consola.
>
> Si en un intento anterior el script ya había creado el endpoint, esta versión lo **reutiliza** en vez de crear uno duplicado.
>
> **Si falla con `500 System error. Please try this operation again`**, es un error transitorio de la plataforma: vuelve a correr `python3 desplegar_v1.py`. Ojo con `Ctrl+C`: solo corta la espera en tu terminal, pero el despliegue **sigue corriendo** en Google. Antes de reintentar, revisa en la consola (**Agent Platform → Models → Online prediction → `fintech-credit-endpoint`**) que no haya un despliegue en curso, para no lanzar dos a la vez.

Cuando termine, envía dos solicitudes de prueba, una de bajo riesgo y una de alto riesgo:

```bash
cat <<'EOF' > predecir.py
"""Envía dos solicitudes de ejemplo al endpoint y muestra la predicción."""
import os

from google.cloud import aiplatform

aiplatform.init(project=os.environ["PROJECT_ID"], location=os.environ["REGION"])
endpoint = aiplatform.Endpoint.list(filter='display_name="fintech-credit-endpoint"')[0]

solicitudes = [
    {"ingreso_mensual_m": 9.5, "ratio_deuda_ingreso": 0.15, "score_crediticio": 800, "plazo_meses": 12},
    {"ingreso_mensual_m": 2.5, "ratio_deuda_ingreso": 0.70, "score_crediticio": 480, "plazo_meses": 48},
]
respuesta = endpoint.predict(instances=solicitudes)
print(f"Versión que respondió: {respuesta.model_version_id}")
for solicitud, prediccion in zip(solicitudes, respuesta.predictions):
    print(solicitud, "->", prediccion)
EOF

python3 predecir.py
```

Cada predicción trae la clase predicha y la probabilidad de cada clase. La primera solicitud debería salir con baja probabilidad de impago y la segunda con alta. Fíjate en la línea `Versión que respondió`: ahora es la `1`. La vas a volver a revisar en el Paso 7.

---

## Paso 5 — Drift: Lotes Nuevos (15 min)

Pasa el tiempo y llegan solicitudes nuevas. Generamos dos escenarios:

```bash
python3 generar_lote.py --perfil sano
python3 generar_lote.py --perfil drift
```

Compara los promedios en BigQuery:

```sql
SELECT 'historico' AS lote, AVG(ingreso_mensual_m) AS ingreso, AVG(ratio_deuda_ingreso) AS ratio,
       AVG(CAST(incumplio AS INT64)) AS tasa_impago FROM `fintech_silver.solicitudes_historicas`
UNION ALL
SELECT 'sano', AVG(ingreso_mensual_m), AVG(ratio_deuda_ingreso), AVG(CAST(incumplio AS INT64))
FROM `fintech_silver.solicitudes_lote_sano`
UNION ALL
SELECT 'drift', AVG(ingreso_mensual_m), AVG(ratio_deuda_ingreso), AVG(CAST(incumplio AS INT64))
FROM `fintech_silver.solicitudes_lote_drift`;
```

El lote `sano` se parece al histórico. El lote `drift` simula una crisis económica: ingresos más bajos, más endeudamiento y mucho más impago. Además, en el lote con drift **cambió la relación** entre las features y el impago: el plazo ahora pesa mucho más y el score casi nada. Eso es *concept drift*, y es lo que hace que el modelo v1 se equivoque más aunque sus entradas sigan siendo válidas.

### 5.1. ¿Cómo detecta el pipeline el drift? El PSI con un ejemplo

Comparar promedios sirve para mirar, pero el pipeline necesita **un número** para decidir si reentrena. Ese número es el **PSI** (*Population Stability Index*), y responde una sola pregunta: **¿los clientes nuevos se parecen a los clientes con los que entrenamos el modelo?**

Para responderla, **agrupa a los clientes en cajones** y compara cuántos caen en cada cajón antes y ahora. Un ejemplo con solo la feature "ingreso" y 3 cajones:

**1. Con los datos de entrenamiento se arman los cajones**, cortados para que en cada uno caiga la misma cantidad de clientes:

| Cajón | Ingreso | % de clientes de entrenamiento |
|---|---|---|
| Bajo | menos de 3 millones | 33% |
| Medio | de 3 a 6 millones | 33% |
| Alto | más de 6 millones | 33% |

**2. El lote nuevo se mete en los mismos cajones**, con los mismos cortes de 3 y 6 millones.

| Cajón | Antes | Lote sano | Lote con drift (crisis) |
|---|---|---|---|
| Bajo | 33% | 35% | **70%** |
| Medio | 33% | 32% | 25% |
| Alto | 33% | 33% | **5%** |

En el lote sano los clientes se reparten casi igual. En el lote con drift se amontonaron en el cajón "bajo".

**3. Se convierte en un número.** Para cada cajón se mide cuánto cambió, con `(ahora − antes) × ln(ahora / antes)`, y se suman los resultados. Para el lote con drift:

| Cajón | Antes | Ahora | Cuánto aporta |
|---|---|---|---|
| Bajo | 0.33 | 0.70 | (0.70 − 0.33) × ln(0.70 / 0.33) = 0.37 × 0.75 = **0.28** |
| Medio | 0.33 | 0.25 | (0.25 − 0.33) × ln(0.25 / 0.33) = −0.08 × −0.28 = **0.02** |
| Alto | 0.33 | 0.05 | (0.05 − 0.33) × ln(0.05 / 0.33) = −0.28 × −1.89 = **0.53** |
| | | **PSI** | **0.83** |

No hace falta memorizar la fórmula. Lo importante:
- Si un cajón **no cambia**, aporta 0. Por eso el lote sano da un PSI cercano a 0.
- Si un cajón **cambia mucho**, aporta mucho, tanto si se llena como si se vacía. Por eso el PSI nunca resta.

| PSI | Lectura |
|---|---|
| Menor a 0.1 | Nada relevante cambió |
| De 0.1 a 0.2 | Cambio moderado, vigilar |
| Mayor a 0.2 | Cambió bastante: el umbral que usa el pipeline para reentrenar |

En el pipeline del Paso 6, el componente `1-calcular-drift-psi` hace exactamente esto, pero con **10 cajones** en vez de 3, y para las **4 features**. Se queda con el PSI más alto de las cuatro, porque basta con que una cambie mucho para que el modelo pueda equivocarse. Lo calcula en SQL: `APPROX_QUANTILES` arma los cortes con los datos del champion, y `RANGE_BUCKET` decide a qué cajón va cada fila.

> [!NOTE]
> El PSI mira si cambiaron las **entradas** (*data drift*). No ve si cambió la relación entre las entradas y el impago (*concept drift*). Por eso el pipeline tiene un segundo filtro: el challenger solo se despliega si le gana en AUC al champion. Más detalle en [teoría §6.2](teoria.md#62-population-stability-index-psi).

---

## Paso 6 — Pipeline de Reentrenamiento: Construirlo y Correrlo con el Lote Sano (15 min)

El pipeline usa **Kubeflow Pipelines (KFP)**. Escribes el flujo en Python con el SDK de KFP, y la plataforma lo ejecuta de forma **serverless**: no hay que administrar ningún cluster de Kubernetes. Cada `@dsl.component` corre en su propio contenedor. `dsl.If` crea las ramas condicionales que funcionan como *quality gates*.

```bash
cat <<'EOF' > pipeline.py
"""Pipeline de reentrenamiento continuo: detecta drift (PSI), reentrena con BigQuery ML,
compara challenger vs. champion y solo despliega si el nuevo modelo es mejor."""
from typing import NamedTuple

from kfp import dsl

PKGS_BQ = ["google-cloud-bigquery==3.46.0"]
PKGS_AIP = ["google-cloud-aiplatform==2.3.0", "google-cloud-bigquery==3.46.0"]


@dsl.component(base_image="python:3.11", packages_to_install=PKGS_BQ)
def leer_champion(project: str, location: str) -> NamedTuple(
        "Champion", [("modelo_bqml", str), ("tabla_entrenamiento", str)]):
    from collections import namedtuple

    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    fila = list(client.query(f"""
    SELECT version_id, modelo_bqml, tabla_entrenamiento
    FROM `{project}.fintech_ml.champion_historial`
    ORDER BY fecha DESC LIMIT 1
    """).result())[0]
    print(f"Champion actual: versión {fila.version_id} ({fila.modelo_bqml}), "
          f"entrenada con {fila.tabla_entrenamiento}")
    Champion = namedtuple("Champion", ["modelo_bqml", "tabla_entrenamiento"])
    return Champion(fila.modelo_bqml, fila.tabla_entrenamiento)


@dsl.component(base_image="python:3.11", packages_to_install=PKGS_BQ)
def calcular_psi(project: str, location: str, tabla_base: str, tabla_actual: str) -> float:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    sql_psi = """
    WITH q AS (
      SELECT APPROX_QUANTILES({f}, 10) AS qs FROM `{base}`
    ),
    cortes AS (
      SELECT ARRAY(
        SELECT DISTINCT x FROM UNNEST(qs) AS x WITH OFFSET o
        WHERE o BETWEEN 1 AND 9 ORDER BY x) AS c
      FROM q
    ),
    dist_base AS (
      SELECT RANGE_BUCKET(t.{f}, cortes.c) AS bin, COUNT(*) / SUM(COUNT(*)) OVER () AS p
      FROM `{base}` t CROSS JOIN cortes GROUP BY bin
    ),
    dist_actual AS (
      SELECT RANGE_BUCKET(t.{f}, cortes.c) AS bin, COUNT(*) / SUM(COUNT(*)) OVER () AS p
      FROM `{actual}` t CROSS JOIN cortes GROUP BY bin
    )
    SELECT SUM(
      (IFNULL(a.p, 0.0001) - IFNULL(b.p, 0.0001)) * LN(IFNULL(a.p, 0.0001) / IFNULL(b.p, 0.0001))
    ) AS psi
    FROM dist_base b FULL OUTER JOIN dist_actual a USING (bin)
    """
    psi_max = 0.0
    for f in ["ingreso_mensual_m", "ratio_deuda_ingreso", "score_crediticio", "plazo_meses"]:
        fila = list(client.query(sql_psi.format(f=f, base=tabla_base, actual=tabla_actual)).result())[0]
        print(f"PSI {f}: {fila.psi:.4f}")
        psi_max = max(psi_max, fila.psi)
    print(f"PSI maximo: {psi_max:.4f}")
    return psi_max


@dsl.component(base_image="python:3.11", packages_to_install=PKGS_BQ)
def reentrenar(project: str, location: str, tabla_actual: str, vertex_model_id: str,
               run_id: str) -> str:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    modelo = f"{project}.fintech_ml.credit_model_{run_id.replace('-', '_')}"
    # Ventana deslizante: se entrena solo con el lote reciente (80% de las filas);
    # el 20% restante (FARM_FINGERPRINT mod 5 = 0) se reserva para evaluar.
    client.query(f"""
    CREATE OR REPLACE MODEL `{modelo}`
    OPTIONS (
      model_type = 'LOGISTIC_REG',
      input_label_cols = ['incumplio'],
      model_registry = 'VERTEX_AI',
      vertex_ai_model_id = '{vertex_model_id}'
    ) AS
    SELECT ingreso_mensual_m, ratio_deuda_ingreso, score_crediticio, plazo_meses, incumplio
    FROM `{tabla_actual}`
    WHERE MOD(ABS(FARM_FINGERPRINT(solicitud_id)), 5) != 0
    """).result()
    print(f"Modelo entrenado y registrado: {modelo}")
    return modelo


@dsl.component(base_image="python:3.11", packages_to_install=PKGS_BQ)
def evaluar_auc(project: str, location: str, modelo: str, tabla_actual: str) -> float:
    from google.cloud import bigquery

    client = bigquery.Client(project=project, location=location)
    fila = list(client.query(f"""
    SELECT roc_auc FROM ML.EVALUATE(MODEL `{modelo}`, (
      SELECT ingreso_mensual_m, ratio_deuda_ingreso, score_crediticio, plazo_meses, incumplio
      FROM `{tabla_actual}`
      WHERE MOD(ABS(FARM_FINGERPRINT(solicitud_id)), 5) = 0
    ))
    """).result())[0]
    print(f"AUC de {modelo} sobre el holdout del lote actual: {fila.roc_auc:.4f}")
    return float(fila.roc_auc)


@dsl.component(base_image="python:3.11", packages_to_install=PKGS_AIP)
def promover_challenger(project: str, location: str, vertex_model_id: str,
                        endpoint_display_name: str, modelo_bqml: str,
                        tabla_entrenamiento: str) -> str:
    from google.cloud import aiplatform, bigquery
    from google.cloud.aiplatform import ModelRegistry
    from google.cloud.aiplatform_v1 import EndpointServiceClient
    from google.cloud.aiplatform_v1.types import DedicatedResources, DeployedModel, MachineSpec

    aiplatform.init(project=project, location=location)
    registry = ModelRegistry(model=vertex_model_id)
    ultima = max(registry.list_versions(), key=lambda v: int(v.version_id))
    modelo = registry.get_model(version=ultima.version_id)

    endpoint = aiplatform.Endpoint.list(filter=f'display_name="{endpoint_display_name}"')[0]
    anteriores = [m.id for m in endpoint.list_models()]

    # API de bajo nivel con disable_explanations=True: con explicaciones activas, el
    # despliegue de modelos de BigQuery ML falla (ver la advertencia del Paso 4).
    cliente = EndpointServiceClient(
        client_options={"api_endpoint": f"{location}-aiplatform.googleapis.com"})
    cliente.deploy_model(
        endpoint=endpoint.resource_name,
        deployed_model=DeployedModel(
            model=modelo.versioned_resource_name,
            display_name=f"fintech-credit-v{ultima.version_id}",
            disable_explanations=True,
            dedicated_resources=DedicatedResources(
                machine_spec=MachineSpec(machine_type="n1-standard-2"),
                min_replica_count=1,
                max_replica_count=1,
            ),
        ),
        traffic_split={"0": 100},  # todo el tráfico a la versión nueva; las anteriores quedan en 0%
    ).result(timeout=3600)
    for deployed_model_id in anteriores:
        endpoint.undeploy(deployed_model_id=deployed_model_id)

    # El challenger pasa a ser el champion: se le mueve el alias (un alias es único
    # dentro del modelo, así que se quita de la versión anterior) y se registra en el
    # historial, que la próxima ejecución leerá en leer_champion.
    registry.add_version_aliases(new_aliases=["champion"], version=ultima.version_id)
    bigquery.Client(project=project, location=location).query(f"""
    INSERT INTO `{project}.fintech_ml.champion_historial`
    VALUES ('{ultima.version_id}', '{modelo_bqml}', '{tabla_entrenamiento}', CURRENT_TIMESTAMP())
    """).result()

    print(f"Desplegada y promovida a champion la versión {ultima.version_id}; "
          f"retiradas: {anteriores}")
    return f"{vertex_model_id}@{ultima.version_id}"


@dsl.pipeline(name="fintech-reentrenamiento")
def pipeline(
    project: str,
    location: str,
    tabla_actual: str,
    vertex_model_id: str = "fintech_credit_risk",
    endpoint_display_name: str = "fintech-credit-endpoint",
    umbral_psi: float = 0.2,
):
    champion = leer_champion(project=project, location=location)
    champion.set_display_name("0-leer-champion")

    # El drift se mide contra los datos con los que se entrenó el champion actual
    psi = calcular_psi(project=project, location=location,
                       tabla_base=champion.outputs["tabla_entrenamiento"],
                       tabla_actual=tabla_actual)
    psi.set_display_name("1-calcular-drift-psi")

    with dsl.If(psi.output > umbral_psi, name="hay-drift"):
        challenger = reentrenar(project=project, location=location, tabla_actual=tabla_actual,
                                vertex_model_id=vertex_model_id,
                                run_id=dsl.PIPELINE_JOB_ID_PLACEHOLDER)
        challenger.set_display_name("2-reentrenar-challenger")

        auc_challenger = evaluar_auc(project=project, location=location,
                                     modelo=challenger.output, tabla_actual=tabla_actual)
        auc_challenger.set_display_name("3a-auc-challenger")

        auc_champion = evaluar_auc(project=project, location=location,
                                   modelo=champion.outputs["modelo_bqml"],
                                   tabla_actual=tabla_actual)
        auc_champion.set_display_name("3b-auc-champion")

        with dsl.If(auc_challenger.output > auc_champion.output, name="challenger-gana"):
            promocion = promover_challenger(project=project, location=location,
                                            vertex_model_id=vertex_model_id,
                                            endpoint_display_name=endpoint_display_name,
                                            modelo_bqml=challenger.output,
                                            tabla_entrenamiento=tabla_actual)
            promocion.set_display_name("4-desplegar-y-promover-challenger")
EOF
```

Ahora el script que **compila** el pipeline y lo **lanza**, ya sea una sola vez o programado:

```bash
cat <<'EOF' > ejecutar_pipeline.py
"""Compila y lanza el pipeline de reentrenamiento (una vez, o programado con cron)."""
import argparse
import os

from google.cloud import aiplatform
from kfp import compiler

from pipeline import pipeline

PROJECT_ID = os.environ["PROJECT_ID"]
REGION = os.environ["REGION"]
BUCKET = os.environ["BUCKET"]

parser = argparse.ArgumentParser()
parser.add_argument("--lote", required=True,
                    help="Tabla del lote nuevo, ej. solicitudes_lote_sano o solicitudes_lote_drift")
parser.add_argument("--cron", help="Si se pasa, crea un schedule en vez de una ejecución única")
args = parser.parse_args()

compiler.Compiler().compile(pipeline, "pipeline.json")
aiplatform.init(project=PROJECT_ID, location=REGION, staging_bucket=BUCKET)

job = aiplatform.PipelineJob(
    display_name="fintech-reentrenamiento",
    template_path="pipeline.json",
    pipeline_root=f"{BUCKET}/pipeline-root",
    parameter_values={
        "project": PROJECT_ID,
        "location": REGION,
        "tabla_actual": f"{PROJECT_ID}.fintech_silver.{args.lote}",
    },
    enable_caching=False,  # con caché, una corrida con los mismos parámetros reutilizaría el PSI viejo
)

if args.cron:
    schedule = job.create_schedule(display_name="fintech-reentrenamiento-programado",
                                   cron=args.cron, max_concurrent_run_count=1)
    print(f"Schedule creado: {schedule.resource_name} (cron: {args.cron})")
else:
    job.submit()
    print(f"Pipeline lanzado: {job._dashboard_uri()}")
EOF
```

Lánzalo con el **lote sano**:

```bash
python3 ejecutar_pipeline.py --lote solicitudes_lote_sano
```

Abre el enlace que imprime, o ve a **Agent Platform → Models → Pipelines**. La primera ejecución tarda unos minutos más porque cada componente descarga su contenedor y sus paquetes. En el grafo vas a ver:

- `0-leer-champion` lee de `champion_historial` cuál es el champion (la v1) y con qué datos se entrenó (`solicitudes_historicas`).
- `1-calcular-drift-psi` compara el lote contra esos datos y termina con un PSI máximo muy bajo, alrededor de 0.01-0.02. Lo ves en los logs del componente.
- La rama `hay-drift` aparece **omitida** (*skipped*): el PSI no superó el umbral de 0.2, así que **no se reentrena nada**.

> [!IMPORTANT]
> Este es el primer *quality gate* funcionando. Un pipeline que reentrena "porque toca, cada lunes" gasta cómputo y arriesga reemplazar un modelo bueno por uno peor sin razón. Este pipeline primero **mide** si el mundo cambió.

---

<img width="951" height="697" alt="Screenshot 2026-10-01 at 10 36 47 p m" src="https://github.com/user-attachments/assets/4ee1e348-f253-4f14-95b2-920a6e950e83" />

---

## Paso 7 — Pipeline con el Lote con Drift (25 min)

```bash
python3 ejecutar_pipeline.py --lote solicitudes_lote_drift
```

Ahora sí recorre todo el camino:

1. `0-leer-champion` y `1-calcular-drift-psi`: el PSI máximo sale **muy por encima** de 0.2. El ingreso y el ratio de deuda cambiaron de distribución.
2. `2-reentrenar-challenger`: entrena un modelo nuevo con el lote reciente y lo registra como una **nueva versión** de `fintech_credit_risk`.
3. `3a`/`3b`: evalúa challenger y champion (la v1, leída del historial) sobre **el mismo holdout** del lote nuevo, el 20% de filas que el challenger nunca vio. El challenger debería sacar un AUC claramente mayor: v1 aprendió que el score importa mucho y el plazo poco, y eso dejó de ser cierto.
4. `4-desplegar-y-promover-challenger`: despliega la nueva versión en el endpoint, retira la v1 y **promueve al challenger a champion**: le mueve el alias `champion` en el registry y agrega una fila a `champion_historial`. **Tarda 10-20 minutos**, igual que el despliegue del Paso 4.

> [!NOTE]
> Comparar sobre el mismo holdout es la clave del segundo gate. Si evaluaras al challenger sobre sus propios datos de entrenamiento, siempre "ganaría". Un AUC más alto en datos que el modelo ya vio no demuestra nada.

Cuando termine, vuelve a predecir:

```bash
python3 predecir.py
```

La línea `Versión que respondió` ya no dice `1`, sino la versión nueva. En **Model Registry → fintech_credit_risk** vas a ver las dos versiones, con el alias `champion` ahora en la nueva. Y en BigQuery:

```sql
SELECT * FROM `fintech_ml.champion_historial` ORDER BY fecha;
```

Ese historial es tu **auditoría**: qué versión estuvo en producción, desde cuándo y con qué datos se entrenó.

### 7.1. El nuevo champion es la nueva referencia

Corre el pipeline **otra vez con el mismo lote con drift**:

```bash
python3 ejecutar_pipeline.py --lote solicitudes_lote_drift
```

Esta vez se detiene en el primer gate: el PSI sale bajo y `hay-drift` queda omitida. ¿Por qué, si es el mismo lote que antes disparó el reentrenamiento? Porque `0-leer-champion` ahora devuelve la versión nueva, entrenada **con ese lote**. El drift se mide contra **los datos del modelo que está en producción**, no contra los de la v1. Para el champion actual, ese lote ya no es un cambio: es su mundo.

> [!IMPORTANT]
> Si el pipeline siguiera comparando contra la v1 y sus datos, cada ejecución detectaría "drift" para siempre y reentrenaría sin parar, y la comparación de AUC enfrentaría a cada challenger con un modelo que ya no está en producción. Leer el champion al inicio de cada ejecución es lo que hace que el ciclo de reentrenamiento **se cierre**.

---

<img width="827" height="696" alt="Screenshot 2026-10-01 at 10 43 44 p m" src="https://github.com/user-attachments/assets/63c928e0-9360-49bb-a6dc-b3b8b5a5d719" />

---

## Paso 8 — Programar el Pipeline con Cron (5 min)

En producción no lanzarías el pipeline a mano. Lo programas:

```bash
python3 ejecutar_pipeline.py --lote solicitudes_lote_sano --cron "0 6 * * *"
```

Esto crea un **schedule** que ejecuta el pipeline todos los días a las 6:00 (UTC). Puedes fijar la zona horaria con el prefijo `TZ=`, por ejemplo `"TZ=America/Bogota 0 6 * * *"`. Lo ves en **Agent Platform → Models → Pipelines → Schedules**.

> [!WARNING]
> El schedule va a seguir lanzando ejecuciones (y cobrándolas) hasta que lo borres. `limpiar.py`, en el Paso 9, lo elimina. En un caso real, el schedule leería "el lote de ayer" con una vista o una tabla particionada por fecha, en vez de una tabla fija como aquí.

---

## Paso 9 — Limpieza (10 min)

> [!IMPORTANT]
> El endpoint factura por hora mientras tenga un modelo desplegado, y el schedule sigue lanzando ejecuciones. Ejecuta todo este paso antes de cerrar la sesión.

```bash
cat <<'EOF' > limpiar.py
"""Borra schedules, endpoint (con sus modelos desplegados), modelos del registry, el experimento y el Feature Group."""
import os

from google.cloud import aiplatform
from vertexai.resources.preview import feature_store

aiplatform.init(project=os.environ["PROJECT_ID"], location=os.environ["REGION"])

for schedule in aiplatform.PipelineJobSchedule.list():
    print(f"Borrando schedule {schedule.display_name}")
    schedule.delete()

for endpoint in aiplatform.Endpoint.list(filter='display_name="fintech-credit-endpoint"'):
    print(f"Borrando endpoint {endpoint.display_name} (y sus modelos desplegados)")
    endpoint.delete(force=True)

for modelo in aiplatform.Model.list(filter='display_name="fintech_credit_risk"'):
    print(f"Borrando modelo del registry {modelo.resource_name}")
    modelo.delete()

try:
    aiplatform.Experiment("fintech-credit-experimentos").delete()
    print("Experimento borrado")
except Exception as e:  # puede no existir si saltaste el Paso 3.1
    print(f"Experimento: {e}")

try:
    feature_store.FeatureGroup("fintech_solicitudes").delete(force=True)
    print("Feature Group borrado")
except Exception as e:  # puede no existir si saltaste el Paso 2
    print(f"Feature Group: {e}")
EOF

python3 limpiar.py

bq rm --recursive --force "${PROJECT_ID}:fintech_ml"
bq rm --recursive --force "${PROJECT_ID}:fintech_silver"
gcloud storage rm --recursive "$BUCKET"

deactivate
```

Verifica en **Agent Platform → Models → Online prediction → Endpoints** que no quede ningún endpoint. Es el recurso que más importa borrar.

> [!NOTE]
> Si `limpiar.py` reporta que no encontró el modelo en el registry, revisa **Model Registry** en la consola. Al borrar los modelos de BigQuery ML (con `bq rm` sobre `fintech_ml`) también se eliminan sus versiones registradas. Si quedó algo, bórralo desde la consola.

---

## Retos Opcionales (Extensión)

### Reto 1: Monitoreo gestionado en vez de PSI casero

La plataforma tiene **Model Monitoring** gestionado, que calcula drift sobre las predicciones del endpoint y envía alertas sin que escribas SQL. Configúralo sobre `fintech-credit-endpoint` usando `solicitudes_historicas` como baseline, envía predicciones con datos del lote con drift, y compara qué detecta frente al PSI del pipeline.

<details>
<summary>👀 Ver Pista de Solución Reto 1</summary>

Model Monitoring está en la consola en **Agent Platform → Models → Monitoring**. Necesita un baseline (los datos de entrenamiento) y tráfico real de predicciones sobre el endpoint: un monitor sin predicciones no tiene nada que comparar. Puedes generar ese tráfico con un bucle sobre `predecir.py` usando filas de `solicitudes_lote_drift`. La diferencia conceptual: el PSI del pipeline mira el **lote nuevo de entrenamiento**, mientras que Model Monitoring mira **lo que el modelo está recibiendo en producción**. Son dos puntos de control distintos y complementarios.

</details>

### Reto 2: Monitoreo de features en el Feature Store

El SDK del Feature Store tiene `FeatureGroup.create_feature_monitor(...)`, que monitorea la distribución de las features directamente sobre el Feature Group. Explora cómo se compara con el PSI del pipeline: ¿qué ventaja tiene monitorear en el Feature Store, que ven todos los modelos que usan esas features, en vez de dentro del pipeline de un solo modelo?

### Reto 3: Cada ejecución del pipeline como un run del experimento

Hoy el experimento solo tiene los 4 candidatos del Paso 3.1. Haz que cada ejecución del pipeline también quede registrada, para comparar en el mismo lugar los reentrenamientos a lo largo del tiempo: el PSI que los disparó y el AUC del challenger contra el del champion.

<details>
<summary>👀 Ver Pista de Solución Reto 3</summary>

Hay dos caminos. El más directo: `job.submit(experiment="fintech-credit-experimentos")` en `ejecutar_pipeline.py` asocia la ejecución al experimento y registra sus parámetros. Para que también aparezcan las métricas, agrega un componente al final del pipeline que reciba el PSI y los dos AUC y los registre con `aiplatform.init(experiment=..., experiment_tensorboard=False)`, `aiplatform.start_run(...)` y `aiplatform.log_metrics(...)`. Ese componente necesita `PKGS_AIP`, y la cuenta de servicio del pipeline ya tiene los permisos.

</details>

---

## Resumen de lo Aprendido

- **Feature Store (offline) no mueve tus datos:** registra columnas de BigQuery como features con dueño y descripción, para que entrenamiento y serving usen la misma definición.
- **Un experimento registra cada intento** (parámetros y métricas) para comparar candidatos con evidencia. Los candidatos quedan en el experimento; solo el elegido va al Model Registry.
- **BigQuery ML + `model_registry='VERTEX_AI'`** lleva un modelo entrenado con SQL directo al Model Registry, con versiones. Versión nueva = mismo `vertex_ai_model_id` + nombre BigQuery ML distinto.
- **Un endpoint factura por hora mientras exista**, aunque nadie lo use. Es el recurso que más importa limpiar.
- **Data drift ≠ concept drift.** El PSI detecta que cambiaron las entradas. Saber si el modelo empeoró requiere etiquetas, y esas llegan tarde.
- **Un buen pipeline de reentrenamiento tiene quality gates**: no reentrena si no hay drift, y no despliega si el challenger no le gana al champion **en el mismo holdout**.
- **Cuando el challenger gana, pasa a ser el champion:** se le mueve el alias `champion`, y desde entonces es la referencia tanto para comparar AUC como para medir drift.
- **Pipelines de la plataforma = Kubeflow Pipelines serverless**: el estándar abierto (KFP) sin administrar Kubernetes, ejecutable a mano o con cron.
- **Vertex AI ahora se llama Gemini Enterprise Agent Platform** (abril 2026), pero la API, el SDK, `gcloud ai` y los roles IAM conservan el nombre "aiplatform".

---

## Referencias

- [Manage BigQuery ML models in the Model Registry](https://docs.cloud.google.com/bigquery/docs/managing-models-vertex)
- [Feature Store — Create a feature group](https://docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/create-featuregroup)
- [Feature Store — Monitor features](https://docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/monitor-features)
- [Schedule a pipeline run with the scheduler API](https://docs.cloud.google.com/vertex-ai/docs/pipelines/schedule-pipeline-run)
- [Gemini Enterprise Agent Platform name changes](https://docs.cloud.google.com/gemini-enterprise-agent-platform/vertex-ai-name-changes)
- [Kubeflow Pipelines SDK (KFP v2)](https://www.kubeflow.org/docs/components/pipelines/)
- [python-aiplatform (código fuente del SDK)](https://github.com/googleapis/python-aiplatform)
