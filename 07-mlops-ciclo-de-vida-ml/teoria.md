# MLOps: Infraestructura para el Ciclo de Vida de Machine Learning en GCP

> **Audiencia Objetivo:** Data Engineers, ML Engineers, Analytics Engineers y Data Architects que ya saben llevar datos a una capa Silver/Gold (Módulos 01-05) y necesitan entender **qué hace falta para que un modelo entrenado sobre esos datos llegue a producción y se mantenga sano**.
>
> **Alcance:** El ciclo de vida de ML de punta a punta: Feature Store, Model Registry, Endpoints de inferencia, pipelines de reentrenamiento continuo, y detección de data drift / concept drift. Todo sobre Google Cloud, con la plataforma que hasta abril de 2026 se llamaba **Vertex AI** y hoy se llama **Gemini Enterprise Agent Platform**.
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 07 — MLOps en GCP](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo: el Modelo es la Parte Pequeña](#1-resumen-ejecutivo-el-modelo-es-la-parte-pequeña)
2. [El Nombre: de Vertex AI a Gemini Enterprise Agent Platform](#2-el-nombre-de-vertex-ai-a-gemini-enterprise-agent-platform)
3. [Las Piezas del Ciclo de Vida](#3-las-piezas-del-ciclo-de-vida)
4. [Niveles de Madurez de MLOps](#4-niveles-de-madurez-de-mlops)
5. [Drift: Por Qué los Modelos se Degradan Solos](#5-drift-por-qué-los-modelos-se-degradan-solos)
6. [Estrategias de Reentrenamiento](#6-estrategias-de-reentrenamiento)
7. [Decisiones de Diseño del Lab 07](#7-decisiones-de-diseño-del-lab-07)
8. [Costos](#8-costos)
9. [Referencias Técnicas](#9-referencias-técnicas)

---

## 1. Resumen Ejecutivo: el Modelo es la Parte Pequeña

El paper de Google *"Hidden Technical Debt in Machine Learning Systems"* (Sculley et al., NeurIPS 2015) tiene un diagrama que se volvió famoso: un sistema de ML real dibujado como un conjunto de cajas, donde el **código del modelo es una caja diminuta en el centro**. Todo lo demás es infraestructura: recolección de datos, extracción de features, verificación, gestión de recursos, serving, monitoreo.

```
LO QUE SE ENSEÑA vs. LO QUE HAY EN PRODUCCIÓN

  Notebook de ciencia de datos         Sistema de ML en producción
  ┌──────────────────────┐             ┌─────────────────────────────────────┐
  │ df = leer_csv()       │             │ Datos Silver ─► Feature Store        │
  │ modelo.fit(X, y)      │             │      │                               │
  │ print(accuracy)       │             │      ▼                               │
  └──────────────────────┘             │ Pipeline de entrenamiento            │
                                        │      │                               │
     "Funciona en mi máquina"           │      ▼                               │
                                        │ Model Registry (versiones, linaje)   │
                                        │      │                               │
                                        │      ▼                               │
                                        │ Endpoint (serving 24/7)              │
                                        │      │                               │
                                        │      ▼                               │
                                        │ Monitoreo de drift ─► reentrenamiento│
                                        └─────────────────────────────────────┘
```

**MLOps** es la disciplina de construir y operar esa infraestructura, de la misma forma que DevOps lo es para el software. La diferencia fundamental con el software tradicional: **un modelo se degrada aunque nadie toque su código**, porque el mundo que modela cambia. Por eso el monitoreo y el reentrenamiento no son extras, sino el centro del problema (§5).

---

## 2. El Nombre: de Vertex AI a Gemini Enterprise Agent Platform

El 22 de abril de 2026, en Google Cloud Next '26, Google presentó **Gemini Enterprise Agent Platform** como la evolución de Vertex AI. Es el mismo patrón de rebranding que vimos con Knowledge Catalog en el [Módulo 06](../06-knowledge-catalog-mesh-fabric/teoria.md):

| Antes (hasta abril 2026) | Ahora |
|---|---|
| Vertex AI | Gemini Enterprise Agent Platform |
| Vertex AI Feature Store | Feature Store on Gemini Enterprise Agent Platform |
| Vertex AI Model Registry | Model Registry on Gemini Enterprise Agent Platform |
| Vertex AI Pipelines | Gemini Enterprise Agent Platform Pipelines |
| Vertex AI Model Monitoring | Model Monitoring on Gemini Enterprise Agent Platform |

**Lo que NO cambió** (y por eso el código del lab dice "aiplatform"):
- La API: `aiplatform.googleapis.com`.
- El SDK de Python: `google-cloud-aiplatform` (`from google.cloud import aiplatform`).
- Los comandos: `gcloud ai ...`.
- Los roles IAM: `roles/aiplatform.*`.

En la consola, las piezas clásicas de ML (Feature Store, Model Registry, Endpoints, Pipelines, Monitoring) quedaron agrupadas bajo un submenú **Models**, junto a las nuevas capacidades de agentes que dieron nombre a la plataforma.

---

## 3. Las Piezas del Ciclo de Vida

### 3.1. Feature Store: Una Sola Definición para Entrenar y Servir

Una *feature* es una variable de entrada del modelo (ej. `ratio_deuda_ingreso`). El problema que resuelve un Feature Store se llama **training-serving skew**: el equipo de ciencia de datos calcula la feature de una forma en el notebook de entrenamiento, el equipo de backend la reimplementa de otra forma en el servicio de predicción, y el modelo recibe en producción algo distinto de lo que vio al entrenar. El modelo no da error: **simplemente predice peor, en silencio**.

```
SIN FEATURE STORE                         CON FEATURE STORE

notebook:  ratio = deuda / ingreso         ┌──────────────────────────┐
backend:   ratio = deuda / (ingreso+1)     │ Feature: ratio_deuda_     │
           ↑ "para evitar dividir por 0"   │ ingreso (1 definición,    │
                                           │ con dueño y descripción)  │
→ el modelo ve datos distintos             └──────┬───────────┬────────┘
  en train y en serving                     entrenamiento   serving
```

Un Feature Store tiene dos almacenes:
- **Offline store**: el historial completo de features, para entrenar. En la plataforma de Google, **el offline store es BigQuery**: el Feature Store no copia los datos, registra tablas o vistas existentes como *Feature Groups*.
- **Online store**: los valores más recientes, con latencia de milisegundos, para servir predicciones en tiempo real. Este sí es infraestructura aparte (con costo por hora) y solo se justifica cuando el endpoint necesita buscar features por clave en el momento de predecir.

**¿Cuándo hace falta el online store?** El modelo necesita las features no solo para entrenar, sino **cada vez que predice**. La pregunta decisiva es quién tiene esas features en el momento de pedir la predicción:

```
LA APP YA TIENE LAS FEATURES             LA APP SOLO TIENE UNA LLAVE
(datos del formulario)                   (features calculadas sobre la historia)

App ──(ingreso, deuda, score, plazo)──►  App ──(cliente_id)──► Servicio
          Endpoint                                               │ búsqueda en ms
                                                                 ▼
 No necesitas online store                                 Online Store
                                                 (últimos valores, sincronizados
                                                  desde BigQuery)
                                                                 │
                                                                 ▼
                                                             Endpoint
```

Si la petición trae todo lo que el modelo necesita, el online store sobra. Si el modelo usa features calculadas en la plataforma de datos (por ejemplo, "pagos atrasados en los últimos 12 meses"), hay que buscarlas por llave en milisegundos, y consultar BigQuery en cada predicción es demasiado lento. Ahí el online store es la copia rápida de los valores más recientes de cada entidad.

Una propiedad clave del offline store es la **corrección point-in-time**: al entrenar con datos históricos, cada fila debe usar el valor que la feature tenía **en ese momento**, no el valor actual. Si no, el modelo "ve el futuro" (*data leakage*) y su desempeño en entrenamiento es falsamente optimista. Para eso existe la columna `feature_timestamp`.

### 3.2. Model Registry: Versiones, Alias y Linaje

El Model Registry es para los modelos lo que Git es para el código: un repositorio central con **versiones**. Cada versión guarda:
- El artefacto del modelo.
- Sus métricas de evaluación.
- Su **linaje**: con qué datos y qué proceso se entrenó.
- **Alias** legibles (`v1`, `champion`, `staging`) que apuntan a una versión concreta y se pueden mover.

Sin registry, la pregunta "¿qué modelo estaba en producción el 15 de marzo, y con qué datos se entrenó?" no tiene respuesta confiable. En industrias reguladas, como el crédito del lab, esa pregunta la hace un auditor.

### 3.3. Endpoints: Inferencia Online vs. Batch

| | Predicción online (Endpoint) | Predicción batch |
|---|---|---|
| **Cuándo** | Una decisión por petición, en tiempo real (aprobar un crédito al instante) | Muchas predicciones de una vez (puntuar toda la cartera cada noche) |
| **Latencia** | Milisegundos | Minutos u horas |
| **Infraestructura** | Nodos encendidos 24/7 | Cómputo efímero mientras corre el job |
| **Costo** | Por hora mientras está desplegado, aunque no reciba tráfico | Solo mientras corre |

Un endpoint puede tener **varias versiones desplegadas a la vez** y repartir el tráfico entre ellas (*traffic split*). Eso habilita estrategias de despliegue seguras:
- **Canary**: la versión nueva recibe el 5-10% del tráfico; si se comporta bien, se sube gradualmente.
- **A/B**: dos versiones reciben tráfico en paralelo para comparar resultados de negocio.
- **Shadow**: la versión nueva recibe copia del tráfico, pero sus respuestas no se usan; solo se comparan.

El lab usa el reemplazo directo (100% a la versión nueva) por simplicidad, pero el *quality gate* previo (§6) cumple parte del rol de protección que en producción daría un canary.

### 3.4. Pipelines: Kubeflow sin Administrar Kubernetes

Un pipeline de ML es un **DAG** (grafo dirigido acíclico) de pasos: preparar datos, entrenar, evaluar, registrar, desplegar. Es la misma idea del DAG de Dataform del [Módulo 01](../01-patrones-y-modelado/teoria.md) o del pipeline de Beam del [Módulo 05](../05-elt-dataflow-iceberg/teoria.md), aplicada al ciclo de vida del modelo.

**Kubeflow Pipelines (KFP)** es el estándar abierto para definir esos DAGs en Python. Tradicionalmente, usar Kubeflow implicaba administrar un cluster de Kubernetes. Las Pipelines de la plataforma de Google ejecutan definiciones KFP **de forma serverless**: escribes el pipeline con el SDK de KFP, y Google aprovisiona y libera el cómputo de cada paso.

```
ANATOMÍA DE UN PIPELINE KFP

@dsl.component  ──► cada componente corre en su propio contenedor,
                    con sus propios paquetes; recibe y devuelve valores
@dsl.pipeline   ──► conecta componentes: la salida de uno es la entrada de otro
dsl.If          ──► rama condicional: el paso solo corre si se cumple la condición
schedule (cron) ──► ejecuta el pipeline periódicamente sin intervención humana
```

Dos detalles prácticos que importan:
- **Caching**: por defecto, si un componente ya corrió con exactamente las mismas entradas, KFP reutiliza su resultado. Es útil para no repetir entrenamientos costosos, pero peligroso en un pipeline de monitoreo: si la tabla cambió pero su *nombre* no, la caché devolvería el PSI viejo. Por eso el lab lo desactiva.
- **Identidad**: cada componente corre con una cuenta de servicio, por defecto la de Compute Engine. Esa cuenta necesita permisos explícitos sobre BigQuery, el registry y el endpoint.

---

## 4. Niveles de Madurez de MLOps

Google describe tres niveles en su guía *"MLOps: Continuous delivery and automation pipelines in machine learning"*:

| Nivel | Cómo se entrena y despliega | Síntoma típico |
|---|---|---|
| **0 — Manual** | Un científico de datos entrena en un notebook y entrega el modelo "por encima de la pared" al equipo de ingeniería | El modelo en producción se reentrena una o dos veces al año, cuando alguien se acuerda |
| **1 — Automatización del pipeline (Continuous Training)** | El **pipeline** de entrenamiento es el artefacto que se despliega; reentrena automáticamente ante datos nuevos o drift, con validación de datos y del modelo | El modelo se mantiene al día solo; los humanos revisan el pipeline, no cada modelo |
| **2 — CI/CD del pipeline** | Además, los **cambios al código del pipeline** pasan por integración y entrega continua (tests, build, despliegue del pipeline) | Varios equipos iteran sobre muchos pipelines con seguridad |

**El Lab 07 te lleva al nivel 1**: el pipeline detecta drift, reentrena, valida contra el modelo actual y despliega, ya sea a mano o programado. El nivel 2 agregaría un repositorio Git con el código del pipeline y un sistema de CI (como Cloud Build) que lo compila, lo prueba y lo publica cada vez que alguien hace un cambio.

---

## 5. Drift: Por Qué los Modelos se Degradan Solos

### 5.1. Los Tipos de Drift

Un modelo aprende la relación entre entradas `X` y salida `y` **tal como era en los datos de entrenamiento**. Hay varias formas en que ese supuesto se rompe:

| Tipo | Qué cambia | Ejemplo en crédito | ¿Se detecta sin etiquetas? |
|---|---|---|---|
| **Data drift** (*covariate shift*) | La distribución de las entradas `P(X)` | Los solicitantes nuevos ganan menos que los del histórico | ✅ Sí: basta comparar distribuciones de features |
| **Label drift** (*prior shift*) | La proporción de la etiqueta `P(y)` | La tasa de impago general sube de 25% a 45% | ⚠️ Solo cuando llegan las etiquetas |
| **Concept drift** | La relación entre entradas y salida `P(y│X)` | El plazo del crédito empieza a pesar más que el score en predecir el impago | ⚠️ Solo cuando llegan las etiquetas |
| **Training-serving skew** | Lo que el modelo recibe en producción vs. lo que vio al entrenar, por un **error de ingeniería**, no por un cambio del mundo | El backend calcula `ratio_deuda_ingreso` distinto que el notebook | ✅ Sí: comparando features de entrenamiento vs. de serving |

El punto incómodo: **el concept drift es el más dañino y el más lento de detectar**, porque requiere la etiqueta real (¿esta persona cayó en impago?), y esa etiqueta llega meses después. Por eso los sistemas reales monitorean el **data drift como señal temprana**: si las entradas cambiaron mucho, es probable que el desempeño también haya cambiado, aunque todavía no se pueda medir.

El lote `drift` del lab inyecta **ambos** tipos a propósito: data drift (ingresos 35% más bajos, más endeudamiento) y concept drift (cambia el peso del plazo y del score en la probabilidad de impago).

### 5.2. Population Stability Index (PSI)

El PSI es la métrica de drift clásica en riesgo de crédito. Compara la distribución de una feature en un período base (el entrenamiento) contra un período actual:

```
1. Dividir la feature en bins usando los deciles de la distribución BASE
2. Para cada bin i:  b_i = % de filas base en el bin,  a_i = % de filas actuales en el bin
3. PSI = Σ (a_i − b_i) × ln(a_i / b_i)
```

Regla práctica de la industria:

| PSI | Interpretación |
|---|---|
| < 0.10 | Sin cambio significativo |
| 0.10 – 0.25 | Cambio moderado: vigilar |
| > 0.25 | Cambio significativo: investigar y probablemente reentrenar |

El lab usa un umbral de **0.2**. Los bins se construyen con los cuantiles de la distribución base (`APPROX_QUANTILES` en BigQuery), y los bins vacíos se reemplazan por un valor pequeño (0.0001) para evitar `ln(0)`. Por eso, cuando una distribución se desplaza tanto que deja bins casi vacíos, el PSI puede subir muy por encima de 1: es la señal de un cambio drástico, no un error.

Otras métricas usadas en monitoreo gestionado: la **distancia L-infinito** y la **divergencia de Jensen-Shannon** (más estables con pocos datos), y la prueba de **Kolmogorov-Smirnov** para features continuas.

---

## 6. Estrategias de Reentrenamiento

| Estrategia | Cuándo reentrena | Ventaja | Riesgo |
|---|---|---|---|
| **Programada** | Cada N días, haya o no cambios | Simple y predecible | Gasta cómputo sin necesidad, o reacciona tarde a un cambio brusco |
| **Por disparador (trigger)** | Cuando una métrica de drift supera un umbral | Reentrena solo cuando hace falta | Depende de elegir bien la métrica y el umbral |
| **Continua / online** | Con cada dato nuevo | Siempre al día | Compleja; un dato malo afecta al modelo de inmediato |

El lab **combina las dos primeras**: un schedule (cron) que dispara el pipeline periódicamente, y dentro del pipeline un disparador (PSI > umbral) que decide si de verdad se reentrena.

**Ventana deslizante vs. acumulativa:** al reentrenar, ¿se usan todos los datos históricos o solo los recientes? Con concept drift, los datos viejos describen un mundo que ya no existe y "diluyen" lo nuevo. Por eso el lab entrena el challenger **solo con el lote reciente** (ventana deslizante). Con drift leve, acumular historia suele dar modelos más estables.

**Champion / challenger:** el modelo en producción es el *champion*; el recién entrenado es el *challenger*. El challenger solo reemplaza al champion si gana en una comparación justa: **la misma métrica, sobre el mismo conjunto de evaluación, que ninguno de los dos vio al entrenar**. Este *quality gate* es lo que separa un pipeline de MLOps de un cron que reentrena a ciegas.

---

## 7. Decisiones de Diseño del Lab 07

| Decisión | Elegido en el lab | Alternativa | Por qué |
|---|---|---|---|
| Cómo entrenar | **BigQuery ML** (`CREATE MODEL`) | Custom training (Python en un contenedor) | El modelo se entrena con SQL, en el mismo lugar donde viven los datos Silver, y se registra solo en el Model Registry con `model_registry = 'VERTEX_AI'`. Custom training agrega unos 30 minutos de setup que no aportan al objetivo del taller |
| Feature Store | **Solo offline** | Online store | El offline store es BigQuery (sin costo de nodos). El online store solo se justifica cuando el endpoint necesita buscar features por clave en milisegundos |
| Detección de drift | **PSI calculado con SQL dentro del pipeline** | Model Monitoring gestionado; *feature monitors* del Feature Store | El PSI es transparente (se ve la fórmula), determinístico para una demo en vivo y prácticamente gratis. Las alternativas gestionadas quedan como retos |
| Inferencia | **Endpoint online** | Batch prediction | Es la pieza más representativa de "modelo en producción", con la advertencia explícita de su costo por hora |
| Datos | **Sintéticos, generados con SQL** | Datos reales de los Módulos 01/05 | Permiten **inyectar drift a propósito** y hacen el lab independiente de los otros módulos |

---

## 8. Costos

> [!IMPORTANT]
> - **BigQuery ML**: el entrenamiento con `CREATE MODEL` se factura por bytes procesados, con 10 GiB/mes gratis. Los datos del lab son de pocos MB.
> - **Feature Store offline**: los datos viven en BigQuery; sin online store no hay nodos de serving.
> - **Endpoint**: se factura **por hora de nodo mientras haya un modelo desplegado**, reciba tráfico o no. Es el costo dominante del lab y el recurso que más importa borrar.
> - **Pipelines**: un cargo pequeño por ejecución más el cómputo de cada componente mientras corre.
> - **Schedules**: no cobran por existir, pero cada ejecución que disparan sí cobra. Hay que borrarlos al terminar.

---

## 9. Referencias Técnicas

1. **Sculley, D., et al. (2015).** *Hidden Technical Debt in Machine Learning Systems.* Advances in Neural Information Processing Systems (NeurIPS) 28.
2. **Google Cloud Architecture Center.** *MLOps: Continuous delivery and automation pipelines in machine learning* (niveles de madurez 0, 1 y 2).
3. **Huyen, C. (2022).** *Designing Machine Learning Systems.* O'Reilly Media. (Cap. 8: *Data Distribution Shifts and Monitoring*.)
4. **Yurdakul, B. (2018).** *Statistical Properties of Population Stability Index.* Western Michigan University.
5. **Google Cloud — Manage BigQuery ML models in the Model Registry.** [docs.cloud.google.com/bigquery/docs/managing-models-vertex](https://docs.cloud.google.com/bigquery/docs/managing-models-vertex).
6. **Google Cloud — About Feature Store.** [docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/overview](https://docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/overview).
7. **Google Cloud — Gemini Enterprise Agent Platform name changes.** [docs.cloud.google.com/gemini-enterprise-agent-platform/vertex-ai-name-changes](https://docs.cloud.google.com/gemini-enterprise-agent-platform/vertex-ai-name-changes).
8. **Kubeflow Pipelines (KFP v2).** [kubeflow.org/docs/components/pipelines](https://www.kubeflow.org/docs/components/pipelines/).
