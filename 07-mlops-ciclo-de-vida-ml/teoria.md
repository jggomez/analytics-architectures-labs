# MLOps: Infraestructura para el Ciclo de Vida de Machine Learning

> **Audiencia Objetivo:** Data Engineers, ML Engineers, Analytics Engineers y Data Architects que ya saben llevar datos a una capa Silver/Gold (Módulos 01-05) y necesitan entender **qué hace falta para que un modelo entrenado sobre esos datos llegue a producción y se mantenga sano**.
>
> **Alcance:** Qué es MLOps, las fases del ciclo de vida de un modelo, los componentes de una plataforma de MLOps (Feature Store, seguimiento de experimentos, Model Registry, serving, pipelines, monitoreo), drift y reentrenamiento, y las ventajas y trade-offs de cada decisión. La teoría es **agnóstica de proveedor**: el capítulo 9 mapea cada concepto a los productos de Google Cloud, AWS, Azure, Databricks y open source, y el lab lo implementa en Google Cloud.
>
> 🛠️ **Laboratorio Práctico Asociado:** [Lab 07 — MLOps en GCP](lab.md)

---

## Tabla de Contenidos

1. [Resumen Ejecutivo: el Modelo es la Parte Pequeña](#1-resumen-ejecutivo-el-modelo-es-la-parte-pequeña)
2. [Qué es MLOps](#2-qué-es-mlops)
3. [El Ciclo de Vida de un Modelo: Fases](#3-el-ciclo-de-vida-de-un-modelo-fases)
4. [Los Componentes de una Plataforma de MLOps](#4-los-componentes-de-una-plataforma-de-mlops)
5. [Niveles de Madurez de MLOps](#5-niveles-de-madurez-de-mlops)
6. [Drift: Por Qué los Modelos se Degradan Solos](#6-drift-por-qué-los-modelos-se-degradan-solos)
7. [Reentrenamiento y Despliegue Seguro](#7-reentrenamiento-y-despliegue-seguro)
8. [Ventajas y Trade-offs](#8-ventajas-y-trade-offs)
9. [Mapa de Herramientas: del Concepto al Producto](#9-mapa-de-herramientas-del-concepto-al-producto)
10. [Decisiones de Diseño del Lab 07](#10-decisiones-de-diseño-del-lab-07)
11. [Costos](#11-costos)
12. [Referencias Técnicas](#12-referencias-técnicas)

---

## 1. Resumen Ejecutivo: el Modelo es la Parte Pequeña

El paper *"Hidden Technical Debt in Machine Learning Systems"* (Sculley et al., NeurIPS 2015) tiene un diagrama que se volvió famoso: un sistema de ML real dibujado como un conjunto de cajas, donde el **código del modelo es una caja diminuta en el centro**. Todo lo demás es infraestructura: recolección de datos, extracción de features, verificación, gestión de recursos, serving, monitoreo.

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
                                        │ Serving (endpoint 24/7 o batch)      │
                                        │      │                               │
                                        │      ▼                               │
                                        │ Monitoreo de drift ─► reentrenamiento│
                                        └─────────────────────────────────────┘
```

Muchos de los modelos que se entrenan nunca llegan a producción, y muchos de los que llegan se degradan sin que nadie lo note. Casi nunca es porque el algoritmo sea malo: es porque falta todo lo que rodea al modelo. **MLOps** es la disciplina que se ocupa de eso.

---

## 2. Qué es MLOps

**MLOps** (*Machine Learning Operations*) es el conjunto de prácticas, procesos y herramientas para **llevar modelos de ML a producción de forma confiable y repetible, y mantenerlos funcionando bien en el tiempo**. Toma las ideas de DevOps (automatización, versionado, integración y entrega continua, monitoreo) y las adapta a un tipo de software con una particularidad: su comportamiento no lo define solo el código, sino también los **datos** con los que se entrenó.

### 2.1. De DevOps a MLOps: Tres Cosas Cambian en Vez de Una

En el software tradicional, el comportamiento del sistema cambia cuando alguien cambia el código. En ML cambia por **tres** ejes, y cualquiera de ellos puede romper el sistema:

```
SOFTWARE TRADICIONAL                 SISTEMA DE ML

   Código ──► Comportamiento          Código ─┐
                                      Datos  ─┼──► Modelo ──► Comportamiento
                                      Modelo ─┘   (artefacto entrenado)
```

| | DevOps (software) | MLOps (sistemas de ML) |
|---|---|---|
| **Qué se versiona** | Código | Código + datos + modelo (y la relación entre los tres) |
| **Qué se prueba** | Que el código haga lo que dice | Además: que los datos sean válidos y que el modelo sea **suficientemente bueno** (una métrica, no un sí/no) |
| **Qué se despliega** | Una aplicación | Un modelo y, con más madurez, **el pipeline que lo entrena** |
| **Por qué falla en producción** | Un bug, una dependencia, la infraestructura | Lo mismo, más: **el mundo cambió** y el modelo quedó desactualizado, sin que nadie tocara nada |
| **Qué se monitorea** | Latencia, errores, uso de recursos | Lo mismo, más: distribución de las entradas, calidad de las predicciones y desempeño real cuando llegan las etiquetas |
| **Ciclo** | Se construye y se despliega | Se construye, se despliega, **se degrada y se reentrena**, en bucle |

La última fila es la diferencia de fondo: **un modelo se degrada aunque nadie toque su código**, porque el mundo que modela cambia. Por eso, en MLOps, el monitoreo y el reentrenamiento no son extras: son el centro del problema (§6 y §7).

### 2.2. Principios

| Principio | Qué significa | Qué pasa si falta |
|---|---|---|
| **Reproducibilidad** | Dado un modelo en producción, se puede volver a construir exactamente: mismo código, mismos datos, mismos parámetros | Nadie puede explicar por qué el modelo de marzo predecía distinto al de abril |
| **Versionado de todo** | Código, datos (o una referencia inmutable a ellos), features y modelos tienen versión | "¿Con qué datos se entrenó esto?" no tiene respuesta |
| **Automatización** | Entrenar, evaluar y desplegar lo hace un pipeline, no una persona copiando archivos | Cada reentrenamiento es un proyecto manual, y por eso casi nunca ocurre |
| **Validación continua** | Los datos y el modelo pasan *quality gates* antes de llegar a producción | Un lote de datos corrupto produce un modelo malo que se despliega solo |
| **Monitoreo** | Se mide en producción lo que el modelo recibe y lo que predice | El modelo predice peor durante meses sin que nadie lo note |
| **Trazabilidad (linaje)** | De cada predicción se puede llegar al modelo, y del modelo a sus datos y su código | Imposible responderle a un auditor o a un cliente afectado |

### 2.3. Quién Hace Qué: Roles

MLOps es un problema de equipo, no de una persona:

| Rol | Responsabilidad principal en el ciclo |
|---|---|
| **Data Engineer** | Que los datos lleguen limpios, a tiempo y con calidad (las capas Silver/Gold de los Módulos 01-05); muchas veces también los pipelines de features |
| **Data Scientist** | Entender el problema, explorar los datos, elegir features y algoritmo, y definir cómo se evalúa el modelo |
| **ML Engineer** | Convertir el experimento en un sistema: pipelines de entrenamiento, serving, monitoreo, reentrenamiento |
| **Platform / MLOps Engineer** | La plataforma compartida que usan todos los equipos: feature store, registry, orquestador, infraestructura de serving |
| **Negocio / Riesgo** | Definir qué es "un buen modelo" en términos del negocio, y aprobar o auditar su uso (crítico en crédito, salud o seguros) |

El síntoma clásico de no tener MLOps es el modelo que se entrega "por encima de la pared": el científico de datos le pasa un notebook al equipo de ingeniería, que lo reimplementa a su manera. Ahí nacen el *training-serving skew* (§4.1) y los modelos que nadie sabe reentrenar.

---

## 3. El Ciclo de Vida de un Modelo: Fases

El ciclo de vida de ML no es una línea, sino un **bucle**: el monitoreo de un modelo en producción alimenta la siguiente versión.

```
  1. Problema ─► 2. Datos ─► 3. Experimentación ─► 4. Evaluación ─► 5. Registro
     y métricas     y features    y entrenamiento      y validación        │
                                         ▲                                  ▼
                                         │                          6. Despliegue
                                         │                             y serving
                                         │                                  │
                                 8. Reentrenamiento ◄──── 7. Monitoreo ◄────┘
```

| Fase | Qué se hace | Pregunta que responde | Artefacto que produce | En el Lab 07 |
|---|---|---|---|---|
| **1. Problema y métricas** | Traducir una necesidad de negocio en una tarea de ML, y definir cómo se mide el éxito (técnico y de negocio) | ¿Qué predecimos, para qué decisión, y cuánto error es aceptable? | Definición del problema, métrica objetivo | Predecir impago de un crédito; métrica AUC |
| **2. Datos y features** | Ingestar, validar y transformar datos; diseñar y calcular las features | ¿Tenemos datos suficientes, correctos y sin fuga del futuro? | Tablas de features con definición única | Paso 1 (datos) y Paso 2 (Feature Store) |
| **3. Experimentación y entrenamiento** | Probar algoritmos, features e hiperparámetros, registrando cada intento | ¿Qué combinación funciona mejor, y la podemos repetir? | Experimentos registrados; un modelo candidato | Paso 3.1 (experimento con 4 candidatos) |
| **4. Evaluación y validación** | Medir el modelo en datos que no vio; compararlo con el modelo actual; revisar sesgos | ¿Es suficientemente bueno, y mejor que lo que ya hay? | Métricas de evaluación; decisión de aprobar o no | `ML.EVALUATE`; comparación de AUC en el pipeline |
| **5. Registro** | Guardar el modelo como versión, con métricas y linaje | ¿Qué versión es esta, de dónde salió y quién la aprobó? | Versión en el Model Registry | Paso 3 (`model_registry`) |
| **6. Despliegue y serving** | Poner el modelo a responder predicciones (online o batch) | ¿Cómo lo consumen las aplicaciones, con qué latencia y costo? | Endpoint o job de predicción | Paso 4 (endpoint) |
| **7. Monitoreo** | Vigilar entradas, predicciones y desempeño real | ¿Sigue funcionando bien el modelo? | Métricas de drift y de desempeño; alertas | Pasos 5-6 (PSI) |
| **8. Reentrenamiento** | Entrenar una versión nueva cuando hace falta, y reemplazar la anterior solo si es mejor | ¿Cuándo y con qué datos actualizamos el modelo? | Nueva versión (vuelve a la fase 4) | Pasos 6-8 (pipeline y cron) |

Dos ideas para llevarse:
- **Las fases 2 a 6 se hacen una vez a mano, y después las hace un pipeline.** El primer modelo se construye explorando; los siguientes, automatizando lo que se aprendió al explorar. Eso es lo que miden los niveles de madurez (§5).
- **La fase 1 se subestima.** Un modelo con un AUC excelente que optimiza la métrica equivocada es un éxito técnico y un fracaso de negocio.

---

## 4. Los Componentes de una Plataforma de MLOps

Cada fase del ciclo necesita una pieza de infraestructura. Los nombres comerciales cambian de proveedor a proveedor (capítulo 9), pero los componentes son los mismos:

```
COMPONENTES DE UNA PLATAFORMA DE MLOPS

   Datos (Silver/Gold)
        │
        ▼
  ┌───────────────┐   ┌─────────────────────┐   ┌────────────────┐
  │ Feature Store │──►│ Pipelines de         │──►│ Model Registry │
  │ (§4.1)        │   │ entrenamiento (§4.5) │   │ (§4.3)         │
  └───────┬───────┘   │ + seguimiento de     │   └───────┬────────┘
          │           │ experimentos (§4.2)  │           │
          │           └─────────────────────┘           ▼
          │                                     ┌────────────────┐
          └──────── features en línea ─────────►│ Serving (§4.4) │
                                                └───────┬────────┘
                                                        ▼
                                                ┌─────────────────┐
                    reentrenar ◄────────────────│ Monitoreo (§4.6)│
                                                └─────────────────┘

  Transversal: metadatos y linaje, control de acceso, CI/CD
```

### 4.1. Feature Store: Una Sola Definición para Entrenar y Servir

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

Un Feature Store tiene tres piezas:
- **Registro (catálogo) de features:** qué features existen, de qué tabla salen, cuál es la llave de la entidad (cliente, solicitud) y quién es el dueño. Son **metadatos**, no datos.
- **Offline store:** el historial completo de features, para entrenar. Suele ser el propio data warehouse o lakehouse; en varias plataformas el Feature Store no copia esos datos, solo los registra.
- **Online store:** una base de datos aparte, con los **valores más recientes** de cada entidad, optimizada para buscar por llave en milisegundos. Se llena **copiando (sincronizando)** desde el offline store, no recalculando. Eso garantiza que entrenamiento y serving vean los mismos valores.

**¿Cuándo hace falta el online store?** El modelo necesita las features no solo para entrenar, sino **cada vez que predice**. La pregunta decisiva es quién tiene esas features en el momento de pedir la predicción:

```
LA APP YA TIENE LAS FEATURES             LA APP SOLO TIENE UNA LLAVE
(datos del formulario)                   (features calculadas sobre la historia)

App ──(ingreso, deuda, score, plazo)──►  App ──(cliente_id)──► Servicio
          Endpoint                                               │ búsqueda en ms
                                                                 ▼
 No necesitas online store                                 Online Store
                                                 (últimos valores, sincronizados
                                                  desde el offline store)
                                                                 │
                                                                 ▼
                                                             Endpoint
```

Si la petición trae todo lo que el modelo necesita, el online store sobra. Si el modelo usa features calculadas sobre la historia (por ejemplo, "ingreso promedio de los últimos 12 meses" o "pagos atrasados en el último año"), esas se calculan antes, en batch o en streaming, y al momento de predecir hay que buscarlas por llave en milisegundos. Consultar el data warehouse en cada predicción tarda segundos: ahí el online store es la copia rápida de los valores más recientes.

Una propiedad clave del offline store es la **corrección point-in-time**: al entrenar con datos históricos, cada fila debe usar el valor que la feature tenía **en ese momento**, no el valor actual. Si no, el modelo "ve el futuro" (*data leakage*) y su desempeño en entrenamiento es falsamente optimista.

### 4.2. Seguimiento de Experimentos

Antes de llegar a un buen modelo, un científico de datos prueba decenas de combinaciones de features, algoritmos e hiperparámetros. El **seguimiento de experimentos** (*experiment tracking*) registra cada intento automáticamente: parámetros, métricas, versión del código y de los datos, y el artefacto resultante.

Sin él, el mejor modelo de la semana pasada es imposible de reproducir ("¿qué `learning_rate` usé?"). Con él, se pueden comparar los intentos lado a lado y elegir con evidencia. Es la memoria de la fase 3, y el punto de partida del linaje que después guarda el registry.

Un experimento y un registry guardan cosas distintas, y conviene no mezclarlos:

| | Experimento | Model Registry |
|---|---|---|
| **Qué guarda** | **Todos** los intentos, incluidos los que se descartan | Solo los modelos que aspiran a producción |
| **Pregunta que responde** | ¿Qué probamos, cómo le fue a cada cosa y por qué elegimos esta? | ¿Qué versión está en producción y de dónde salió? |
| **Quién lo usa más** | Quien explora (ciencia de datos) | Quien despliega y audita (ingeniería, riesgo) |

En el Lab 07 (Paso 3.1), cuatro candidatos se entrenan con BigQuery ML y cada uno queda como un *run* del experimento, con sus parámetros y sus métricas sobre el mismo holdout. Solo el elegido se registra en el registry como v1. Es de las primeras piezas que conviene adoptar en un proyecto real, porque es barata y su ausencia duele pronto.

### 4.3. Model Registry: Versiones, Alias y Linaje

El Model Registry es para los modelos lo que Git es para el código: un repositorio central con **versiones**. Cada versión guarda:
- El artefacto del modelo.
- Sus métricas de evaluación.
- Su **linaje**: con qué datos y con qué proceso se entrenó.
- **Alias** o etapas legibles (`v1`, `champion`, `staging`, `production`) que apuntan a una versión concreta y se pueden mover.

Sin registry, la pregunta "¿qué modelo estaba en producción el 15 de marzo, y con qué datos se entrenó?" no tiene respuesta confiable. En industrias reguladas, como el crédito del lab, esa pregunta la hace un auditor.

El registry también **desacopla** a quien entrena de quien despliega: el pipeline de entrenamiento publica versiones, y el despliegue consume "la versión con alias `champion`", sin saber cómo se construyó.

### 4.4. Serving: Inferencia Online vs. Batch

| | Predicción online (endpoint) | Predicción batch |
|---|---|---|
| **Cuándo** | Una decisión por petición, en tiempo real (aprobar un crédito al instante) | Muchas predicciones de una vez (puntuar toda la cartera cada noche) |
| **Latencia** | Milisegundos | Minutos u horas |
| **Infraestructura** | Servidores encendidos 24/7 | Cómputo efímero mientras corre el job |
| **Costo** | Por hora mientras está desplegado, aunque no reciba tráfico | Solo mientras corre |

Hay dos variantes más: **streaming**, donde el modelo se aplica a eventos que llegan por un flujo (como en detección de fraude sobre transacciones), y **edge**, donde el modelo corre en el dispositivo del usuario, sin red. La regla para elegir: **usa batch salvo que la decisión de verdad necesite tomarse en el momento**. Batch es más barato, más simple y más fácil de reprocesar.

Un endpoint puede tener **varias versiones desplegadas a la vez** y repartir el tráfico entre ellas (*traffic split*). Eso habilita las estrategias de despliegue seguro de §7.4.

### 4.5. Orquestación de Pipelines

Un pipeline de ML es un **DAG** (grafo dirigido acíclico) de pasos: preparar datos, entrenar, evaluar, registrar, desplegar. Es la misma idea del DAG de Dataform del [Módulo 01](../01-patrones-y-modelado/teoria.md) o del pipeline de Beam del [Módulo 05](../05-elt-dataflow-iceberg/teoria.md), aplicada al ciclo de vida del modelo.

Lo que distingue a un orquestador de ML de un simple cron:
- **Cada paso es aislado y reproducible:** corre en su propio contenedor, con sus propias dependencias.
- **Los pasos se pasan artefactos y valores:** la salida de "entrenar" es la entrada de "evaluar".
- **Ramas condicionales:** un paso solo corre si se cumple una condición. Así se implementan los *quality gates*.
- **Metadatos de cada ejecución:** qué entradas recibió cada paso y qué produjo, lo que alimenta el linaje.

**Kubeflow Pipelines (KFP)** es el estándar abierto más extendido para definir estos DAGs en Python. Tradicionalmente, usar Kubeflow implicaba administrar un cluster de Kubernetes; hoy varias nubes ejecutan definiciones KFP **de forma serverless**. Otras opciones comunes son Airflow (orquestador general, muy usado en ingeniería de datos), Metaflow y ZenML.

```
ANATOMÍA DE UN PIPELINE KFP

@dsl.component  ──► cada componente corre en su propio contenedor,
                    con sus propios paquetes; recibe y devuelve valores
@dsl.pipeline   ──► conecta componentes: la salida de uno es la entrada de otro
dsl.If          ──► rama condicional: el paso solo corre si se cumple la condición
schedule (cron) ──► ejecuta el pipeline periódicamente sin intervención humana
```

Dos detalles prácticos que importan:
- **Caching:** por defecto, si un componente ya corrió con exactamente las mismas entradas, KFP reutiliza su resultado. Es útil para no repetir entrenamientos costosos, pero peligroso en un pipeline de monitoreo: si la tabla cambió pero su *nombre* no, la caché devolvería el PSI viejo. Por eso el lab lo desactiva.
- **Identidad:** cada componente corre con una identidad (una cuenta de servicio). Esa identidad necesita permisos explícitos sobre los datos, el registry y el serving, y conviene que tenga solo esos.

### 4.6. Monitoreo

El monitoreo de un modelo tiene tres capas, de la más fácil a la más valiosa:

| Capa | Qué mide | Cuándo está disponible | Ejemplo |
|---|---|---|---|
| **Operacional** | Latencia, errores, disponibilidad, costo del serving | Inmediatamente | El endpoint responde en 80 ms con 0.1% de errores |
| **Datos (drift)** | Si cambió la distribución de las entradas o de las predicciones | Inmediatamente, sin etiquetas | El ingreso de los solicitantes bajó 35% (§6) |
| **Desempeño del modelo** | Si el modelo acierta: AUC, precisión, error | Cuando llegan las **etiquetas reales**, a veces meses después | De los créditos aprobados hace 6 meses, cuántos cayeron en impago |

La capa operacional la da cualquier herramienta de observabilidad. Las otras dos son específicas de ML, y la tensión entre ellas es el corazón del capítulo 6: lo que más importa (el desempeño) es lo que más tarda en poder medirse.

---

## 5. Niveles de Madurez de MLOps

Google describe tres niveles en su guía *"MLOps: Continuous delivery and automation pipelines in machine learning"*, y Microsoft publica un modelo equivalente en cinco niveles. La idea es la misma: **cuánto del ciclo de vida está automatizado**.

| Nivel | Cómo se entrena y despliega | Síntoma típico |
|---|---|---|
| **0 — Manual** | Un científico de datos entrena en un notebook y entrega el modelo "por encima de la pared" al equipo de ingeniería | El modelo en producción se reentrena una o dos veces al año, cuando alguien se acuerda |
| **1 — Automatización del pipeline (Continuous Training)** | El **pipeline** de entrenamiento es el artefacto que se despliega; reentrena automáticamente ante datos nuevos o drift, con validación de datos y del modelo | El modelo se mantiene al día solo; los humanos revisan el pipeline, no cada modelo |
| **2 — CI/CD del pipeline** | Además, los **cambios al código del pipeline** pasan por integración y entrega continua (tests, build, despliegue del pipeline) | Varios equipos iteran sobre muchos pipelines con seguridad |

**El Lab 07 te lleva al nivel 1:** el pipeline detecta drift, reentrena, valida contra el modelo actual y despliega, ya sea a mano o programado. El nivel 2 agregaría un repositorio Git con el código del pipeline y un sistema de CI que lo compila, lo prueba y lo publica cada vez que alguien hace un cambio.

No todo modelo necesita el nivel 2, ni siquiera el 1 (ver §8.4). El nivel correcto depende de cuántos modelos hay, qué tan rápido cambia su mundo y cuánto cuesta que uno se equivoque.

---

## 6. Drift: Por Qué los Modelos se Degradan Solos

### 6.1. Los Tipos de Drift

Un modelo aprende la relación entre entradas `X` y salida `y` **tal como era en los datos de entrenamiento**. Hay varias formas en que ese supuesto se rompe:

| Tipo | Qué cambia | Ejemplo en crédito | ¿Se detecta sin etiquetas? |
|---|---|---|---|
| **Data drift** (*covariate shift*) | La distribución de las entradas `P(X)` | Los solicitantes nuevos ganan menos que los del histórico | ✅ Sí: basta comparar distribuciones de features |
| **Label drift** (*prior shift*) | La proporción de la etiqueta `P(y)` | La tasa de impago general sube de 25% a 45% | ⚠️ Solo cuando llegan las etiquetas |
| **Concept drift** | La relación entre entradas y salida `P(y│X)` | El plazo del crédito empieza a pesar más que el score en predecir el impago | ⚠️ Solo cuando llegan las etiquetas |
| **Training-serving skew** | Lo que el modelo recibe en producción vs. lo que vio al entrenar, por un **error de ingeniería**, no por un cambio del mundo | El backend calcula `ratio_deuda_ingreso` distinto que el notebook | ✅ Sí: comparando features de entrenamiento vs. de serving |

El punto incómodo: **el concept drift es el más dañino y el más lento de detectar**, porque requiere la etiqueta real (¿esta persona cayó en impago?), y esa etiqueta llega meses después. Por eso los sistemas reales monitorean el **data drift como señal temprana**: si las entradas cambiaron mucho, es probable que el desempeño también haya cambiado, aunque todavía no se pueda medir.

El lote `drift` del lab inyecta **ambos** tipos a propósito: data drift (ingresos 35% más bajos, más endeudamiento) y concept drift (cambia el peso del plazo y del score en la probabilidad de impago).

### 6.2. Population Stability Index (PSI)

El PSI es la métrica de drift clásica en riesgo de crédito. Responde una sola pregunta: **¿los datos nuevos se parecen a los datos con los que se entrenó el modelo?** Para responderla, **agrupa los valores de una feature en cajones** (*bins*) y compara qué porcentaje cae en cada cajón antes y ahora.

**Un ejemplo con la feature "ingreso" y 3 cajones.**

**1. Con los datos de entrenamiento se arman los cajones**, cortados para que en cada uno caiga la misma cantidad de clientes:

| Cajón | Ingreso | % de clientes de entrenamiento |
|---|---|---|
| Bajo | menos de 3 millones | 33% |
| Medio | de 3 a 6 millones | 33% |
| Alto | más de 6 millones | 33% |

**2. Los datos nuevos se meten en los mismos cajones**, con los mismos cortes:

| Cajón | Antes | Lote sano | Lote con drift (crisis) |
|---|---|---|---|
| Bajo | 33% | 35% | **70%** |
| Medio | 33% | 32% | 25% |
| Alto | 33% | 33% | **5%** |

En el lote sano los clientes se reparten casi igual. En el lote con drift se amontonaron en el cajón "bajo".

**3. Se convierte en un número.** Para cada cajón *i*, con `bᵢ` = % antes (base) y `aᵢ` = % ahora (actual):

```
PSI = Σ (aᵢ − bᵢ) × ln(aᵢ / bᵢ)
```

Para el lote con drift:

| Cajón | Antes | Ahora | Cuánto aporta |
|---|---|---|---|
| Bajo | 0.33 | 0.70 | (0.70 − 0.33) × ln(0.70 / 0.33) = 0.37 × 0.75 = **0.28** |
| Medio | 0.33 | 0.25 | (0.25 − 0.33) × ln(0.25 / 0.33) = −0.08 × −0.28 = **0.02** |
| Alto | 0.33 | 0.05 | (0.05 − 0.33) × ln(0.05 / 0.33) = −0.28 × −1.89 = **0.53** |
| | | **PSI** | **0.83** |

Si un cajón **no cambia**, aporta 0; por eso el lote sano da un PSI cercano a 0. Si un cajón **cambia mucho**, aporta mucho, tanto si se llena como si se vacía: los dos factores tienen siempre el mismo signo, así que el PSI nunca resta.

Regla práctica de la industria:

| PSI | Interpretación |
|---|---|
| < 0.10 | Sin cambio significativo |
| 0.10 – 0.25 | Cambio moderado: vigilar |
| > 0.25 | Cambio significativo: investigar y probablemente reentrenar |

En la práctica se usan **10 cajones**, construidos con los deciles de la distribución base, y los cajones vacíos se reemplazan por un valor pequeño (0.0001) para evitar `ln(0)`. Por eso, cuando una distribución se desplaza tanto que deja cajones casi vacíos, el PSI puede subir muy por encima de 1: es la señal de un cambio drástico, no un error. El lab usa un umbral de **0.2** y calcula el PSI de cada feature, quedándose con el máximo: basta con que una cambie mucho para que el modelo pueda equivocarse.

**Limitaciones del PSI:**
- **Mira cada feature por separado.** Si cambia la **relación** entre dos features, pero no cada una por sí sola, no lo ve.
- **No detecta concept drift.** Mide si cambiaron las entradas, no si cambió la relación entre entradas y salida. Para eso hace falta comparar el desempeño con etiquetas reales (§7.3).
- **Necesita volumen.** Con pocas filas, los porcentajes por cajón son ruidosos y el PSI sube aunque no haya cambio real.

Otras métricas comunes en herramientas de monitoreo: la **distancia L-infinito** y la **divergencia de Jensen-Shannon** (más estables con pocos datos), y la prueba de **Kolmogorov-Smirnov** para features continuas.

---

## 7. Reentrenamiento y Despliegue Seguro

### 7.1. Cuándo Reentrenar

| Estrategia | Cuándo reentrena | Ventaja | Riesgo |
|---|---|---|---|
| **Programada** | Cada N días, haya o no cambios | Simple y predecible | Gasta cómputo sin necesidad, o reacciona tarde a un cambio brusco |
| **Por disparador (trigger)** | Cuando una métrica de drift o de desempeño supera un umbral | Reentrena solo cuando hace falta | Depende de elegir bien la métrica y el umbral |
| **Continua / online** | Con cada dato nuevo | Siempre al día | Compleja; un dato malo afecta al modelo de inmediato |

El lab **combina las dos primeras**: un schedule (cron) que dispara el pipeline periódicamente, y dentro del pipeline un disparador (PSI > umbral) que decide si de verdad se reentrena.

### 7.2. Con Qué Datos

**Ventana deslizante vs. acumulativa:** al reentrenar, ¿se usan todos los datos históricos o solo los recientes? Con concept drift, los datos viejos describen un mundo que ya no existe y "diluyen" lo nuevo. Por eso el lab entrena el challenger **solo con el lote reciente** (ventana deslizante). Con drift leve, acumular historia suele dar modelos más estables. Un punto intermedio común es acumular, pero dándoles más peso a los datos recientes.

### 7.3. Champion / Challenger

El modelo en producción es el *champion*; el recién entrenado es el *challenger*. El challenger solo reemplaza al champion si gana en una comparación justa: **la misma métrica, sobre el mismo conjunto de evaluación, que ninguno de los dos vio al entrenar**. Este *quality gate* es lo que separa un pipeline de MLOps de un cron que reentrena a ciegas.

Cuando el challenger gana, **pasa a ser el champion**: desde ese momento es la referencia contra la que se compara el siguiente challenger, y contra cuyos datos de entrenamiento se mide el drift. Si el pipeline siguiera comparando contra el champion original, detectaría "drift" para siempre y enfrentaría a cada challenger con un modelo que ya no está en producción.

### 7.4. Cómo Desplegar sin Riesgo

Ganar en el holdout no garantiza ganar en producción. Las estrategias de despliegue gradual reducen el riesgo de que una versión nueva se equivoque con todo el tráfico a la vez:

| Estrategia | Cómo funciona | Cuándo usarla |
|---|---|---|
| **Reemplazo directo** | La versión nueva recibe el 100% del tráfico | Cuando el quality gate previo es confiable y un error es barato de revertir |
| **Canary** | La versión nueva recibe el 5-10% del tráfico; si se comporta bien, se sube gradualmente | El estándar para modelos con impacto en clientes |
| **A/B** | Dos versiones reciben tráfico en paralelo para comparar **resultados de negocio** | Cuando la métrica técnica (AUC) no basta y hay que medir el efecto real |
| **Shadow** | La versión nueva recibe una copia del tráfico, pero sus respuestas no se usan; solo se comparan | Cuando equivocarse con un cliente real es inaceptable (crédito, salud) |

El lab usa el reemplazo directo por simplicidad, pero el quality gate de champion/challenger cumple parte del rol de protección que en producción daría un canary.

---

## 8. Ventajas y Trade-offs

### 8.1. Qué Ganas

- **Velocidad:** pasar de un experimento a producción en días, no en meses, y reentrenar sin abrir un proyecto nuevo cada vez.
- **Confiabilidad:** los modelos se validan antes de desplegarse y se vigilan después; un modelo degradado se detecta, en vez de descubrirse por una queja.
- **Reproducibilidad y auditoría:** cada predicción se puede rastrear hasta el modelo, sus datos y su código. En sectores regulados no es opcional.
- **Escala:** un equipo puede operar decenas de modelos con la misma plataforma, en vez de uno por persona.
- **Reutilización:** las features y los pipelines de un modelo sirven para el siguiente.

### 8.2. Qué Cuesta

- **Infraestructura:** un endpoint 24/7, un online store o una plataforma de orquestación cuestan aunque nadie los use.
- **Complejidad:** cada componente es una pieza más que configurar, asegurar, actualizar y entender.
- **Personas:** hace falta gente que sepa operar la plataforma, no solo entrenar modelos.
- **Rigidez inicial:** formalizar el ciclo de vida frena la exploración si se impone demasiado pronto.

### 8.3. Construir vs. Comprar

Cada componente se puede resolver de tres maneras, y la decisión de fondo es **quién mantiene la pieza: tu código o un servicio**:

| Opción | Ejemplo | A favor | En contra |
|---|---|---|---|
| **Servicio gestionado de una nube** | El Feature Store o el Model Registry de tu proveedor | Integración lista, sin servidores que operar, soporte | Dependencia del proveedor (*lock-in*); costo por uso; menos control |
| **Open source autogestionado** | MLflow, Feast, Kubeflow sobre tu propio Kubernetes | Portable entre nubes, sin licencias, control total | Lo operas tú: actualizaciones, seguridad, disponibilidad |
| **Construirlo con piezas genéricas** | Un servicio en contenedores que busca features en una base NoSQL y llama al modelo | Simple, barato, hecho a la medida de tu caso | Reinventas lo que ya existe; la consistencia entre entrenamiento y serving depende de tu disciplina |

Un ejemplo concreto: para servir features en línea, un servicio propio que lee de una base NoSQL **funciona bien** con pocos modelos y pocas features, siempre que el job que la llena **copie** los valores desde la tabla de entrenamiento en vez de recalcularlos. Con muchos modelos y equipos compartiendo features, mantener esa sincronización a mano se vuelve el problema, y ahí un Feature Store gestionado se paga solo.

### 8.4. Cuándo No Invertir (Tanto)

MLOps completo es una inversión que no todos los casos justifican:
- **Un solo modelo que se reentrena una vez al año** y se consume en batch: un notebook versionado y un job programado bastan.
- **Un modelo cuyo mundo casi no cambia** (por ejemplo, clasificar documentos con un formato estable): el monitoreo de drift aporta poco.
- **Una prueba de concepto:** primero hay que demostrar que el modelo genera valor. La plataforma se construye cuando hay algo que operar.

La señal para invertir más es el dolor: modelos que nadie sabe reentrenar, degradaciones que descubren los clientes, auditorías sin respuesta o equipos que reimplementan las mismas features.

---

## 9. Mapa de Herramientas: del Concepto al Producto

Los conceptos de los capítulos anteriores existen en todas las plataformas, con nombres distintos. Esta tabla sirve para **traducir**: si conoces el concepto, puedes ubicar el producto en cualquier nube.

| Componente | Google Cloud (Agent Platform, antes Vertex AI) | AWS (SageMaker AI) | Azure Machine Learning | Databricks | Open source |
|---|---|---|---|---|---|
| **Feature Store** | Feature Store (offline = BigQuery) | SageMaker Feature Store | Managed feature store | Feature Engineering en Unity Catalog | Feast, Hopsworks |
| **Seguimiento de experimentos** | Experiments | MLflow gestionado en SageMaker AI | MLflow integrado | MLflow gestionado | MLflow |
| **Model Registry** | Model Registry | SageMaker Model Registry | Model registry (y *registries* compartidos) | Models en Unity Catalog | MLflow Model Registry |
| **Orquestación de pipelines** | Pipelines (KFP serverless) | SageMaker Pipelines | Azure ML pipelines | Lakeflow Jobs | Kubeflow Pipelines, Airflow, Metaflow, ZenML |
| **Serving online** | Endpoints | Real-time / serverless inference | Managed online endpoints | Model Serving | KServe, BentoML, Ray Serve |
| **Predicción batch** | Batch prediction; `ML.PREDICT` en BigQuery ML | Batch Transform | Batch endpoints | Jobs / funciones de IA en SQL | Spark, Beam |
| **Monitoreo de modelos** | Model Monitoring | SageMaker Model Monitor | Model monitoring | Lakehouse Monitoring | Evidently, NannyML |
| **Validación de datos** | Data quality scans de Knowledge Catalog (antes Dataplex) | Glue Data Quality (basado en Deequ) | Microsoft Purview Data Quality | Expectations en Lakeflow Declarative Pipelines | Great Expectations, TFDV, Deequ |
| **CI/CD del pipeline** | Cloud Build | CodePipeline | Azure DevOps / GitHub Actions | Databricks Asset Bundles + CI | GitHub Actions, GitLab CI, Jenkins |

> [!NOTE]
> Los nombres comerciales cambian seguido: Google renombró Vertex AI a Gemini Enterprise Agent Platform en 2026, y AWS renombró SageMaker a SageMaker AI a fines de 2024. Por eso esta guía enseña primero el concepto: el componente sigue siendo el mismo aunque cambie la etiqueta. Antes de usar la tabla en un proyecto, verifica los nombres actuales en la documentación de cada proveedor.

### 9.1. Cómo lo Arma el Lab 07 en Google Cloud

| Fase / componente | Implementación en el lab | Por qué así |
|---|---|---|
| Datos | Tablas sintéticas en BigQuery (capa Silver) | Permiten inyectar drift a propósito |
| Feature Store | Feature Group que registra la tabla de BigQuery (solo offline, sin online store) | Las features vienen en la solicitud; no hace falta buscarlas por llave |
| Experimentación | Experiments, con un run por candidato (parámetros y métricas) y sin TensorBoard | Se comparan los candidatos con evidencia; sin TensorBoard no hay costo de almacenamiento |
| Entrenamiento | BigQuery ML (`CREATE MODEL`), con SQL | El modelo se entrena donde viven los datos, sin contenedores ni infraestructura |
| Registro | Model Registry, con `model_registry = 'VERTEX_AI'` y alias `v1` / `champion` | BigQuery ML registra cada versión automáticamente |
| Serving | Endpoint online | La pieza más representativa de "modelo en producción" |
| Monitoreo | PSI calculado con SQL dentro del pipeline | Transparente y casi gratis; Model Monitoring gestionado queda como reto |
| Orquestación | Pipelines (Kubeflow serverless) + schedule con cron | Estándar abierto, sin administrar Kubernetes |
| Champion / challenger | Comparación de AUC en el pipeline; alias `champion` + tabla de historial | El quality gate y la promoción quedan automatizados |

---

## 10. Decisiones de Diseño del Lab 07

| Decisión | Elegido en el lab | Alternativa | Por qué |
|---|---|---|---|
| Cómo entrenar | **BigQuery ML** (`CREATE MODEL`) | Custom training (Python en un contenedor) | El modelo se entrena con SQL, en el mismo lugar donde viven los datos Silver, y se registra solo en el Model Registry con `model_registry = 'VERTEX_AI'`. Custom training agrega unos 30 minutos de setup que no aportan al objetivo del taller |
| Experimentos | **4 candidatos con BigQuery ML, registrados en Experiments**; solo el elegido va al registry | Entrenar un solo modelo directo al registry | Muestra cómo se elige un modelo con evidencia, y la diferencia entre experimento y registry. Se desactiva el TensorBoard asociado (`experiment_tensorboard=False`) porque solo se registran parámetros y métricas finales |
| Feature Store | **Solo offline** | Online store | El offline store es BigQuery (sin costo de nodos). El online store solo se justifica cuando el endpoint necesita buscar features por clave en milisegundos |
| Detección de drift | **PSI calculado con SQL dentro del pipeline** | Model Monitoring gestionado; *feature monitors* del Feature Store | El PSI es transparente (se ve la fórmula), determinístico para una demo en vivo y prácticamente gratis. Las alternativas gestionadas quedan como retos |
| Quién es el champion | **Alias `champion` en el registry + tabla `champion_historial` en BigQuery**, leídos al inicio de cada ejecución | Parámetro fijo del pipeline | Con un parámetro fijo, tras promover la v2 el pipeline seguiría comparando AUC y midiendo drift contra la v1, que ya no está en producción. El alias dice qué versión sirve; la tabla agrega lo que el alias no guarda (el nombre BigQuery ML para `ML.EVALUATE` y los datos de entrenamiento para el PSI) y deja un historial de promociones |
| Inferencia | **Endpoint online** | Batch prediction | Es la pieza más representativa de "modelo en producción", con la advertencia explícita de su costo por hora |
| Despliegue | **Reemplazo directo** (100% a la versión nueva) | Canary o shadow | Simplicidad; el quality gate de champion/challenger cubre parte del riesgo |
| Datos | **Sintéticos, generados con SQL** | Datos reales de los Módulos 01/05 | Permiten **inyectar drift a propósito** y hacen el lab independiente de los otros módulos |

---

## 11. Costos

> [!IMPORTANT]
> - **BigQuery ML**: el entrenamiento con `CREATE MODEL` se factura por bytes procesados, con 10 GiB/mes gratis. Los datos del lab son de pocos MB.
> - **Feature Store offline**: los datos viven en BigQuery; sin online store no hay nodos de serving.
> - **Endpoint**: se factura **por hora de nodo mientras haya un modelo desplegado**, reciba tráfico o no. Es el costo dominante del lab y el recurso que más importa borrar.
> - **Pipelines**: un cargo pequeño por ejecución más el cómputo de cada componente mientras corre.
> - **Schedules**: no cobran por existir, pero cada ejecución que disparan sí cobra. Hay que borrarlos al terminar.

---

## 12. Referencias Técnicas

1. **Sculley, D., et al. (2015).** *Hidden Technical Debt in Machine Learning Systems.* Advances in Neural Information Processing Systems (NeurIPS) 28.
2. **Kreuzberger, D., Kühl, N., & Hirschl, S. (2023).** *Machine Learning Operations (MLOps): Overview, Definition, and Architecture.* IEEE Access, 11.
3. **Google Cloud Architecture Center.** *MLOps: Continuous delivery and automation pipelines in machine learning* (niveles de madurez 0, 1 y 2).
4. **Microsoft — Azure Architecture Center.** *Machine Learning operations maturity model.*
5. **Huyen, C. (2022).** *Designing Machine Learning Systems.* O'Reilly Media. (Cap. 8: *Data Distribution Shifts and Monitoring*; Cap. 10: *Infrastructure and Tooling for MLOps*.)
6. **Yurdakul, B. (2018).** *Statistical Properties of Population Stability Index.* Western Michigan University.
7. **Google Cloud — Manage BigQuery ML models in the Model Registry.** [docs.cloud.google.com/bigquery/docs/managing-models-vertex](https://docs.cloud.google.com/bigquery/docs/managing-models-vertex).
8. **Google Cloud — About Feature Store.** [docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/overview](https://docs.cloud.google.com/gemini-enterprise-agent-platform/machine-learning/featurestore/latest/overview).
9. **Kubeflow Pipelines (KFP v2).** [kubeflow.org/docs/components/pipelines](https://www.kubeflow.org/docs/components/pipelines/).
10. **MLflow.** [mlflow.org/docs/latest](https://mlflow.org/docs/latest/).
11. **Feast — Open source feature store.** [docs.feast.dev](https://docs.feast.dev/).
