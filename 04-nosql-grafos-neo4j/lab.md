# Lab 04 — Bases de Datos de Grafos con Neo4j AuraDB (Cloud Free Tier)

> 📖 **Marco Teórico:** Consulta la [Guía de NoSQL y Bases de Datos de Grafos](teoria.md) para profundizar en el modelo Labeled Property Graph (LPG), la adyacencia libre de índices (*Index-Free Adjacency*), el estándar Cypher y algoritmos de redes.

---

## Codelab Paso a Paso

Bienvenido al laboratorio práctico de **Bases de Datos de Grafos**. En este codelab aprenderás a diseñar, consultar y optimizar un grafo de propiedades enriquecido (*Labeled Property Graph*) utilizando **Neo4j** en su servicio gestionado en la nube: **Neo4j AuraDB (Capa Gratuita / Free Tier)**.

No necesitas instalar software en tu máquina local ni configurar servidores: todo el laboratorio se ejecuta desde la interfaz web interactiva **Neo4j Workspace / Query Editor**.

```
ARQUITECTURA DEL LABORATORIO (GRAPHCOMMERCE & SOCIAL NETWORK)

        ┌─────────────┐                      ┌─────────────┐
        │  Customer   │                      │  Customer   │
        │   (Alice)   │───[:FOLLOWS]────────►│    (Bob)    │
        └──────┬──────┘                      └──────┬──────┘
               │                                    │
          [:PURCHASED]                         [:PURCHASED]
               │                                    │
               ▼                                    ▼
        ┌─────────────┐                      ┌─────────────┐
        │   Product   │                      │   Product   │
        │(4K Monitor) │                      │(Mech Keys)  │
        └──────┬──────┘                      └──────┬──────┘
               │                                    │
         [:IN_CATEGORY]                       [:IN_CATEGORY]
               │                                    │
               └───────────────►┌─────────────┐◄────┘
                                │  Category   │
                                │  (Hardware) │
                                └─────────────┘
```

---

### Objetivos

Al terminar este laboratorio serás capaz de:

1. **Aprovisionar una base de datos de grafos en la nube** con Neo4j AuraDB Free sin costo ni tarjeta de crédito.
2. **Definir esquemas de integridad:** Crear restricciones de unicidad (*constraints*) e índices para acelerar búsquedas iniciales.
3. **Ingestar y modelar un grafo (LPG):** Crear nodos y relaciones con propiedades utilizando la cláusula idempotente `MERGE`.
4. **Comprobar empíricamente la ventaja de los grafos sobre SQL:** Comparar una consulta de recomendación social en Cypher (4 líneas) contra su equivalente relacional en SQL (múltiples `JOIN`s anidados y subconsultas de exclusión).
5. **Navegar relaciones de N-saltos y caminos mínimos:** Usar `shortestPath` para calcular grados de separación y rutas de influencia.
6. **Detectar patrones complejos y anillos sospechosos:** Descubrir identidades sintéticas y fraudes por recursos compartidos (dispositivos y tarjetas).
7. **Explorar visualmente el grafo:** Personalizar colores, tamaños y expandir relaciones interactivamente en Neo4j Workspace.
8. **Auditar el rendimiento:** Usar `EXPLAIN` y `PROFILE` para inspeccionar operadores internos (`NodeIndexSeek`, `Expand(All)`).
9. **FinOps y Limpieza:** Pausar o eliminar instancias en la nube al terminar la sesión.

---

### Prerrequisitos

- Un navegador web moderno (Google Chrome, Mozilla Firefox, Safari o Microsoft Edge).
- Una cuenta de correo electrónico o cuenta de Google / GitHub para registrarte en la consola de **Neo4j Aura**.
- **No se requiere tarjeta de crédito ni facturación.**
- Conocimientos básicos de bases de datos y SQL.

---

### Costo Estimado (FinOps)

| Concepto | Recurso en el Lab | Capa Gratuita (Free Tier) | Costo Total |
|---|---|---|---|
| **Base de Datos Neo4j** | 1 Instancia AuraDB Free | 1 instancia gratuita perpetua (hasta 200.000 nodos y 400.000 relaciones) | **$0.00 USD** |
| **Consola y Herramientas** | Neo4j Workspace / Browser | Incluido en Aura Free | **$0.00 USD** |
| **Infraestructura Local** | Cero cómputo local requerido | Ejecución 100% en la nube | **$0.00 USD** |

---

### Mapa del Laboratorio

```
Paso 0  Aprovisionar Neo4j AuraDB Free en la Nube
Paso 1  Crear Restricciones de Unicidad e Índices
Paso 2  Cargar el Grafo "GraphCommerce" con MERGE
Paso 3  Consultas Básicas de Exploración y Agregación
Paso 4  El Gran Duelo: SQL vs. Cypher (Ventaja de los Grafos)
Paso 5  Recomendaciones en Tiempo Real y Caminos Mínimos (shortestPath)
Paso 6  Detección de Patrones Complejos y Anillos de Fraude
Paso 7  Inspección del Plan de Ejecución con PROFILE y EXPLAIN
Paso 8  Exploración Visual Interactiva en Neo4j Workspace
Paso 9  Limpieza y Gestión FinOps
Retos   Retos de Extensión para el Estudiante
```

---

## Paso 0 — Aprovisionar Neo4j AuraDB Free en la Nube

Neo4j AuraDB es el servicio de base de datos de grafos totalmente gestionado (*DBaaS*) de Neo4j, disponible en Google Cloud Platform y AWS.

### 0.1. Registro en la consola de Neo4j Aura
1. Ingresa a la consola de Neo4j Aura: [https://console.neo4j.io/](https://console.neo4j.io/)
2. Inicia sesión con tu cuenta de **Google**, **GitHub** o mediante correo electrónico.

### 0.2. Crear la instancia gratuita (AuraDB Free)
1. En el panel principal (*Instances*), haz clic en el botón azul **"New Instance"**.
2. En la lista de opciones, selecciona la tarjeta **AuraDB Free**:
   - **Type:** Free
   - **Storage / Capacity:** Hasta 200k nodos y 400k relaciones.
   - **Costo:** $0 / mes (Free tier).
3. Asigna un nombre a tu instancia (por ejemplo: `analytics-lab-graph`).
4. Selecciona la región geográfica más cercana a ti.
5. Haz clic en **"Create Instance"**.

> [!IMPORTANT]
> **Descarga y guarda tus credenciales:**
> La consola generará automáticamente una contraseña aleatoria para el usuario por defecto `neo4j`.
> Aparecerá una ventana modal con un botón para **descargar el archivo de credenciales (`.env` o `.txt`)**.
> **Descárgalo de inmediato.** Neo4j no almacena esta contraseña y no podrás volver a verla.

```
EJEMPLO DE CREDENCIALES GENERADAS
URI:        neo4j+s://a1b2c3d4.databases.neo4j.io
Username:   neo4j
Password:   Xy7$kL9#mP2vQ8wZ
```

### 0.3. Conexión a Neo4j Workspace
1. Espera entre 1 y 2 minutos hasta que el estado de la instancia cambie de `Creating` a `Running` (con un indicador verde).
2. En la tarjeta de tu instancia, haz clic en el botón **"Open"** (o en la pestaña **"Query"** del menú superior).
3. Se abrirá **Neo4j Workspace**. Si te solicita autenticación, ingresa el usuario `neo4j` y la contraseña que guardaste en el paso anterior.
4. Ya estás en el editor interactivo de Cypher. Puedes escribir consultas en la barra superior y ejecutarlas con el botón **Play (▶)** o presionando `Ctrl + Enter` (o `Cmd + Enter` en Mac).

---

## Paso 1 — Crear Restricciones de Unicidad e Índices

Antes de cargar datos, es fundamental definir **restricciones de unicidad (*constraints*)**. En Neo4j, crear una restricción de unicidad sobre una etiqueta y propiedad genera automáticamente un **índice de búsqueda exacto**, garantizando dos cosas:
1. Integridad de datos: no existirán duplicados con el mismo identificador.
2. Rendimiento óptimo: operaciones de búsqueda inicial como `MATCH (c:Customer {id: 'C101'})` se resolverán en tiempo constante $O(1)$ o $O(\log N)$ mediante un índice de árbol antes de comenzar los saltos de memoria.

Copia y ejecuta cada bloque en el editor de Neo4j Workspace:

```cypher
-- 1. Restricción de unicidad para Clientes
CREATE CONSTRAINT customer_id_unique IF NOT EXISTS
FOR (c:Customer) REQUIRE c.id IS UNIQUE;
```

```cypher
-- 2. Restricción de unicidad para Productos
CREATE CONSTRAINT product_id_unique IF NOT EXISTS
FOR (p:Product) REQUIRE p.id IS UNIQUE;
```

```cypher
-- 3. Restricción de unicidad para Categorías
CREATE CONSTRAINT category_name_unique IF NOT EXISTS
FOR (cat:Category) REQUIRE cat.name IS UNIQUE;
```

```cypher
-- 4. Restricción de unicidad para Dispositivos y Tarjetas (entidades de seguridad)
CREATE CONSTRAINT device_id_unique IF NOT EXISTS
FOR (d:Device) REQUIRE d.id IS UNIQUE;

CREATE CONSTRAINT card_number_unique IF NOT EXISTS
FOR (cc:CreditCard) REQUIRE cc.number IS UNIQUE;
```

Para verificar las restricciones creadas:
```cypher
SHOW CONSTRAINTS;
```

> [!NOTE]
> Observa que cada `CONSTRAINT` creada tiene asociado un `INDEX` de tipo `RANGE`.

---

## Paso 2 — Cargar el Grafo "GraphCommerce" con MERGE

Vamos a poblar la base de datos con un ecosistema realista de e-commerce y red social. Usaremos la cláusula `MERGE`, que actúa como un *"get-or-create"*: si el nodo o relación ya existe, no lo duplica; si no existe, lo crea atómicamente.

### 2.1. Cargar Categorías y Productos
Ejecuta la siguiente consulta:

```cypher
// 1. Crear Categorías
MERGE (electronics:Category {name: 'Electronics'})
MERGE (gaming:Category {name: 'Gaming'})
MERGE (books:Category {name: 'Books'})
MERGE (lifestyle:Category {name: 'Lifestyle'})

// 2. Crear Productos y asociarlos a Categorías
MERGE (p1:Product {id: 'PROD-01', name: 'Mechanical Keyboard RGB', price: 120.00})
MERGE (p1)-[:IN_CATEGORY]->(gaming)

MERGE (p2:Product {id: 'PROD-02', name: 'Ultra-Wide Gaming Monitor 34"', price: 450.00})
MERGE (p2)-[:IN_CATEGORY]->(gaming)

MERGE (p3:Product {id: 'PROD-03', name: 'Wireless Ergonomic Mouse', price: 75.00})
MERGE (p3)-[:IN_CATEGORY]->(electronics)

MERGE (p4:Product {id: 'PROD-04', name: 'Designing Data-Intensive Applications', price: 45.00})
MERGE (p4)-[:IN_CATEGORY]->(books)

MERGE (p5:Product {id: 'PROD-05', name: 'Noise-Canceling Headphones', price: 280.00})
MERGE (p5)-[:IN_CATEGORY]->(electronics)

MERGE (p6:Product {id: 'PROD-06', name: 'Smart Fitness Watch', price: 199.00})
MERGE (p6)-[:IN_CATEGORY]->(lifestyle)

MERGE (p7:Product {id: 'PROD-07', name: 'Ergonomic Desk Chair', price: 320.00})
MERGE (p7)-[:IN_CATEGORY]->(lifestyle);
```

### 2.2. Cargar Clientes y Red de Seguimiento Social (FOLLOWS)
Ejecuta la siguiente consulta:

```cypher
// 1. Crear Clientes
MERGE (alice:Customer {id: 'CUST-01', name: 'Alice', city: 'Madrid', loyaltyLevel: 'Gold'})
MERGE (bob:Customer {id: 'CUST-02', name: 'Bob', city: 'Barcelona', loyaltyLevel: 'Silver'})
MERGE (charlie:Customer {id: 'CUST-03', name: 'Charlie', city: 'Valencia', loyaltyLevel: 'Platinum'})
MERGE (david:Customer {id: 'CUST-04', name: 'David', city: 'Madrid', loyaltyLevel: 'Bronze'})
MERGE (eve:Customer {id: 'CUST-05', name: 'Eve', city: 'Sevilla', loyaltyLevel: 'Silver'})
MERGE (frank:Customer {id: 'CUST-06', name: 'Frank', city: 'Bilbao', loyaltyLevel: 'Bronze'})
MERGE (grace:Customer {id: 'CUST-07', name: 'Grace', city: 'Madrid', loyaltyLevel: 'Gold'})

// 2. Crear Relaciones de Seguimiento (Red Social)
MERGE (alice)-[:FOLLOWS {since: date('2025-01-15')}]->(bob)
MERGE (alice)-[:FOLLOWS {since: date('2025-02-01')}]->(charlie)
MERGE (bob)-[:FOLLOWS {since: date('2025-02-10')}]->(david)
MERGE (charlie)-[:FOLLOWS {since: date('2025-03-01')}]->(eve)
MERGE (david)-[:FOLLOWS {since: date('2025-03-15')}]->(frank)
MERGE (eve)-[:FOLLOWS {since: date('2025-04-01')}]->(grace)
MERGE (frank)-[:FOLLOWS {since: date('2025-04-10')}]->(grace);
```

### 2.3. Cargar Transacciones de Compra (PURCHASED)
Cada compra vincula a un cliente con un producto, enriquecida con metadatos de la compra (`rating`, `date`, `orderId`):

```cypher
MATCH (alice:Customer {id: 'CUST-01'})
MATCH (bob:Customer {id: 'CUST-02'})
MATCH (charlie:Customer {id: 'CUST-03'})
MATCH (david:Customer {id: 'CUST-04'})
MATCH (eve:Customer {id: 'CUST-05'})
MATCH (frank:Customer {id: 'CUST-06'})
MATCH (grace:Customer {id: 'CUST-07'})

MATCH (p1:Product {id: 'PROD-01'}) // Mechanical Keyboard
MATCH (p2:Product {id: 'PROD-02'}) // Ultra-Wide Monitor
MATCH (p3:Product {id: 'PROD-03'}) // Ergonomic Mouse
MATCH (p4:Product {id: 'PROD-04'}) // Data-Intensive Apps Book
MATCH (p5:Product {id: 'PROD-05'}) // Headphones
MATCH (p6:Product {id: 'PROD-06'}) // Fitness Watch
MATCH (p7:Product {id: 'PROD-07'}) // Desk Chair

// Alice ha comprado el teclado y el libro
MERGE (alice)-[:PURCHASED {rating: 5, date: date('2025-05-01')}]->(p1)
MERGE (alice)-[:PURCHASED {rating: 5, date: date('2025-05-10')}]->(p4)

// Bob (a quien Alice sigue) ha comprado el teclado, el monitor y el mouse
MERGE (bob)-[:PURCHASED {rating: 4, date: date('2025-05-12')}]->(p1)
MERGE (bob)-[:PURCHASED {rating: 5, date: date('2025-05-15')}]->(p2)
MERGE (bob)-[:PURCHASED {rating: 4, date: date('2025-05-20')}]->(p3)

// Charlie (a quien Alice sigue) ha comprado el monitor y los auriculares
MERGE (charlie)-[:PURCHASED {rating: 5, date: date('2025-05-18')}]->(p2)
MERGE (charlie)-[:PURCHASED {rating: 4, date: date('2025-05-22')}]->(p5)

// David ha comprado el mouse y la silla ergonómica
MERGE (david)-[:PURCHASED {rating: 5, date: date('2025-06-01')}]->(p3)
MERGE (david)-[:PURCHASED {rating: 4, date: date('2025-06-05')}]->(p7)

// Eve ha comprado el reloj y los auriculares
MERGE (eve)-[:PURCHASED {rating: 5, date: date('2025-06-10')}]->(p6)
MERGE (eve)-[:PURCHASED {rating: 4, date: date('2025-06-12')}]->(p5)

// Grace ha comprado la silla y el monitor
MERGE (grace)-[:PURCHASED {rating: 5, date: date('2025-06-15')}]->(p7)
MERGE (grace)-[:PURCHASED {rating: 5, date: date('2025-06-18')}]->(p2);
```

### 2.4. Cargar Patrón de Fraude / Seguridad (Dispositivos y Tarjetas Compartidas)
Para demostrar la detección de identidades sintéticas y anillos sospechosos en el Paso 6, añadiremos nodos de dispositivos (`Device`) y tarjetas (`CreditCard`) compartidos:

```cypher
// Dos cuentas sospechosas operando desde la misma IP/Dispositivo con la misma tarjeta
MERGE (hacker1:Customer {id: 'CUST-98', name: 'Ghost_User_A', city: 'Unknown', loyaltyLevel: 'Bronze'})
MERGE (hacker2:Customer {id: 'CUST-99', name: 'Ghost_User_B', city: 'Unknown', loyaltyLevel: 'Bronze'})

MERGE (dev:Device {id: 'DEV-XYZ-900', os: 'Android 14', ip: '198.51.100.44'})
MERGE (card:CreditCard {number: '************4411', bank: 'NeoBank'})

MERGE (hacker1)-[:USES_DEVICE {firstSeen: date('2026-01-01')}]->(dev)
MERGE (hacker2)-[:USES_DEVICE {firstSeen: date('2026-01-02')}]->(dev)

MERGE (hacker1)-[:PAYS_WITH]->(card)
MERGE (hacker2)-[:PAYS_WITH]->(card);
```

---

## Paso 3 — Consultas Básicas de Exploración y Agregación

Comprobemos que los datos están correctamente cargados con algunas consultas fundamentales de Cypher.

### 3.1. Conteo global del inventario de grafos
```cypher
MATCH (n)
RETURN labels(n)[0] AS EntityType, count(n) AS TotalNodes
ORDER BY TotalNodes DESC;
```

*Resultado esperado:* Verás el recuento de nodos agrupados por etiqueta (`Customer`, `Product`, `Category`, `Device`, `CreditCard`).

### 3.2. Listar productos y sus categorías
```cypher
MATCH (p:Product)-[:IN_CATEGORY]->(cat:Category)
RETURN p.name AS Product, p.price AS Price, cat.name AS Category
ORDER BY Price DESC;
```

### 3.3. Clientes con mayor número de compras
```cypher
MATCH (c:Customer)-[:PURCHASED]->(p:Product)
RETURN c.name AS Customer, c.loyaltyLevel AS Tier, count(p) AS PurchasedItems
ORDER BY PurchasedItems DESC;
```

---

## Paso 4 — El Gran Duelo: SQL vs. Cypher (Ventaja de los Grafos)

Aquí radica el aprendizaje central del laboratorio: **experimentar por qué los grafos transforman radicalmente la complejidad del código y del cómputo**.

### El Requerimiento de Negocio:
> *"Recomendar a Alice productos comprados por personas a las que ella sigue directamente (`Alice -> FOLLOWS -> Amigo -> PURCHASED -> Producto`), pero descartando los productos que Alice ya haya comprado previamente. Además, mostrar cuántos de sus amigos compraron cada producto y la calificación promedio recibida."*

### Variante A: ¿Cómo se resuelve esto en SQL Relacional?
En una base de datos relacional estándar (PostgreSQL, MySQL, Oracle), el esquema normalizado requeriría 6 tablas:
- `customers`
- `follows` (tabla intermedia M:N)
- `orders`
- `order_items`
- `products`
- `categories`

El código SQL necesario sería el siguiente:

```sql
-- CONSULTA EN SQL RELACIONAL TRADICIONAL
SELECT
    p.id AS product_id,
    p.name AS product_name,
    COUNT(DISTINCT friend.id) AS friends_who_bought,
    ROUND(AVG(oi.rating), 2) AS avg_friend_rating
FROM customers me
JOIN follows f ON me.id = f.follower_id
JOIN customers friend ON f.followed_id = friend.id
JOIN orders o ON friend.id = o.customer_id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
WHERE me.name = 'Alice'
  -- Subconsulta correlacionada para excluir productos ya comprados por Alice
  AND p.id NOT IN (
      SELECT my_oi.product_id
      FROM orders my_o
      JOIN order_items my_oi ON my_o.id = my_oi.order_id
      WHERE my_o.customer_id = me.id
  )
GROUP BY p.id, p.name
ORDER BY friends_who_bought DESC, avg_friend_rating DESC;
```

#### Problemas del enfoque SQL:
1. **Sobrecarga de JOINs:** Requiere 6 operaciones `JOIN` y una subconsulta con `NOT IN` (o `NOT EXISTS`).
2. **Degradación con el volumen:** A medida que la tabla `order_items` acumula millones de filas, el optimizador de SQL debe hacer *scans* masivos o cargar índices gigantescos en memoria.
3. **Mantenibilidad:** Si agregamos un segundo nivel de recomendación (*amigos de amigos*), la consulta en SQL requiere duplicar los `JOIN`s a `follows`, volviéndose prácticamente inmanejable.

---

### Variante B: La Solución en Cypher (Neo4j)

Ejecuta la siguiente consulta en Neo4j Workspace:

```cypher
MATCH (alice:Customer {name: 'Alice'})-[:FOLLOWS]->(friend:Customer)-[buy:PURCHASED]->(p:Product)
WHERE NOT (alice)-[:PURCHASED]->(p)
RETURN
    p.name AS RecommendedProduct,
    p.price AS Price,
    count(DISTINCT friend) AS FriendsWhoBought,
    round(avg(buy.rating), 2) AS AvgFriendRating
ORDER BY FriendsWhoBought DESC, AvgFriendRating DESC;
```

#### ¿Por qué Cypher es superior en este escenario?
1. **Legibilidad visual:** El patrón `(alice)-[:FOLLOWS]->(friend)-[:PURCHASED]->(p)` describe la ruta exacta del dato tal como lo dibuja la mente humana.
2. **Exclusión natural:** La cláusula `WHERE NOT (alice)-[:PURCHASED]->(p)` es una verificación de punteros locales $O(1)$, sin necesidad de escanear subconsultas intermedias.
3. **Cero JOINs globales:** Neo4j navega directamente por los punteros de memoria (*Index-Free Adjacency*) entre los nodos de Alice, Bob, Charlie y los productos, sin consultar índices secundarios para cada salto.

*Resultado obtenido:*
Alice recibe la recomendación del **"Ultra-Wide Gaming Monitor 34\""** (comprado por 2 de sus amigos: Bob y Charlie, con rating 5.0) y del **"Wireless Ergonomic Mouse"** (comprado por Bob), excluyendo el Mechanical Keyboard que ella ya tiene.

---

## Paso 5 — Recomendaciones en Tiempo Real y Caminos Mínimos (shortestPath)

Una de las capacidades más potentes de las bases de datos de grafos es la navegación a **profundidad arbitraria (*variable-length paths*)** y el cálculo del **camino más corto**.

### 5.1. Recomendación a 2 saltos (Amigos de Mis Amigos)
¿Qué productos están comprando las personas que están a 2 saltos de Alice (`Alice -> FOLLOWS -> Amigo -> FOLLOWS -> AmigoDeAmigo -> PURCHASED -> Producto`)?

```cypher
MATCH (alice:Customer {name: 'Alice'})-[:FOLLOWS*2]->(fof:Customer)-[buy:PURCHASED]->(p:Product)
WHERE NOT (alice)-[:PURCHASED]->(p)
RETURN
    p.name AS DeepRecommendation,
    fof.name AS Influencer,
    buy.rating AS Rating
ORDER BY Rating DESC;
```

> 💡 **Nota la sintaxis `[:FOLLOWS*2]`:** Permite especificar exactamente dos saltos de relación sin tener que escribir variables intermedias adicionales. En SQL esto exigiría dos uniones explícitas a la tabla de amistades.

### 5.2. Encontrar el Camino Más Corto (`shortestPath`)
Supongamos que el equipo comercial quiere saber: *"¿Cuál es la cadena de contactos más corta para que Alice pueda llegar a Grace?"*

Ejecuta:

```cypher
MATCH (alice:Customer {name: 'Alice'}), (grace:Customer {name: 'Grace'})
MATCH path = shortestPath((alice)-[:FOLLOWS*..10]->(grace))
RETURN
    path,
    length(path) AS DegreesOfSeparation,
    [n IN nodes(path) | n.name] AS ConnectionChain;
```

*Resultado esperado:*
Neo4j encuentra la ruta óptima: `Alice -> Charlie -> Eve -> Grace` (3 grados de separación).
Hacer esto en una base de datos relacional requeriría escribir una expresión de tabla común recursiva (`WITH RECURSIVE`), extremadamente costosa en CPU y con alto riesgo de bucles infinitos.

---

## Paso 6 — Detección de Patrones Complejos y Anillos de Fraude

En la banca y el comercio digital, los atacantes suelen crear identidades sintéticas utilizando nombres y correos falsos, pero reutilizan recursos físicos (la misma tarjeta de crédito, número de teléfono o dirección MAC del dispositivo).

### 6.1. Detección de Cuentas Conectadas por Dispositivo Compartido
Identificar pares de clientes que comparten el mismo dispositivo físico o dirección IP:

```cypher
MATCH (c1:Customer)-[:USES_DEVICE]->(dev:Device)<-[:USES_DEVICE]-(c2:Customer)
WHERE c1.id < c2.id  // Evita comparar un nodo consigo mismo y elimina resultados duplicados invertidos
RETURN
    c1.name AS UserA,
    c2.name AS UserB,
    dev.id AS SharedDevice,
    dev.ip AS SharedIP,
    dev.os AS OperatingSystem;
```

### 6.2. Detección de Anillo de Fraude Completo (Dispositivo + Medio de Pago)
Detectar si esas mismas cuentas sospechosas también comparten la misma tarjeta de crédito:

```cypher
MATCH (c1:Customer)-[:USES_DEVICE]->(dev:Device)<-[:USES_DEVICE]-(c2:Customer)
MATCH (c1)-[:PAYS_WITH]->(card:CreditCard)<-[:PAYS_WITH]-(c2)
WHERE c1.id < c2.id
RETURN
    c1.name AS Account1,
    c2.name AS Account2,
    dev.ip AS SuspiciousIP,
    card.number AS CompromisedCard,
    "ALERTA: Posible anillo de identidad sintética" AS Status;
```

> 🛡️ **Aplicación en Producción:** Este tipo de consulta en Neo4j se ejecuta como un filtro de validación en tiempo real en la pasarela de pagos. Si el patrón devuelve coincidencias en menos de 10 milisegundos, la transacción se bloquea antes de autorizarse.

---

## Paso 7 — Inspección del Plan de Ejecución con PROFILE y EXPLAIN

Al igual que en BigQuery y PostgreSQL, en Neo4j es vital saber cómo el motor ejecuta la consulta.

- `EXPLAIN`: Muestra el plan de ejecución estimado por el optimizador de costos sin ejecutar la consulta.
- `PROFILE`: **Ejecuta la consulta** y devuelve las métricas reales de filas procesadas (*Rows*) y operaciones de memoria física (*db hits*).

Ejecuta el siguiente comando:

```cypher
PROFILE
MATCH (alice:Customer {name: 'Alice'})-[:FOLLOWS]->(friend:Customer)-[:PURCHASED]->(p:Product)
WHERE NOT (alice)-[:PURCHASED]->(p)
RETURN p.name, count(*) AS total;
```

### ¿Qué observar en el panel de resultados?
Haz clic en el icono del **árbol de ejecución** que aparece en el panel de resultados de Neo4j Workspace:

```
VISUALIZACIÓN DEL OPERADOR PROFILE EN NEO4J
┌────────────────────────────────────────────────────────┐
│ ProduceResults (Variables finales)                     │
└───────────────────────────▲────────────────────────────┘
                            │
┌───────────────────────────┴────────────────────────────┐
│ Filter (NOT (alice)-[:PURCHASED]->(p))                 │
└───────────────────────────▲────────────────────────────┘
                            │
┌───────────────────────────┴────────────────────────────┐
│ Expand(All) (friend)-[:PURCHASED]->(p)                 │
└───────────────────────────▲────────────────────────────┘
                            │
┌───────────────────────────┴────────────────────────────┐
│ Expand(All) (alice)-[:FOLLOWS]->(friend)               │
└───────────────────────────▲────────────────────────────┘
                            │
┌───────────────────────────┴────────────────────────────┐
│ NodeIndexSeek (c:Customer WHERE name = 'Alice')        │
└────────────────────────────────────────────────────────┘
```

1. **`NodeIndexSeek` en la base:** Neo4j utiliza el índice creado en el Paso 1 para localizar el nodo de Alice en tiempo instantáneo.
2. **`Expand(All)`:** A partir de Alice, el motor no vuelve a tocar ningún índice; simplemente recorre los punteros físicos directos (*db hits*) a sus amigos y a los productos comprados.
3. **Métrica "db hits":** Representa el número de accesos al subsistema de almacenamiento de registros. En bases de datos optimizadas de grafos, los *db hits* se mantienen extremadamente bajos en comparación con los scans relacionales.

---

## Paso 8 — Exploración Visual Interactiva en Neo4j Workspace

Una gran fortaleza de Neo4j es su capacidad de **exploración visual inmediata**:

1. En el editor de Neo4j Workspace, escribe y ejecuta:
   ```cypher
   MATCH (c:Customer)-[r]-(target)
   RETURN c, r, target
   LIMIT 50;
   ```
2. Observa la pestaña de vista **"Graph"** (ícono de burbujas interactivas).
3. **Interactividad:**
   - Haz clic y arrastra cualquier nodo para reorganizar la física de la red.
   - Haz **doble clic** sobre cualquier nodo (por ejemplo, el nodo de `Bob` o de un `Product`) para expandir automáticamente todas sus relaciones en vivo sin escribir más código.
4. **Personalización de estilos:**
   - En el panel derecho de visualización, selecciona la etiqueta `Customer`. Puedes cambiar el color del círculo (por ejemplo, azul) y elegir qué propiedad mostrar como texto visible (`name`).
   - Selecciona `Product` y cambia su color a verde, mostrando la propiedad `name`.
   - Selecciona `Category` y dale un color amarillo/naranja.
5. **Exportar:** Puedes exportar el grafo generado como archivo vectorial SVG o imagen PNG haciendo clic en el botón de descarga del visor.

---

## Paso 9 — Limpieza y Gestión FinOps

Para garantizar una administración responsable de los recursos de la nube y mantener el entorno dentro de la política de **Costo Cero**:

> [!WARNING]
> **Comportamiento automático de AuraDB Free:** Las instancias de la capa gratuita se **pausan automáticamente tras 72 horas de inactividad** (sin conexiones ni consultas). Si una instancia permanece pausada durante **más de 90 días** sin ser reanudada, Neo4j **la elimina de forma permanente** junto con todos sus datos. Si planeas continuar el laboratorio en otra sesión, reanuda la instancia periódicamente desde la consola o exporta un respaldo de tus datos.

### Opción 1: Pausar la Instancia (Recomendado si quieres continuar explorando después)
En la consola de Neo4j Aura ([https://console.neo4j.io/](https://console.neo4j.io/)):
1. Ubica tu instancia `analytics-lab-graph`.
2. Haz clic en el menú de tres puntos (`...`) a la derecha de la tarjeta.
3. Selecciona **"Pause"**. La instancia se detendrá y no consumirá cómputo. Podrás reanudarla con un clic cuando desees.

### Opción 2: Eliminar la Instancia (Si has completado todo el ejercicio)
1. En el menú de tres puntos (`...`), selecciona **"Delete"**.
2. Escribe el nombre de la instancia para confirmar la eliminación definitiva.

> 💡 **Limpieza interna de datos sin borrar la instancia:**
> Si deseas vaciar todos los nodos y relaciones creados para empezar de nuevo con otro ejercicio sin destruir la base de datos, ejecuta en Neo4j Workspace:
> ```cypher
> MATCH (n)
> DETACH DELETE n;
> ```
> *(La cláusula `DETACH DELETE` borra los nodos junto con todas sus relaciones conectadas).*

---

## Retos Opcionales (Extensión para Estudiantes)

Pon a prueba lo aprendido resolviendo los siguientes retos en Cypher:

### Reto 1: Identificar al "Super-Influencer" (Degree Centrality)
Escribe una consulta Cypher que determine qué cliente tiene el mayor número de seguidores (`FOLLOWS` entrantes) y qué nivel de fidelidad (*loyaltyLevel*) tiene.
<details>
<summary>👀 Ver Solución Reto 1</summary>

```cypher
MATCH (c:Customer)<-[:FOLLOWS]-(follower:Customer)
RETURN c.name AS Influencer, c.loyaltyLevel AS Tier, count(follower) AS TotalFollowers
ORDER BY TotalFollowers DESC
LIMIT 1;
```
</details>

### Reto 2: Análisis de la Cesta de Compra (Market Basket Analysis)
Encuentra qué pares de productos se compran frecuentemente juntos por los mismos clientes: *"Clientes que compraron el producto X también compraron el producto Y"*.
<details>
<summary>👀 Ver Solución Reto 2</summary>

```cypher
MATCH (p1:Product)<-[:PURCHASED]-(c:Customer)-[:PURCHASED]->(p2:Product)
WHERE p1.id < p2.id
RETURN
    p1.name AS ProductA,
    p2.name AS ProductB,
    count(DISTINCT c) AS BoughtTogetherTimes
ORDER BY BoughtTogetherTimes DESC;
```
</details>

### Reto 3: Camino de Conexión Más Corto no Dirigido
Encuentra la conexión social más corta entre `Alice` y `Frank`, considerando las relaciones de seguimiento en cualquier dirección (`-[:FOLLOWS]-`).
<details>
<summary>👀 Ver Solución Reto 3</summary>

```cypher
MATCH (alice:Customer {name: 'Alice'}), (frank:Customer {name: 'Frank'})
MATCH path = shortestPath((alice)-[:FOLLOWS*]-(frank))
RETURN
    [n IN nodes(path) | n.name] AS NetworkRoute,
    length(path) AS TotalHops;
```
</details>

---

## Resumen de lo Aprendido

- **Index-Free Adjacency:** Neo4j trata las relaciones como estructuras de punteros físicos en memoria $O(1)$, eliminando la penalización exponencial de los `JOIN`s en RDBMS.
- **Declaratividad Cypher:** Expresar patrones visuales con ASCII-art reduce decenas de líneas complejas de SQL a consultas intuitivas y directas.
- **Casos de éxito:** Recomendaciones hiper-personalizadas en tiempo real, detección de fraudes e identidades compartidas, y análisis de caminos críticos son tareas naturales para las bases de datos de grafos.
