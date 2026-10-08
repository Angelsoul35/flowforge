# 01 — El DAG y el algoritmo de Kahn

## Qué es un DAG

Un flujo de trabajo (workflow) es un **grafo dirigido acíclico** (DAG): los pasos
son nodos y las dependencias son aristas dirigidas ("deploy depende de package").
"Acíclico" es clave: si hubiera un ciclo (A depende de B que depende de A), nunca
podrías empezar. Un motor de CI o un task runner es, en el fondo, esto.

## Tres cosas que hay que hacer al grafo antes de ejecutar

1. **Validarlo:** ¿toda dependencia nombra un paso real?, ¿hay algún ciclo?
2. **Ordenarlo:** un orden que respete las dependencias (orden topológico).
3. **Paralelizarlo:** ¿qué pasos pueden correr a la vez?

Podrían ser tres pasadas distintas. Pero el **algoritmo de Kahn** las da las tres
de una sola pasada.

## El algoritmo de Kahn

Procesa el grafo quitando repetidamente los nodos sin dependencias pendientes:

1. Calcula el **grado de entrada** de cada paso (cuántas dependencias distintas
   tiene).
2. Los pasos con grado 0 forman la siguiente **capa**; emítela.
3. Resta 1 al grado de entrada de sus dependientes; los que lleguen a 0 forman la
   capa siguiente. Repite.
4. Si al final se emitieron **menos** pasos de los que hay, los que sobran tienen
   un **ciclo** (se reportan).

De una pasada salen las tres cosas:

- **Validez:** un ciclo es exactamente "no se resolvieron todos los pasos".
- **Orden topológico:** las capas aplanadas.
- **Capas paralelas:** cada grupo emitido. Los pasos de una misma capa no tienen
  camino entre sí, así que es **seguro ejecutarlos en paralelo**.

Ejemplo: `[[build], [lint, test], [package], [deploy]]`. `lint` y `test` están en
la misma capa → pueden ir a la vez.

## Detalles que importan

- Las dependencias se **deduplican** (`set`), para que una arista repetida no
  distorsione el grado de entrada.
- Las capas se emiten **ordenadas**, para que el resultado sea **determinista**
  (mismo workflow → mismas capas y mismo orden), vital para runs y pruebas
  reproducibles.

## Para fijar la idea

- Workflow = DAG (pasos = nodos, dependencias = aristas), sin ciclos.
- Kahn (quitar nodos de grado 0) da validación + orden topológico + capas
  paralelas de una pasada.
- Determinista: capas ordenadas, dependencias deduplicadas.
