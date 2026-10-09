# 04 — Un pipeline de datos reales como DAG

Los tres capítulos anteriores explican el motor en abstracto. Este lo pone a
trabajar sobre **datos reales**: la demanda eléctrica horaria de España (datos
públicos de ENTSO-E / Red Eléctrica de España, vía Hugging Face). El objetivo es
ver cómo un pipeline de datos de verdad —con sus huecos, su validación y sus
agregaciones— se convierte en un DAG que el motor valida, ordena, paraleliza y
ejecuta con las mismas garantías de siempre.

El código está en `flowforge/real_pipeline.py`; se ejecuta con
`python -m flowforge.real_benchmark`.

## De un pipeline real a un DAG

Un pipeline de datos es una secuencia de **transformaciones**, cada una con sus
entradas y salidas. Si dibujas qué transformación necesita el resultado de cuál,
obtienes un grafo dirigido: justo lo que el motor sabe manejar. Nuestro pipeline
tiene siete etapas:

| Paso            | Qué hace (transformación real)                                   | Depende de            |
| --------------- | ---------------------------------------------------------------- | --------------------- |
| `fetch`         | Carga el slice real (168 horas) desde el disco o la API          | —                     |
| `clean`         | Parsea fechas, convierte números, **interpola** los huecos       | `fetch`               |
| `validate`      | Verifica esquema, cadencia horaria y fracción de nulos; **lanza** si algo falla | `fetch`      |
| `aggregate`     | Agrupa de horario a **diario** (total, media, pico, renovables)  | `clean`, `validate`   |
| `summary_stats` | Estadísticos de la semana (total, día de más/menos demanda)      | `aggregate`           |
| `detect_peaks`  | Pico de demanda por día y de toda la semana                      | `aggregate`           |
| `report`        | Arma el informe de texto final                                   | `summary_stats`, `detect_peaks` |

El grafo que sale de esa columna "depende de":

```
fetch ──▶ clean ────┐
      └─▶ validate ─┴─▶ aggregate ──▶ summary_stats ─┐
                                    └▶ detect_peaks ──┴─▶ report
```

### El detalle clave: los pasos comparten un "workspace"

El motor exige que cada acción sea un **callable sin argumentos** (`Callable[[],
object]`) y **no pasa** el valor de retorno de un paso al siguiente. ¿Cómo fluyen
entonces los datos? Con un objeto de estado compartido, `PipelineState`: `fetch`
escribe `state.raw`, `clean` lee `state.raw` y escribe `state.observations`, y así.
Es exactamente lo que hace un runner de CI con un directorio de build que todas las
etapas comparten. El motor se mantiene genérico (no sabe nada de electricidad); el
pipeline mete su dominio en las clausuras que capturan `state`.

Esto importa para entender por qué las garantías del motor siguen valiendo: el
grafo de dependencias que el motor ve es real (las aristas de la tabla), aunque los
datos viajen "por fuera", por el workspace.

## Capas, paralelismo y orden topológico, con el algoritmo

El motor aplica el **algoritmo de Kahn** (cap. 01) a este grafo. Recordatorio: el
**grado de entrada** de un nodo es cuántas dependencias distintas tiene. Se emiten
en capas los nodos de grado 0, se restan sus aristas, y se repite. Veámoslo paso a
paso con el pipeline real.

Grados de entrada iniciales:

```
fetch=0  clean=1  validate=1  aggregate=2  summary_stats=1  detect_peaks=1  report=2
```

1. **Grado 0:** solo `fetch`. Se emite la capa `[fetch]`. Al quitarlo, bajan
   `clean` (1→0) y `validate` (1→0).
2. **Grado 0 ahora:** `clean` y `validate`. Capa `[clean, validate]` (ordenada).
   Al quitarlos, `aggregate` baja 2→1 (por `clean`) y 1→0 (por `validate`).
3. **Grado 0:** `aggregate`. Capa `[aggregate]`. Bajan `summary_stats` (1→0) y
   `detect_peaks` (1→0).
4. **Grado 0:** `detect_peaks` y `summary_stats`. Capa `[detect_peaks,
   summary_stats]` (ordenada). `report` baja 2→1→0.
5. **Grado 0:** `report`. Capa `[report]`.

Se resolvieron 7 de 7 pasos → no hay ciclo. Las **capas** son:

```
[['fetch'], ['clean', 'validate'], ['aggregate'], ['detect_peaks', 'summary_stats'], ['report']]
```

Y el **orden topológico** (capas aplanadas):

```
['fetch', 'clean', 'validate', 'aggregate', 'detect_peaks', 'summary_stats', 'report']
```

Dos cosas que leer en ese resultado:

- **`clean` y `validate` caen en la misma capa.** No tienen camino entre sí (ambos
  solo dependen de `fetch`), así que es **seguro ejecutarlos en paralelo**. Lo
  mismo con `summary_stats` y `detect_peaks`. El motor lo descubrió **solo de las
  dependencias**: nadie le dijo "esto va en paralelo".
- **El paralelismo real** lo da el `ThreadPoolExecutor` acotado por `max_parallel`:
  dentro de una capa de dos pasos independientes, ambos corren a la vez; entre capas
  se respeta el orden de dependencias.

## Reintentos y propagación de fallos en este DAG

Las garantías del motor (cap. 02) se demuestran sobre este mismo grafo real.

### Un ciclo se rechaza antes de ejecutar

Si añadimos una arista de vuelta `report → fetch`, el grafo deja de ser acíclico.
Kahn nunca llega a grado 0 en ninguno (todos dependen de alguien), así que resuelve
menos pasos de los que hay y el motor lanza `DagError` **antes de ejecutar un solo
paso**. Ningún dato se toca.

### Un fallo con reintentos: cascada solo a los dependientes

Hagamos que `detect_peaks` falle siempre, con `max_retries=2`. El motor:

1. Lo intenta `1 + 2 = 3` veces; como todas lanzan, lo marca `failed` y guarda el
   último error.
2. `report` depende de `detect_peaks`, así que queda **`skipped`**, anotando que lo
   bloqueó `detect_peaks`.
3. Pero `summary_stats` **no** depende de `detect_peaks` (es una rama
   independiente de la misma capa): **se ejecuta y tiene éxito**.
4. Todo lo de aguas arriba (`fetch`, `clean`, `validate`, `aggregate`) también
   terminó bien.

El run acaba con estado global `failed`, pero con un **informe completo**: un
estado por cada paso, la causa a un salto de distancia, y las ramas sanas
completadas. Eso es lo contrario de reventar con un stack trace a la primera. La
diferencia entre reintentar y no: `clean`/`validate`/`aggregate` son deterministas
(fallar rápido es lo correcto), pero un paso que, por ejemplo, llamara a la API en
vivo sí pondría `max_retries>0` para absorber un blip de red.

## Ejemplo trabajado: capas y salida reales

Al correr `python -m flowforge.real_benchmark` sobre el slice vendorizado (7 días,
del 2015-01-01 al 2015-01-07), el motor imprime las capas de arriba y este
resultado real:

```
Spain hourly electricity demand — weekly report
  window         : 2015-01-01 .. 2015-01-07 (7 days)
  week total     : 4,463,717 MWh
  mean daily     : 637,674 MWh
  highest demand : 2015-01-07 (770,520 MWh)
  lowest demand  : 2015-01-05 (545,021 MWh)
  week peak hour : 2015-01-07T17:00:00+01:00 (36,195 MW)
  mean renewables: 32.6% of demand (solar + onshore wind)
  per-day:
    2015-01-02  total   654,031 MWh  mean   27,251 MW  peak   33,668 MW @ 21:00  renew 45.8%
    ...
```

Cómo leer los números, y de dónde salen:

- **`total ... MWh` por día.** Cada lectura horaria está en MW (potencia media de
  esa hora); como dura una hora, sumar las 24 lecturas da la **energía** del día en
  MWh. Para el 2015-01-02, las 24 horas suman **654.031 MWh** — un valor que el test
  `test_known_daily_total_matches_independent_reference` fija, calculado a mano
  desde el fichero vendorizado. Si una mutación rompe `aggregate`, ese test cae.
- **El hueco real de 6 horas (2015-01-05, 12:00–17:00).** Los datos reales traen
  nulos. `clean` los **interpola linealmente** entre el vecino válido de la
  izquierda (11:00) y el de la derecha (18:00); el mediodía del 05 queda en 24.066
  MW. `validate`, en paralelo, comprueba que la fracción de nulos (6/168 ≈ 3,6 %)
  esté por debajo del 10 % antes de dejar agregar.
- **`week peak hour`.** `detect_peaks` recorre las 168 observaciones y encuentra el
  máximo: el 2015-01-07 a las 17:00, con 36.195 MW (tarde de invierno, pico de
  calefacción e iluminación). El día de más demanda total es ese mismo 07.
- **`renewables`.** Solar + eólica terrestre sobre la demanda del día; varía mucho
  (del 17 % al 46 %) según el viento, lo que ilustra por qué estos datos son un
  caso de prueba honrado para un pipeline.

## Para fijar la idea

- Un pipeline de datos = un DAG: transformaciones (nodos) y "quién necesita a quién"
  (aristas). El motor no cambia; el dominio vive en las clausuras sobre un
  `PipelineState` compartido (como el directorio de build de un CI).
- Kahn, sobre el grafo real, da en una pasada: validez (sin ciclo), orden
  topológico y las capas paralelas `[[fetch],[clean,validate],[aggregate],
  [detect_peaks,summary_stats],[report]]`.
- `clean`/`validate` y `summary_stats`/`detect_peaks` corren en paralelo porque son
  independientes; el motor lo deduce solo de las dependencias.
- Las garantías valen sobre datos reales: el ciclo se rechaza sin ejecutar; un fallo
  con reintentos se intenta `1+max_retries` veces y luego salta **solo** a sus
  dependientes, dejando correr las ramas independientes; el run termina con un
  informe completo.
- La salida es real y verificada: el total diario, el pico semanal y los agregados
  están fijados en tests contra valores calculados de forma independiente.
