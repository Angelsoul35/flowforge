# 02 — Ejecución: reintentos y skips

## Ejecución capa por capa

El motor recorre las capas en orden. Para cada capa, primero **parte** los pasos:

- un paso cuyas dependencias **todas** tuvieron éxito es **ejecutable**;
- un paso con **alguna** dependencia no exitosa se marca **skipped** al momento,
  guardando qué dependencias lo bloquearon.

Como las capas van en orden de dependencia, cuando se considera un paso sus
dependencias ya tienen resultado, así que la decisión de skip siempre está bien
definida y los skips **cascada** transitivamente (un paso saltado porque su
dependencia fue saltada).

Los pasos ejecutables de una capa corren en un `ThreadPoolExecutor` limitado por
`max_parallel` (secuencial si el límite o la capa es 1). Los resultados se guardan
por nombre, así que la concurrencia no afecta la corrección ni el orden reportado.

## Reintentos por paso

Un paso se intenta `1 + max_retries` veces; gana el primer intento cuya acción no
lanza excepción, y se guarda el número de intentos. Si todos lanzan, el paso es
**failed** y se captura el último error.

¿Por qué por paso y no global? Porque algunos pasos son inestables (un test de
integración con un blip de red) y conviene reintentarlos; otros son deterministas
y deben **fallar rápido**, porque reintentarlos solo esconde un bug real. Un paso
no inestable deja `max_retries=0` y falla al primer error.

## El fallo cascada como skips, no como crash

La decisión clave: la excepción de un paso se **captura dentro del motor** y se
convierte en un resultado `failed`; sus dependientes (transitivos) se marcan
`skipped`. El run **continúa**. El estado global es `failed` si algún paso falló,
si no `success`.

Así el run **siempre termina con un informe completo** (un estado por cada paso),
en vez de reventar a la primera con un stack trace. Y un paso saltado dice qué
dependencia lo bloqueó: la causa raíz está a un salto. Las ramas independientes de
capas posteriores siguen corriendo; solo se saltan los descendientes del que falló.

## La API "solo estructura"

Los pasos llevan callables de Python, que **no** se pueden (ni se deben) mandar por
HTTP: ejecutar código arbitrario de un cliente en un servicio web es ejecución
remota de código (RCE). Por eso la API se parte: `/validate` acepta una spec
declarativa (nombres y dependencias) y devuelve las capas sin ejecutar nada, y
`/run/{name}` ejecuta un workflow **que viene en el código del servidor**. La
ejecución arbitraria vive en la librería, usada por código de confianza.

## Para fijar la idea

- Ejecución por capas; dentro de una capa, en paralelo (acotado).
- Reintentos **por paso**: lo inestable se reintenta, lo determinista falla rápido.
- Fallo = `failed` + dependientes `skipped`, el run continúa → informe completo.
- API solo estructura (sin RCE): validar specs y correr workflows internos.
