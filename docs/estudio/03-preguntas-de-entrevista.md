# 03 — Preguntas de entrevista

Preguntas típicas sobre este proyecto y cómo responderlas con seguridad.
Practica diciéndolas en voz alta en inglés también.

## Sobre el grafo

**¿Por qué el algoritmo de Kahn?**
Porque de una sola pasada me da las tres cosas que necesito antes de ejecutar:
validación (un ciclo es "no se resolvieron todos los nodos"), orden topológico
(las capas aplanadas) y las capas paralelas (cada ronda de nodos de grado 0). Menos
código y resultados consistentes por construcción.

**¿Cómo detectas un ciclo?**
Con el propio Kahn: si al terminar se emitieron menos pasos de los que hay, los que
quedan con grado de entrada > 0 están en un ciclo, y los reporto.

**¿Qué hace que la salida sea determinista?**
Emito las capas ordenadas y deduplico dependencias, así que el mismo workflow da
siempre las mismas capas y el mismo orden. Importa para runs y tests reproducibles.

## Sobre la ejecución

**¿Qué pasa cuando un paso falla?**
Capturo la excepción dentro del motor, lo marco `failed`, y marco `skipped` a sus
dependientes (transitivos), guardando qué los bloqueó. El run continúa y termina
con un informe completo, no con un stack trace. Las ramas independientes siguen.

**¿Por qué reintentos por paso y no globales?**
Porque lo inestable (un test flaky) conviene reintentarlo, pero lo determinista
debe fallar rápido para no esconder bugs. Cada paso define su `max_retries`; se
intenta `1 + max_retries` veces y se guardan los intentos (señal de flakiness).

**¿Cómo corres pasos en paralelo sin romper la corrección?**
Solo dentro de una capa (pasos sin dependencia entre sí), con un ThreadPoolExecutor
acotado por `max_parallel`, y guardo resultados por nombre, así la concurrencia no
afecta el orden reportado ni la corrección.

## Sobre seguridad

**¿Por qué la API no acepta workflows ejecutables?**
Porque un cuerpo de paso es código arbitrario, y ejecutar código arbitrario de un
cliente por HTTP es RCE. Separo: `/validate` acepta solo estructura (sin ejecutar)
y `/run/{name}` corre workflows que vienen en el servidor. La ejecución arbitraria
se queda en la librería, para código de confianza.

## Pregunta trampa

**Tu scheduling por capas: ¿un paso lento no retrasa toda la capa siguiente?**
Sí, un planificador puro por dependencias arrancaría cada paso en cuanto terminen
las suyas, sin esperar a toda la capa. Elegí capas porque son mucho más simples de
razonar y probar, y son deterministas. El modelo de capas deja sitio para un
scheduling más fino después sin tocar la validación. Para la mayoría de pipelines,
la diferencia es pequeña y la simplicidad gana.
