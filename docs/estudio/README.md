# Guía de estudio — flowforge

Esta carpeta es una guía en español para entender flowforge a fondo. El código,
los comentarios y la documentación técnica están en inglés (nivel B2) a propósito,
para que el proyecto sea creíble y sirva para practicar. Esta guía explica el "por
qué" de cada decisión en tu idioma.

## Orden sugerido de lectura

1. **[01-dag-y-kahn.md](01-dag-y-kahn.md)** — qué es un DAG, el algoritmo de Kahn
   y cómo da validación, orden topológico y capas paralelas de una sola pasada.
2. **[02-ejecucion-reintentos-y-skips.md](02-ejecucion-reintentos-y-skips.md)** —
   el modelo de ejecución: reintentos por paso, el fallo que cascada como skips, y
   la API "solo estructura".
3. **[03-preguntas-de-entrevista.md](03-preguntas-de-entrevista.md)** — preguntas
   típicas y cómo responderlas con seguridad.
4. **[04-pipeline-de-datos-reales.md](04-pipeline-de-datos-reales.md)** — un
   pipeline de datos reales (demanda eléctrica de España) modelado como DAG: cómo se
   mapea, sus capas y orden topológico paso a paso con Kahn, reintentos y
   propagación de fallos, y un ejemplo trabajado con las capas y la salida reales.

## Resumen en una frase

flowforge es un motor de orquestación de flujos sobre un DAG: valida el grafo de
dependencias (dependencias inexistentes, ciclos) con el algoritmo de Kahn, agrupa
los pasos en capas paralelizables, y los ejecuta con reintentos por paso y una
semántica de "skip" que hace cascada ante un fallo — sin ML ni LLM, ingeniería pura.
