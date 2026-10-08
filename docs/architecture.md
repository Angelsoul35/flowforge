# Architecture

flowforge validates a dependency graph, groups it into parallel layers, and runs
it. This document covers the algorithms and the execution model.

## The pieces

| Layer     | Module                | Responsibility                                      |
| --------- | --------------------- | --------------------------------------------------- |
| Model     | `flowforge/models.py` | Step, status, step result, run report               |
| DAG       | `flowforge/dag.py`    | Validation, topological order, parallel layers      |
| Engine    | `flowforge/engine.py` | Layer-by-layer execution with retries and skips     |
| Samples   | `flowforge/samples.py`| Example workflows for the demo and tests            |
| HTTP API  | `flowforge/main.py`   | Structure-only `/validate` and built-in `/run`      |
| CLI       | `flowforge/cli.py`    | Demo                                                |

## Kahn's algorithm: validation and layering in one pass

A workflow is a directed graph (steps → their dependents). Kahn's algorithm
processes it by repeatedly removing the nodes with in-degree zero:

1. Compute each step's in-degree (number of distinct dependencies).
2. The steps with in-degree 0 form the next **layer**; emit it.
3. Decrement the in-degree of their dependents; any that reach 0 form the next
   layer. Repeat.
4. If, at the end, fewer steps were emitted than exist, the leftovers have a
   dependency cycle — reported with the involved steps.

This single pass gives three things at once: a **validity check** (a cycle is
exactly "not all steps were resolved"), a **topological order** (the layers
flattened), and the **parallel layers** (each emitted group). Steps within a
layer have no path between them, so running them concurrently is safe.

Dependencies and dependents are deduplicated (`set(depends_on)`) so a repeated
edge does not distort in-degree, and layers are emitted in sorted order so the
output is deterministic.

## Execution model

The engine walks the layers in order. For each layer it first partitions the
steps:

- a step whose dependencies **all** succeeded is **runnable**;
- a step with any non-successful dependency is **skipped** immediately, recording
  the blocking dependencies.

Because layers are processed in dependency order, a step's dependencies always
have results by the time it is considered, so the skip decision is always
well-defined and skips cascade transitively.

Runnable steps in a layer execute on a `ThreadPoolExecutor` bounded by
`max_parallel` (sequentially when the bound or the layer size is 1). Results are
keyed by name, so concurrency does not affect correctness or the reported order.

### Retries and failure isolation

A step is attempted `1 + max_retries` times; the first attempt whose action does
not raise succeeds, and the attempt count is recorded. If every attempt raises,
the step is **failed** and the last error string is captured. Crucially, a step's
exception is caught inside the engine — one failing step never aborts the run; it
turns into a `failed` result, and its dependents become `skipped`. This is what
makes a workflow run produce a complete report rather than a stack trace.

## Why the API is structure-only

Steps carry Python callables, which cannot (and must not) be sent over HTTP:
executing caller-supplied code in a web service is remote code execution. So the
API splits the two things a caller legitimately wants remotely — `/validate`
checks a declarative spec (names and dependencies) and returns its layers without
running anything, and `/run/{name}` executes a workflow that **ships with the
server**. Arbitrary execution stays in the library, used by trusted code.

## Extension points

- **Step actions.** Any zero-argument callable; wrap a shell command, an HTTP
  call, or a Python function.
- **Scheduling.** The layer model is the hook for richer scheduling (resource
  limits, priorities) without changing validation.
- **Persistence.** `RunReport` is a plain dataclass, easy to serialise to a store
  for history and resumption.
