# ADR 0001: Kahn's algorithm for validation and layering

## Status

Accepted.

## Context

A workflow engine must do three things to a dependency graph before it runs:
confirm it is valid (no missing dependency, no cycle), decide an order that
respects dependencies, and — to be more than sequential — find which steps can run
at the same time. These could be three separate passes (a DFS for cycle
detection, another for ordering, a grouping pass for parallelism).

## Decision

Use **Kahn's algorithm** (repeatedly remove in-degree-zero nodes) and get all
three from one pass: each removal round is a **parallel layer**, the rounds
flattened are a **topological order**, and "not every node was removed" is exactly
a **cycle**, reported with the steps still involved.

## Consequences

- **One algorithm, three results.** Less code, and the results are consistent by
  construction (the order is the layers, the cycle check is the layering's
  leftover).
- **Parallelism for free.** The layers are precisely the sets of mutually
  independent steps, which is the information the scheduler needs.
- **Deterministic.** Layers are emitted in sorted order, so the same workflow
  always produces the same layering and order — important for reproducible runs
  and tests.
- **Trade-off.** Layer-based scheduling is slightly less aggressive than a pure
  dependency-driven scheduler that starts each step the instant its own
  dependencies finish (a long step in a layer holds back the next layer). Layers
  are far simpler to reason about and test, and the model leaves room to move to
  finer scheduling later without changing validation.
