"""The dependency graph: validation, topological order, and parallel layers.

A workflow is a directed acyclic graph of steps. Before anything runs, the graph
is validated — every dependency must exist and there must be no cycle — using
Kahn's algorithm, which produces a topological order and, grouped by level, the
*layers* of steps that have no dependency on each other and may run in parallel.
"""

from __future__ import annotations

from .models import Step


class DagError(ValueError):
    """Raised when the workflow graph is invalid (missing dep or a cycle)."""


def validate(steps: list[Step]) -> None:
    names = {s.name for s in steps}
    if len(names) != len(steps):
        raise DagError("duplicate step names")
    for step in steps:
        for dep in step.depends_on:
            if dep not in names:
                raise DagError(f"step '{step.name}' depends on unknown step '{dep}'")
            if dep == step.name:
                raise DagError(f"step '{step.name}' depends on itself")


def layers(steps: list[Step]) -> list[list[str]]:
    """Group steps into dependency levels (Kahn's algorithm). Raises on a cycle."""
    validate(steps)
    by_name = {s.name: s for s in steps}
    indegree = {s.name: len(set(s.depends_on)) for s in steps}
    dependents: dict[str, list[str]] = {s.name: [] for s in steps}
    for step in steps:
        for dep in set(step.depends_on):
            dependents[dep].append(step.name)

    frontier = sorted(n for n, d in indegree.items() if d == 0)
    result: list[list[str]] = []
    resolved = 0
    while frontier:
        result.append(list(frontier))
        resolved += len(frontier)
        nxt: list[str] = []
        for name in frontier:
            for child in dependents[name]:
                indegree[child] -= 1
                if indegree[child] == 0:
                    nxt.append(child)
        frontier = sorted(nxt)

    if resolved != len(by_name):
        remaining = sorted(n for n, d in indegree.items() if d > 0)
        raise DagError(f"workflow has a cycle involving: {', '.join(remaining)}")
    return result


def topological_order(steps: list[Step]) -> list[str]:
    return [name for layer in layers(steps) for name in layer]
