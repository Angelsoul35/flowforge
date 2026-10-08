# ADR 0003: A structure-only HTTP API

## Status

Accepted.

## Context

The engine runs steps whose actions are Python callables. It is tempting to expose
a "submit a workflow and run it" HTTP endpoint. But a step body is arbitrary code,
and accepting arbitrary code to execute over HTTP is textbook remote code
execution — one of the most serious vulnerabilities a service can have.

## Decision

Split what a remote caller legitimately needs into two safe endpoints:

- **`/validate`** accepts a *declarative* spec — step names and dependency names
  only — and returns the computed layers or a validation error. It never executes
  anything.
- **`/run/{name}`** executes a workflow that is **defined in the server's own
  code**, selected by name. Only code that ships with the server ever runs.

Executable step bodies are never accepted from a request.

## Consequences

- **No RCE surface.** The web layer cannot be made to run caller-supplied code.
- **Still useful remotely.** Validation and layering — the parts that benefit from
  being a service — are available; so is running vetted, built-in workflows.
- **Clear boundary.** Arbitrary execution lives in the library, used by trusted
  in-process code; the network boundary only ever sees structure or a workflow
  name.
- **Trade-off.** A general "run any workflow" API would need a sandbox, an allow
  list, or a non-code workflow definition language — a substantial, security-
  critical subsystem. Deferring it and shipping the safe subset is the honest
  default; the DSL/sandbox is a deliberate future project, not an accident.
