# ADR 0002: A failure cascades as skips, not a crash

## Status

Accepted.

## Context

When a step fails, its dependants cannot run — their inputs are not there. The
engine has to decide what happens to them and to the run as a whole. The naive
behaviour (let the exception propagate) aborts the run at the first failure and
produces a stack trace instead of a report, losing the status of everything else.

## Decision

Catch a step's exception inside the engine and turn it into a **failed** result;
mark every dependant (transitively) as **skipped**, recording which dependencies
blocked it; and continue the run. The overall status is `failed` if any step
failed, else `success`.

## Consequences

- **A complete report every time.** The run always finishes with a status for
  every step — succeeded, failed, or skipped — rather than crashing midway.
- **Clear blame.** A skipped step names the dependency that blocked it, so the
  root cause is one hop away and the cascade is traceable.
- **Independent branches still run.** A failure in one branch does not stop an
  unrelated branch in a later layer; only the failed step's descendants are
  skipped.
- **Trade-off.** "Continue despite failure" is the right default for a reporting
  run but not for every pipeline (sometimes you want to stop the world on the
  first failure). The model supports that as a thin policy on top of the complete
  report; the engine's job is to produce the full picture.
