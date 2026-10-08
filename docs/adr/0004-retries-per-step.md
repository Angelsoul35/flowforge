# ADR 0004: Retries as a per-step property

## Status

Accepted.

## Context

Some steps are flaky — a network blip, a race, an eventually-consistent resource —
and should be retried; others are deterministic and should fail fast, because
retrying them only wastes time and hides a real bug. A single global retry policy
serves neither well.

## Decision

Make retries a **per-step** property (`max_retries`, default 0). The engine
attempts a step up to `1 + max_retries` times, stops at the first attempt that
does not raise, and records the number of attempts. A step that is not expected to
be flaky simply leaves `max_retries` at 0 and fails on the first error.

## Consequences

- **Targeted resilience.** The flaky integration test gets retries; the
  deterministic build step fails fast, surfacing real errors immediately.
- **Observable.** The recorded `attempts` count shows which steps needed a retry,
  a useful signal that a step is flaky and worth fixing.
- **Simple semantics.** "First non-raising attempt wins, up to a cap" is easy to
  reason about and test, with no backoff state to get wrong.
- **Trade-off.** There is no built-in backoff or jitter between attempts, which a
  production runner hammering a rate-limited API would want. The retry loop is the
  single, obvious place to add a sleep policy; keeping it a bare count now keeps
  the engine deterministic and its tests fast.
