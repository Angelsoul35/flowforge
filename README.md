# flowforge

A small, correct DAG workflow orchestration engine. Describe a set of steps and
their dependencies; flowforge validates the graph, groups the steps into layers
that can run in parallel, and executes them with per-step retries and
failure-driven skip semantics — the core of what a CI pipeline or a task runner
does, in a few hundred readable lines.

[![CI](https://img.shields.io/badge/CI-GitHub%20Actions-blue)](.github/workflows/ci.yml)
[![Python](https://img.shields.io/badge/python-3.11%2B-blue)](pyproject.toml)
[![License](https://img.shields.io/badge/license-MIT-green)](LICENSE)

---

## What it does

```mermaid
flowchart LR
    A[Steps + deps] --> B["Validate — Kahn's algorithm<br/>missing deps? cycle?"]
    B --> C[Layers<br/>parallelisable groups]
    C --> D[Execute<br/>parallel, retries + skips]
```

1. **Validate (`dag.py`).** Every dependency must name a real step, no step may
   depend on itself, and the graph must be acyclic. Validation and layering use
   **Kahn's algorithm**: repeatedly take the steps with no unresolved
   dependencies, which yields both a topological order and the dependency
   **layers**. If any steps remain with non-zero in-degree, they form a cycle and
   it is reported with the involved steps.
2. **Layer.** Steps in the same layer have no dependency on each other, so they
   are safe to run in parallel. `layers()` returns them grouped, e.g.
   `[[build], [lint, test], [package], [deploy]]`.
3. **Execute (`engine.py`).** Layer by layer. Within a layer, up to
   `FF_MAX_PARALLEL` steps run concurrently on a thread pool. A step runs only if
   **all** its dependencies succeeded; if any failed or was skipped, the step is
   **skipped** and records what blocked it, so a failure cascades cleanly instead
   of crashing the run. A failing step is retried up to its `max_retries`.

## Execution semantics

- **Success** — the step's action returned without raising (a step with no action
  is a no-op that always succeeds). Retries: a step is attempted
  `1 + max_retries` times; the first attempt that does not raise wins, and the
  `attempts` count is recorded.
- **Failed** — every attempt raised; the last error is captured. A step's failure
  never propagates as an exception out of the engine.
- **Skipped** — at least one dependency did not succeed. The step does not run and
  records the dependencies that blocked it. Skips cascade transitively
  (a step skipped because its dependency was skipped).
- **Run status** — `failed` if any step failed, otherwise `success`. Skips alone
  (with no failure) cannot happen, because a skip always traces back to a failure.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

python -m flowforge.cli demo        # run a flaky CI workflow and a failing one
uvicorn flowforge.main:app --reload # HTTP API
```

Build and run a workflow in code:

```python
from flowforge.engine import Engine
from flowforge.models import Step

steps = [
    Step("build"),
    Step("test", action=run_tests, depends_on=("build",), max_retries=2),
    Step("lint", depends_on=("build",)),  # runs in parallel with test
    Step("deploy", action=deploy, depends_on=("test", "lint")),
]
report = Engine(max_parallel=4).run(steps)
print(report.status, report.layers)
```

## Running a real data pipeline

The engine is not a toy: it runs a genuine multi-stage data pipeline over a **real
public dataset** — hourly electricity demand for Spain (ENTSO-E / Red Eléctrica de
España, via Hugging Face; a 7-day slice is vendored under
[`datasets/`](datasets/README.md)). The pipeline is defined as a flowforge DAG in
`flowforge/real_pipeline.py` and run by the same `Engine`:

```
fetch ──▶ clean ────┐
      └─▶ validate ─┴─▶ aggregate ──▶ summary_stats ─┐
                                    └▶ detect_peaks ──┴─▶ report
```

`clean` and `validate` both depend only on `fetch`, so flowforge puts them in the
same layer and runs them in parallel; likewise `summary_stats` and `detect_peaks`.
Each step is a real transform — parsing, gap-interpolation, a data-quality gate,
hourly→daily aggregation, peak detection — sharing a workspace the way a CI runner
shares a build directory.

```bash
python -m flowforge.real_benchmark
```

prints the layers the engine computes for the real DAG, the real result, and three
demonstrations of the engine's guarantees *on this DAG*:

```
   layers : [['fetch'], ['clean', 'validate'], ['aggregate'],
             ['detect_peaks', 'summary_stats'], ['report']]

Spain hourly electricity demand — weekly report
  window         : 2015-01-01 .. 2015-01-07 (7 days)
  week total     : 4,463,717 MWh
  highest demand : 2015-01-07 (770,520 MWh)
  week peak hour : 2015-01-07T17:00:00+01:00 (36,195 MW)
  mean renewables: 32.6% of demand (solar + onshore wind)
  ...
```

- **A cycle is rejected** before any step runs (a back-edge `report → fetch` raises
  `DagError`).
- **A failing step is retried, then cascades to its dependents only**: a crashing
  `detect_peaks` with `max_retries=2` is attempted 3 times, then `report` (which
  depends on it) is **skipped** while `summary_stats` (an independent branch) still
  **succeeds**.

**Extending it.** A step's action is any zero-argument callable, so swapping the
vendored loader for `real_data.fetch_rows` runs the same DAG against the live API;
adding a stage (say `forecast` after `aggregate`) is one more `Step` with its
`depends_on` — the engine re-derives the layers and parallelism automatically.

## The HTTP API is structure-only, by design

`/validate` accepts a declarative spec (step names and dependencies) and returns
its layers or a validation error — it never executes anything. `/run/{name}` runs
a **built-in** workflow by name. The API deliberately does **not** accept
executable step bodies over HTTP: running arbitrary caller-supplied code in a web
service is a remote-code-execution hazard, so execution is limited to code that
ships with the server.

```bash
curl -s -X POST localhost:8000/validate -H 'content-type: application/json' \
  -d '[{"name":"a","depends_on":[]},{"name":"b","depends_on":["a"]}]'
curl -s -X POST localhost:8000/run/sample
```

## Configuration

| Variable          | Default | Meaning                                         |
| ----------------- | ------- | ----------------------------------------------- |
| `FF_MAX_PARALLEL` | `4`     | Max steps run concurrently within a layer       |

## Testing

```bash
ruff check flowforge tests
pytest --cov=flowforge --cov-report=term-missing --cov-fail-under=80
```

Covers the DAG (layering, topological order, missing-dep/self-dep/duplicate/cycle
errors), the engine (success, retry success, retry exhaustion, failure cascade,
parallel execution, no-op steps), the HTTP API (validate, cycle rejection, run,
404), the CLI, and the **real data pipeline** (`tests/test_real_benchmark.py`): the
real daily/weekly aggregates against independently computed reference values, the
layers and topological order of the real DAG, gap interpolation, the validation
gate, cycle rejection, retry-then-cascade, and the fetch path with `urlopen`
monkeypatched (no network).

## Documentation

- `docs/architecture.md` — the DAG algorithms, execution model, and data flow.
- `docs/adr/` — the main design decisions and their trade-offs.
- `docs/estudio/` — a study guide in Spanish (includes a chapter on the real
  data pipeline worked end to end).
- `datasets/` — the vendored real dataset slice, its provenance and how to
  regenerate it.

## License

[MIT](LICENSE) © Angelo David Padilla Romero
