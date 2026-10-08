"""flowforge HTTP API.

The API validates declarative workflow specs (names + dependencies) and runs
built-in sample workflows. It deliberately does NOT execute arbitrary code
supplied over HTTP — only step *structure* is accepted from a request.
"""

from __future__ import annotations

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from .config import get_settings
from .dag import DagError, layers
from .engine import Engine
from .models import RunReport, Step
from .samples import failing_workflow, sample_workflow

app = FastAPI(
    title="flowforge",
    version="1.0.0",
    summary="A DAG workflow orchestration engine: validate, layer, and run.",
)

_WORKFLOWS = {"sample": sample_workflow, "failing": failing_workflow}


class StepSpec(BaseModel):
    name: str
    depends_on: list[str] = Field(default_factory=list)


def _report_dict(report: RunReport) -> dict:
    return {
        "status": report.status.value,
        "order": report.order,
        "layers": report.layers,
        "results": {
            n: {
                "status": r.status.value,
                "attempts": r.attempts,
                "error": r.error,
                "skipped_due_to": r.skipped_due_to,
            }
            for n, r in report.results.items()
        },
    }


@app.get("/health", tags=["ops"])
def health() -> dict:
    return {"status": "ok"}


@app.post("/validate", tags=["dag"])
def validate_spec(steps: list[StepSpec]) -> dict:
    """Validate a declarative spec and return its parallel layers (no execution)."""
    workflow = [Step(name=s.name, depends_on=tuple(s.depends_on)) for s in steps]
    try:
        return {"valid": True, "layers": layers(workflow)}
    except DagError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc


@app.post("/run/{name}", tags=["run"])
def run_builtin(name: str) -> dict:
    if name not in _WORKFLOWS:
        raise HTTPException(status_code=404, detail=f"unknown workflow: {name}")
    engine = Engine(max_parallel=get_settings().max_parallel)
    return _report_dict(engine.run(_WORKFLOWS[name]()))
