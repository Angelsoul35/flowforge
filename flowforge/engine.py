"""Execute a validated workflow: layer by layer, with retries and skip semantics.

Steps run in dependency layers. Within a layer, steps are independent and may run
in parallel. A step runs only if all its dependencies succeeded; if any dependency
failed or was skipped, the step is skipped (the failure cascades downstream rather
than crashing the run). A failing step is retried up to its ``max_retries``.
"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from .dag import layers
from .models import RunReport, Step, StepResult, StepStatus


class Engine:
    def __init__(self, max_parallel: int = 1) -> None:
        self.max_parallel = max(1, max_parallel)

    def _run_step(self, step: Step) -> StepResult:
        attempts = 0
        last_error = ""
        for _ in range(step.max_retries + 1):
            attempts += 1
            try:
                if step.action is not None:
                    step.action()
                return StepResult(step.name, StepStatus.SUCCESS, attempts=attempts)
            except Exception as exc:  # a step's failure must not crash the engine
                last_error = f"{type(exc).__name__}: {exc}"
        return StepResult(step.name, StepStatus.FAILED, attempts=attempts, error=last_error)

    def run(self, steps: list[Step]) -> RunReport:
        layer_list = layers(steps)  # validates the DAG (raises on cycle/missing dep)
        by_name = {s.name: s for s in steps}
        results: dict[str, StepResult] = {}

        for layer in layer_list:
            to_run: list[Step] = []
            for name in layer:
                step = by_name[name]
                blocked = [
                    d for d in step.depends_on if results[d].status is not StepStatus.SUCCESS
                ]
                if blocked:
                    results[name] = StepResult(
                        name, StepStatus.SKIPPED, skipped_due_to=sorted(set(blocked))
                    )
                else:
                    to_run.append(step)

            if self.max_parallel > 1 and len(to_run) > 1:
                with ThreadPoolExecutor(max_workers=self.max_parallel) as pool:
                    for result in pool.map(self._run_step, to_run):
                        results[result.name] = result
            else:
                for step in to_run:
                    results[step.name] = self._run_step(step)

        order = [name for layer in layer_list for name in layer]
        overall = (
            StepStatus.FAILED
            if any(r.status is StepStatus.FAILED for r in results.values())
            else StepStatus.SUCCESS
        )
        return RunReport(status=overall, results=results, order=order, layers=layer_list)
