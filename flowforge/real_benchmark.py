"""Run the real data pipeline and demonstrate the engine's guarantees on it.

    python -m flowforge.real_benchmark

Prints three things from a genuine run over the vendored real slice of Spain's
hourly electricity demand:

1. the execution **layers** and **topological order** flowforge computes for the
   real DAG;
2. the real **result** the pipeline produces (daily aggregates, peaks, summary);
3. three demonstrations of the engine's guarantees *on this real DAG* — a cycle is
   rejected, a failing step is retried then cascades to its dependents only, and an
   independent branch still completes.
"""

from __future__ import annotations

from .dag import DagError, topological_order
from .engine import Engine
from .models import RunReport, StepStatus
from .real_data import DATASET
from .real_pipeline import PipelineState, build_pipeline

RULE = "=" * 72


def _show_run(report: RunReport) -> None:
    print(f"   overall: {report.status.value.upper()}")
    print(f"   layers : {report.layers}")
    for name in report.order:
        r = report.results[name]
        extra = ""
        if r.attempts > 1:
            extra += f" ({r.attempts} attempts)"
        if r.skipped_due_to:
            extra += f" (blocked by {', '.join(r.skipped_due_to)})"
        if r.error:
            extra += f" [{r.error}]"
        print(f"     {r.status.value:8} {name}{extra}")


def run_pipeline() -> tuple[PipelineState, RunReport]:
    """Run the real pipeline through the engine and return the state + report."""
    state = PipelineState()
    steps = build_pipeline(state)
    report = Engine(max_parallel=4).run(steps)
    return state, report


def demo_real_run() -> PipelineState:
    print(RULE)
    print("1) The real pipeline, run by flowforge's engine")
    print(RULE)
    print(f"   dataset: {DATASET}")
    state, report = run_pipeline()
    print(f"   topological order: {topological_order(build_pipeline(PipelineState()))}")
    _show_run(report)
    assert report.status is StepStatus.SUCCESS
    print()
    print(state.report)
    return state


def demo_cycle_rejected() -> None:
    print()
    print(RULE)
    print("2) Guarantee: a cycle is rejected (not run)")
    print(RULE)
    # Add a back-edge report -> fetch, turning the DAG into a cycle.
    cyclic = build_pipeline(PipelineState(), extra_edges={"fetch": ("report",)})
    try:
        Engine().run(cyclic)
    except DagError as exc:
        print(f"   DagError raised before any step ran: {exc}")
    else:  # pragma: no cover - the engine must reject the cycle
        raise AssertionError("cycle was not rejected")


def demo_retry_and_cascade() -> None:
    print()
    print(RULE)
    print("3) Guarantee: retry, then cascade to dependents only")
    print(RULE)
    attempts = {"n": 0}

    def flaky_detect() -> None:
        attempts["n"] += 1
        raise RuntimeError("peak detector crashed")

    state = PipelineState()
    steps = build_pipeline(
        state,
        actions={"detect_peaks": flaky_detect},
        retries={"detect_peaks": 2},  # 1 + 2 = 3 attempts
    )
    report = Engine(max_parallel=4).run(steps)
    _show_run(report)
    print(f"   -> detect_peaks was attempted {attempts['n']} times (1 + 2 retries)")
    print(
        "   -> report is SKIPPED (depends on detect_peaks); "
        "summary_stats still SUCCEEDED (independent branch)"
    )
    assert report.results["detect_peaks"].status is StepStatus.FAILED
    assert report.results["detect_peaks"].attempts == 3
    assert report.results["report"].status is StepStatus.SKIPPED
    assert report.results["summary_stats"].status is StepStatus.SUCCESS


def main() -> None:
    demo_real_run()
    demo_cycle_rejected()
    demo_retry_and_cascade()


if __name__ == "__main__":
    main()
