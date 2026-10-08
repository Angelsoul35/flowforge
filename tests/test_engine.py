from __future__ import annotations

from flowforge.engine import Engine
from flowforge.models import Step, StepStatus
from flowforge.samples import failing_workflow, sample_workflow


def test_sample_workflow_succeeds_with_retry():
    report = Engine().run(sample_workflow())
    assert report.status is StepStatus.SUCCESS
    assert report.results["test"].attempts == 2  # failed once, passed on retry
    assert set(report.succeeded()) == {"build", "test", "lint", "package", "deploy"}


def test_failure_cascades_as_skips():
    report = Engine().run(failing_workflow())
    assert report.status is StepStatus.FAILED
    assert report.results["migrate"].status is StepStatus.FAILED
    assert report.results["deploy"].status is StepStatus.SKIPPED
    assert report.results["notify"].status is StepStatus.SKIPPED
    assert report.results["deploy"].skipped_due_to == ["migrate"]


def test_retry_exhaustion_fails():
    calls = {"n": 0}

    def always_fail() -> None:
        calls["n"] += 1
        raise ValueError("nope")

    steps = [Step("x", action=always_fail, max_retries=2)]
    report = Engine().run(steps)
    assert report.status is StepStatus.FAILED
    assert report.results["x"].attempts == 3  # 1 + 2 retries
    assert "ValueError" in report.results["x"].error
    assert calls["n"] == 3


def test_parallel_layer_runs_all_steps():
    done = []
    steps = [
        Step("root"),
        Step("a", action=lambda: done.append("a"), depends_on=("root",)),
        Step("b", action=lambda: done.append("b"), depends_on=("root",)),
        Step("c", action=lambda: done.append("c"), depends_on=("root",)),
    ]
    report = Engine(max_parallel=3).run(steps)
    assert report.status is StepStatus.SUCCESS
    assert set(done) == {"a", "b", "c"}


def test_no_op_step_succeeds():
    report = Engine().run([Step("noop")])
    assert report.results["noop"].status is StepStatus.SUCCESS
    assert report.results["noop"].attempts == 1
