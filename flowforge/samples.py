"""A sample CI-style workflow for the demo and tests.

build -> test -> {lint, package} -> deploy, with a flaky 'test' step that fails
once and succeeds on retry, to exercise the retry and layering logic.
"""

from __future__ import annotations

from .models import Step


def sample_workflow() -> list[Step]:
    state = {"test_calls": 0}

    def flaky_test() -> None:
        state["test_calls"] += 1
        if state["test_calls"] < 2:  # fail the first attempt, pass on retry
            raise RuntimeError("one test failed (transient)")

    return [
        Step("build"),
        Step("test", action=flaky_test, depends_on=("build",), max_retries=2),
        Step("lint", depends_on=("build",)),
        Step("package", depends_on=("test", "lint")),
        Step("deploy", depends_on=("package",)),
    ]


def failing_workflow() -> list[Step]:
    """A workflow where one step fails hard, so its dependants are skipped."""

    def boom() -> None:
        raise RuntimeError("migration failed")

    return [
        Step("build"),
        Step("migrate", action=boom, depends_on=("build",)),
        Step("deploy", depends_on=("migrate",)),
        Step("notify", depends_on=("deploy",)),
    ]
