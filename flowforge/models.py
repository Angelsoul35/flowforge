"""The workflow data model: steps, statuses, and results."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from enum import StrEnum


class StepStatus(StrEnum):
    PENDING = "pending"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"  # an upstream dependency failed or was skipped


@dataclass(frozen=True)
class Step:
    name: str
    action: Callable[[], object] | None = None  # None = a no-op step
    depends_on: tuple[str, ...] = ()
    max_retries: int = 0  # extra attempts after the first


@dataclass
class StepResult:
    name: str
    status: StepStatus
    attempts: int = 0
    error: str = ""
    skipped_due_to: list[str] = field(default_factory=list)


@dataclass
class RunReport:
    status: StepStatus  # SUCCESS if all non-skipped succeeded, else FAILED
    results: dict[str, StepResult]
    order: list[str]  # topological execution order
    layers: list[list[str]]  # groups that may run in parallel

    def succeeded(self) -> list[str]:
        return [n for n, r in self.results.items() if r.status is StepStatus.SUCCESS]

    def failed(self) -> list[str]:
        return [n for n, r in self.results.items() if r.status is StepStatus.FAILED]

    def skipped(self) -> list[str]:
        return [n for n, r in self.results.items() if r.status is StepStatus.SKIPPED]
