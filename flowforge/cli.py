"""flowforge command-line demo."""

from __future__ import annotations

import argparse

from .config import get_settings
from .dag import layers
from .engine import Engine
from .models import RunReport
from .samples import failing_workflow, sample_workflow


def _show(report: RunReport) -> None:
    print(f"   overall: {report.status.value.upper()}")
    print(f"   layers (parallelisable groups): {report.layers}")
    for name in report.order:
        r = report.results[name]
        extra = ""
        if r.attempts > 1:
            extra = f" ({r.attempts} attempts)"
        if r.skipped_due_to:
            extra = f" (blocked by {', '.join(r.skipped_due_to)})"
        if r.error:
            extra += f" [{r.error}]"
        print(f"     {r.status.value:8} {name}{extra}")


def demo() -> None:
    engine = Engine(max_parallel=get_settings().max_parallel)

    print("1) A CI workflow with a flaky test that passes on retry:")
    steps = sample_workflow()
    print(f"   dependency layers: {layers(steps)}")
    _show(engine.run(steps))

    print("\n2) A workflow where a migration fails, cascading skips downstream:")
    _show(engine.run(failing_workflow()))


def main() -> None:
    parser = argparse.ArgumentParser(description="flowforge CLI")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("demo", help="run two sample workflows")
    args = parser.parse_args()
    if args.command == "demo":
        demo()


if __name__ == "__main__":
    main()
