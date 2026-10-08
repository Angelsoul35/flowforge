from __future__ import annotations

import pytest

from flowforge.dag import DagError, layers, topological_order, validate
from flowforge.models import Step


def test_layers_group_independent_steps():
    steps = [
        Step("build"),
        Step("test", depends_on=("build",)),
        Step("lint", depends_on=("build",)),
        Step("package", depends_on=("test", "lint")),
    ]
    assert layers(steps) == [["build"], ["lint", "test"], ["package"]]


def test_topological_order_respects_dependencies():
    steps = [Step("a"), Step("b", depends_on=("a",)), Step("c", depends_on=("b",))]
    assert topological_order(steps) == ["a", "b", "c"]


def test_missing_dependency_raises():
    with pytest.raises(DagError, match="unknown step"):
        validate([Step("a", depends_on=("ghost",))])


def test_self_dependency_raises():
    with pytest.raises(DagError, match="itself"):
        validate([Step("a", depends_on=("a",))])


def test_duplicate_names_raise():
    with pytest.raises(DagError, match="duplicate"):
        validate([Step("a"), Step("a")])


def test_cycle_is_detected():
    steps = [
        Step("a", depends_on=("c",)),
        Step("b", depends_on=("a",)),
        Step("c", depends_on=("b",)),
    ]
    with pytest.raises(DagError, match="cycle"):
        layers(steps)
