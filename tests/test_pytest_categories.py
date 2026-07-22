from __future__ import annotations

import importlib.util
from pathlib import Path
from types import ModuleType

import pytest


class _Config:
    def __init__(self, *, run_fuzz: bool, run_integration: bool, markexpr: str = "") -> None:
        self._options = {
            "--run-fuzz": run_fuzz,
            "--run-integration": run_integration,
            "-m": markexpr,
        }

    def getoption(self, name: str) -> bool | str:
        return self._options[name]


class _Item:
    def __init__(self, *markers: str) -> None:
        self.keywords = dict.fromkeys(markers)
        self.markers: list[pytest.MarkDecorator] = []

    def add_marker(self, marker: pytest.MarkDecorator) -> None:
        self.markers.append(marker)


@pytest.fixture(scope="module")
def pytest_hooks() -> ModuleType:
    conftest_path = Path(__file__).with_name("conftest.py")
    spec = importlib.util.spec_from_file_location("rollup_pytest_hooks", conftest_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    ("run_fuzz", "run_integration", "markexpr", "expected_skips"),
    [
        (False, False, "", {"fuzz", "integration"}),
        (False, True, "", {"fuzz"}),
        (True, False, "", {"integration"}),
        (True, True, "", set()),
        (False, False, "integration", {"fuzz"}),
        (False, False, "fuzz", {"integration"}),
    ],
)
def test_category_hooks_skip_only_unselected_opt_in_categories(
    pytest_hooks: ModuleType,
    run_fuzz: bool,
    run_integration: bool,
    markexpr: str,
    expected_skips: set[str],
) -> None:
    items = [_Item(), _Item("fuzz"), _Item("integration")]

    pytest_hooks.pytest_collection_modifyitems(
        _Config(
            run_fuzz=run_fuzz,
            run_integration=run_integration,
            markexpr=markexpr,
        ),
        items,
    )

    actual_skips = {
        marker_name
        for marker_name, item in zip(("normal", "fuzz", "integration"), items, strict=True)
        if item.markers
    }
    assert actual_skips == expected_skips
