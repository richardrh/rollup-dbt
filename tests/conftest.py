"""Pytest bootstrap."""

import pytest


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-fuzz",
        action="store_true",
        default=False,
        help="run property-based / fuzz tests (uses Hypothesis)",
    )
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run synthetic cross-boundary and filesystem integration tests",
    )


def pytest_collection_modifyitems(
    config: pytest.Config, items: list[pytest.Item]
) -> None:
    run_fuzz = config.getoption("--run-fuzz")
    run_integration = config.getoption("--run-integration")
    m_option = config.getoption("-m") or ""

    if not run_fuzz and "fuzz" not in m_option:
        skip_fuzz = pytest.mark.skip(reason="needs --run-fuzz (or -m fuzz)")
        for item in items:
            if "fuzz" in item.keywords:
                item.add_marker(skip_fuzz)

    if not run_integration and "integration" not in m_option:
        skip_integration = pytest.mark.skip(
            reason="needs --run-integration (or -m integration)"
        )
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)
