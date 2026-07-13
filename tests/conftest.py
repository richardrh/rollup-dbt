"""Pytest bootstrap."""
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-fuzz",
        action="store_true",
        default=False,
        help="run property-based / fuzz tests (uses Hypothesis)",
    )


def pytest_collection_modifyitems(config, items):
    run_fuzz = config.getoption("--run-fuzz")
    m_option = config.getoption("-m") or ""

    if not run_fuzz and "fuzz" not in m_option:
        skip_fuzz = pytest.mark.skip(reason="needs --run-fuzz (or -m fuzz)")
        for item in items:
            if "fuzz" in item.keywords:
                item.add_marker(skip_fuzz)
