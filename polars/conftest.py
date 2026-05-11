"""Pytest bootstrap.

Two responsibilities:

1. Put this folder on `sys.path` so `import rollup` resolves. The on-disk
   folder is `polars/` to match the project's mental model, but the
   importable package inside is `rollup/` to avoid shadowing the polars
   library. See polars/README.md.

2. Gate integration tests behind `--run-integration`. Tests marked
   `@pytest.mark.integration` are SKIPPED by default. Pass the flag (or
   `-m integration`) to opt in. Integration tests typically require
   external resources (Docker for SQL Server, network, etc.) and are
   slower / heavier than the default unit suite.
"""
import sys
from pathlib import Path

import pytest


sys.path.insert(0, str(Path(__file__).parent))


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="run integration tests (require Docker / external services)",
    )
    parser.addoption(
        "--run-fuzz",
        action="store_true",
        default=False,
        help="run property-based / fuzz tests (uses Hypothesis)",
    )


def pytest_collection_modifyitems(config, items):
    run_integration = config.getoption("--run-integration")
    run_fuzz = config.getoption("--run-fuzz")
    m_option = config.getoption("-m") or ""

    if not run_integration and "integration" not in m_option:
        skip_integration = pytest.mark.skip(reason="needs --run-integration (or -m integration)")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip_integration)

    if not run_fuzz and "fuzz" not in m_option:
        skip_fuzz = pytest.mark.skip(reason="needs --run-fuzz (or -m fuzz)")
        for item in items:
            if "fuzz" in item.keywords:
                item.add_marker(skip_fuzz)
