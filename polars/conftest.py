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


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-integration"):
        return
    # `-m integration` already filters; only auto-skip when neither was set.
    if "integration" in (config.getoption("-m") or ""):
        return
    skip = pytest.mark.skip(reason="needs --run-integration (or -m integration)")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip)
