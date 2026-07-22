# Azure Pipelines

This directory contains the Azure Pipelines definition for the repository.
Configure the GitHub-connected Azure Pipeline to use `/pipelines/azure-pipelines.yml`.
It runs on Microsoft-hosted `ubuntu-latest` agents for pushes to `master` and
pull requests targeting `master`.

`UsePythonVersion@0` selects Python 3.13. Project dependencies are managed by
`uv` from `uv.lock` with `uv sync --locked`; the pipeline does not use `pip` or
`pipx` to install project dependencies.

The setup installs `uv` 0.11.31 from the official standalone installer at
`https://astral.sh/uv/0.11.31/install.sh`, adds the uv binary path for later
steps, and caches uv downloads in a workspace-local `UV_CACHE_DIR` keyed by OS
and `uv.lock`. Agents with restricted outbound network access must either allow
that installer URL or provide an approved preinstalled/mirrored uv alternative
while preserving locked dependency installation.

Jobs:

- **Quality**: Ruff lint, Ruff format check, mypy, and Zensical docs build.
- **Tests**: normal, integration-only, and fuzz-only pytest runs as separate
  visible steps. The normal run uses `-m "not integration and not fuzz"`, so its
  JUnit XML contains normal tests only and has zero intentional category skips.
  Integration and fuzz runs use explicit marker selections and still run after an
  earlier category fails.

The Tests job publishes three distinct Azure Tests tab runs with exact JUnit XML
files and titles: `Pytest normal`, `Pytest integration`, and `Pytest fuzz`. Each
publisher runs on `succeededOrFailed()` and fails the task if its XML is missing,
contains failed tests, or cannot be published.

After the pytest commands, `pipelines/summarize_pytest.py` generates a Markdown
summary table from the three XML files. The table is printed in the job logs and
uploaded with `##vso[task.uploadsummary]`, so it appears in the pipeline run's
Extensions summary. Missing XML is shown as `MISSING` in that human-readable
table; the Azure test publisher remains responsible for failing missing machine
results. Use the raw XML and Azure Tests tab entries for detailed, machine-
readable test-case data.
