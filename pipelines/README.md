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
  visible steps, each producing its own JUnit XML before publishing all available
  results.
