# Windows install and run guide

Use this guide on a fresh Windows machine. Run all project commands from the
repository root: the folder that contains `pyproject.toml`, `README.md`, and
`zensical.toml`.

## 1. Install uv

Recommended: install `uv` directly from PowerShell.

```powershell
powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
```

Close and reopen PowerShell so the updated `PATH` is loaded. If `uv` is still
not found, refresh the current shell path or open a new terminal window.

Optional: if you already use Scoop, you can install uv through Scoop instead.
Scoop is not required for this app; the direct uv install above is preferred.

```powershell
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
irm get.scoop.sh | iex
scoop install uv
```

## 2. Install project dependencies

From the repository root, run:

```powershell
uv sync
```

The project expects Python `3.13.*` as declared in `pyproject.toml`. `uv sync`
should manage and install the matching Python version for the project when it is
needed.

Do not copy `.venv` from another machine. Some dependencies, including Polars,
contain native Windows-specific binaries and must be rebuilt on each machine.

## 3. Add analyst data

Drop input files under `data/`. Generated files are written under root
`output/`.

See [Loading your data](load-data.md) for the exact required locations and file
formats. The short version is:

```text
data/ylt/verisk/*.parquet
data/ylt/risklink/*.parquet
data/ep_summaries/verisk/verisk_ep_summary.long.csv
data/ep_summaries/risklink/rms_ep_summary.long.csv
data/seeds/**
```

## 4. Validate and run

Run these commands from the repository root.

```powershell
uv run rollup validate
```

To keep validation CSVs:

```powershell
uv run rollup validate --report-dir output/validation
```

Run the pipeline:

```powershell
uv run rollup run
```

Run with debug outputs:

```powershell
uv run rollup run --debug
```

Generate the EP analysis report:

```powershell
uv run rollup analyze
```

Serve the docs locally:

```powershell
uv run rollup docs --port 8010
```

For the normal first-run flow, continue with the [Quickstart](first-run.md).

## Troubleshooting Polars import errors

If imports fail with a missing binary error or an error mentioning
`polars._utils.polars_version`, rebuild the environment on this machine:

```powershell
Remove-Item -Recurse -Force .venv
uv sync
```

This recreates `.venv` and downloads/builds the native packages for the current
Windows machine.
