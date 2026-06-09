# Building

The project has two build outputs:

| Output | Use it when | Command |
| --- | --- | --- |
| Python wheel | Installing/importing `rollup` in another Python environment, such as Dataiku | `uv run python scripts/build.py package --no-version-prompt` |
| PyInstaller bundle | Sending a self-contained executable folder to analysts | `uv run --group build python scripts/build.py binary` |

Use the wheel for development and programmatic API usage. Use the PyInstaller
bundle only when the recipient should not need Python, `uv`, or the repository.

## Build the Python wheel

From the repository root:

```bash
uv sync
uv run python scripts/build.py package --no-version-prompt
```

The wheel is written under `dist/`, for example:

```text
dist/rollup-0.12.0-py3-none-any.whl
```

The version comes from `pyproject.toml`.

Quick import check:

```bash
python -m venv .venv-test
.venv-test/bin/python -m pip install --upgrade pip
.venv-test/bin/python -m pip install dist/*.whl
.venv-test/bin/python -c "import rollup.api; print('rollup.api import ok')"
```

On PowerShell, use `.\.venv-test\Scripts\python.exe` instead of
`.venv-test/bin/python`.

## Build the analyst bundle

From the repository root:

```bash
uv run --group build python scripts/build.py binary
```

The output is a one-folder distribution:

```text
dist/rollup/
```

Send the whole `dist/rollup/` folder to the analyst. Do not send only the
executable; the folder also contains bundled libraries, docs, and assets.

Smoke test after each build:

```bash
dist/rollup/rollup --help
dist/rollup/rollup run --help
```

On Windows:

```powershell
.\dist\rollup\rollup.exe --help
.\dist\rollup\rollup.exe run --help
```

## Analyst folder layout

Analysts should place their `data/` folder next to the copied `rollup/` folder:

```text
work/
├── data/
│   ├── ep_summaries/
│   ├── seeds/
│   └── ylt/
└── rollup/
    ├── rollup.exe
    └── _internal/
```

Run from `work/`:

```powershell
rollup\rollup.exe run
```

Use `rollup\rollup.exe run --help` to see runtime options such as
`--data-root`, `--output-root`, `--target-currency`, and `--duckdb`.

Outputs are written to `work/output/`.

## Clean build artifacts

`dist/`, `build/`, and `src/rollup.egg-info/` are generated and can be deleted.

```bash
rm -rf dist/ build/ src/rollup.egg-info/
```

PowerShell:

```powershell
Remove-Item -Recurse -Force .\dist, .\build, .\src\rollup.egg-info -ErrorAction SilentlyContinue
```

## See also

- [Developer guide](developer-guide.md) for the pipeline change checklist
- [Programmatic API](programmatic-api.md) for importing the package from Python
- [Quickstart](first-run.md) for analyst data and run commands
