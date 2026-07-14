# Building the Python package

Build a standard Python source distribution and wheel when `rollup` needs to be
installed into another Python environment, such as Dataiku or a controlled venv.

## Build artifacts

From the repository root:

```bash
uv sync
uv build
```

`uv build` writes artifacts under `dist/`, typically:

```text
dist/rollup-<version>.tar.gz
dist/rollup-<version>-py3-none-any.whl
```

The version comes from `pyproject.toml`.

## Wheel install and import smoke check

Create a clean environment, install the wheel, and verify the public API imports:

```bash
python -m venv .venv-test
.venv-test/bin/python -m pip install --upgrade pip
.venv-test/bin/python -m pip install dist/*.whl
.venv-test/bin/python -c "import rollup.api; print('rollup.api import ok')"
```

On PowerShell, use `\.venv-test\Scripts\python.exe` instead of
`.venv-test/bin/python`.

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
