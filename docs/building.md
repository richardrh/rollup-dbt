
# Building the rollup package and standalone bundle

There are two supported build outputs:

1. **Python package wheel** â the normal developer build. This packages the
   library code under `src/rollup/`, including the public API in
   `src/rollup/api.py`.
2. **Standalone PyInstaller bundle** â an analyst-facing executable bundle for
   people who do not have Python, `uv`, or the repository.

Most development work should use the Python package build. Use the PyInstaller
bundle only when you need to send a self-contained executable folder to an
analyst.

---

# Building the Python package

The rollup code is packaged as a Python library from:

```text
src/rollup/
```

The public API lives in:

```text
src/rollup/api.py
```

This package does **not** need a `main()` function and does **not** need a
console script entry point. It is intended to be imported from Python, for
example:

```python
import rollup.api
```

or:

```python
from rollup.api import ...
```

## Package prerequisites

The package builder uses the Python `build` package.

From the repository root:

```bash
uv add --dev build
```

If `build` is already listed in the dev dependency group, just sync the
environment:

```bash
uv sync
```

## Required `pyproject.toml` shape

The package build expects a `src` layout. The important parts of
`pyproject.toml` are:

```toml
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "rollup"
version = "0.1.0"
description = "Retail rollup library."
requires-python = ">=3.11"
dependencies = [
  "polars",
  "pyyaml",
]

[tool.setuptools.package-dir]
"" = "src"

[tool.setuptools.packages.find]
where = ["src"]
include = ["rollup*"]
```

Do **not** add a `[project.scripts]` entry unless the package later grows a real
CLI entry point. The library package does not require this:

```toml
[project.scripts]
rollup = "rollup.cli:main"
```

## Package build

Build the wheel from the repository root:

```bash
uv run python scripts/build.py package
```

The built wheel is written to:

```text
dist/
```

You should see an artifact like:

```text
dist/rollup-0.1.0-py3-none-any.whl
```

The exact version comes from `pyproject.toml`.

## Check the package built correctly

First, list the build artifacts:

```bash
ls dist/
```

On PowerShell:

```powershell
Get-ChildItem .\dist
```

Then inspect the wheel contents and check that `rollup/api.py` is included:

```bash
python -c "import zipfile, pathlib; whl=next(pathlib.Path('dist').glob('*.whl')); print(whl); print('\n'.join(zipfile.ZipFile(whl).namelist()))"
```

You should see files like:

```text
rollup/__init__.py
rollup/api.py
```

## Smoke test the wheel in a clean environment

The best check is to install the wheel into a clean temporary virtual
environment and import the API.

On PowerShell:

```powershell
python -m venv .venv-test
.\.venv-test\Scripts\python.exe -m pip install --upgrade pip
.\.venv-test\Scripts\python.exe -m pip install (Get-ChildItem .\dist\*.whl | Select-Object -First 1).FullName
.\.venv-test\Scripts\python.exe -c "import rollup.api; print('rollup.api import ok')"
```

If that prints:

```text
rollup.api import ok
```

the package built and installed successfully.

To confirm Python is importing the installed wheel rather than the local source
tree:

```powershell
.\.venv-test\Scripts\python.exe -c "import rollup, rollup.api; print(rollup.__file__); print(rollup.api.__file__)"
```

The paths should point into:

```text
.venv-test/Lib/site-packages/rollup/
```

## Clean package build artifacts

The build output can be deleted freely:

```bash
rm -rf dist/ build/ src/rollup.egg-info/
```

On PowerShell:

```powershell
Remove-Item -Recurse -Force .\dist, .\build, .\src\rollup.egg-info -ErrorAction SilentlyContinue
```

---

# Building the standalone bundle

The standalone bundle is built with
[PyInstaller](https://pyinstaller.org). This lets analysts run the pipeline
without installing Python, `uv`, or a virtual environment.

Use this when you need to send a self-contained executable folder to an analyst.
For normal development and API usage, prefer the Python package build above.

## Bundle prerequisites

From a repository checkout with the build environment set up:

```bash
uv run --group build pyinstaller --version
```

The `build` dependency group is in `pyproject.toml`.

## Bundle build

Build from the repository root:

```bash
uv run --group build pyinstaller -y rollup.spec
```

The output is a one-folder distribution under:

```text
dist/rollup/
```

It contains the `rollup` executable plus all bundled Python libraries, assets,
and data files.

## What the bundle includes

`rollup.spec` controls what is included:

| Item | Bundled location | Why |
| --- | --- | --- |
| `src/rollup/cli.py` | Entry point | Main CLI for the standalone executable |
| `docs/` | `docs/` | Static site source for the docs command |
| `zensical.toml` | `zensical.toml` | Zensical/MkDocs configuration |
| Zensical assets | Collected data files | Theme, templates, fonts, icons for docs serving |
| Hidden imports | Submodules | `zensical`, `markdown`, `pymdownx`, `pygments.lexers` and other docs dependencies |

The PyInstaller build is a **one-folder COLLECT** distribution, not a
single-file build.

## Bundle smoke test

After each PyInstaller build, verify the executable works:

```bash
dist/rollup/rollup --help
dist/rollup/rollup generate-ep-summaries --help
dist/rollup/rollup docs
```

On Windows, use:

```powershell
.\dist\rollup\rollup.exe --help
.\dist\rollup\rollup.exe generate-ep-summaries --help
.\dist\rollup\rollup.exe docs
```

## Clean bundle artifacts

`dist/` is gitignored and not committed. Delete it freely:

```bash
rm -rf dist/
```

On PowerShell:

```powershell
Remove-Item -Recurse -Force .\dist -ErrorAction SilentlyContinue
```

---

# What to send the analyst

For analysts, send the PyInstaller bundle, not the Python wheel.

After the PyInstaller build finishes you have a folder at:

```text
./dist/rollup/
```

That is the only thing you send to the analyst.

The analyst does **not** need Python, `uv`, or this repository. They only need:

1. The `./dist/rollup/` folder you built.
2. A `data/` folder with their own inputs, such as YLTs, EP summaries, and
   seeds.

## How the analyst uses it

1. Put the `rollup` folder somewhere on their machine.
2. Create a `data/` folder **next to** the `rollup` folder, or **above** it.
   The CLI walks up the directory tree looking for `data/`:

   ```text
   work/
   âââ data/              â analyst puts their inputs here
   â   âââ ep_summaries/
   â   âââ seeds/
   â   âââ ylt/
   âââ rollup/            â the dist/rollup/ folder you sent
       âââ rollup         â the executable
   ```

3. Open a terminal in `work/` and run:

   ```bash
   # Windows
   rollup\rollup run
   rollup\rollup docs

   # macOS / Linux
   ./rollup/rollup run
   ./rollup/rollup docs
   ```

Outputs are written to:

```text
work/output/
```

---

# See also

- [Developer guide](developer-guide.md) â pipeline development workflow
- [First run](first-run.md) â what goes in the `data/` folder
