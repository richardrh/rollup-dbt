# Building the standalone bundle

The pipeline ships as a standalone CLI bundle built with
[PyInstaller](https://pyinstaller.org). This lets analysts run the pipeline
without installing Python, `uv`, or a virtual environment.

## Prerequisites

From a repository checkout with the dev environment set up:

```bash
uv run --group build pyinstaller --version
```

The `build` dependency group is in `pyproject.toml`.

## Build

Build from the repository root:

```bash
uv run --group build pyinstaller -y rollup.spec
```

The output is a one-folder distribution under `dist/rollup/`. It contains the
`rollup` executable plus all bundled Python libraries, assets, and data files.

## What the build bundles

`rollup.spec` controls what is included:

| Item | Bundled location | Why |
| --- | --- | --- |
| `src/rollup/cli.py` | Entry point | Main CLI |
| `docs/` | `docs/` | Static site source for the docs command |
| `zensical.toml` | `zensical.toml` | Zensical/MkDocs configuration |
| Zensical assets | Collected data files | Theme, templates, fonts, icons for docs serving |
| Hidden imports | Submodules | `zensical`, `markdown`, `pymdownx`, `pygments.lexers` (docs dependencies) |

The build is a **one-folder COLLECT** distribution, not a single-file build.

## Smoke test

After each build, verify the executable works:

```bash
dist/rollup/rollup --help
dist/rollup/rollup generate-ep-summaries --help
dist/rollup/rollup docs
```

## Clean up

`dist/` is gitignored and not committed. Delete it freely:

```bash
rm -rf dist/
```

## What the analyst needs

The `dist/rollup/` folder is the **complete software package**. The analyst
only needs to copy that folder to their machine.

| What | Included in bundle? | Notes |
| --- | --- | --- |
| `rollup` executable | Yes | Entry point inside `dist/rollup/` |
| Documentation site | Yes | All `.md` files bundled in `_internal/docs/` |
| Zensical config/theme | Yes | `zensical.toml` and Zensical assets bundled |
| Pipeline code | Yes | All Python libraries bundled |
| `data/` directory | **No** | Analyst provides their own inputs |
| `rollup.local.toml` | **No** | Optional; only needed for SQL push |

## Deployment

1. Build the bundle:

   ```bash
   uv run --group build pyinstaller -y rollup.spec
   ```

2. Copy the entire `dist/rollup/` folder to the analyst's machine.

3. Analyst creates a working directory with their data:

   ```text
   analyst-work/
   ├── data/
   │   ├── ep_summaries/
   │   ├── seeds/
   │   └── ylt/
   └── rollup/          ← copied dist/rollup/ folder
   ```

4. Analyst runs the pipeline:

   ```bash
   cd analyst-work
   ./rollup/rollup run
   ./rollup/rollup docs
   ```

## See also

- [Developer guide](developer-guide.md) — pipeline development workflow
