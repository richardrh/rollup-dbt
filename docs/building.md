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

## See also

- [Developer guide](developer-guide.md) — pipeline development workflow
