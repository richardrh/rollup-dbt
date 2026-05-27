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

## What to send the analyst

After the build finishes you have a folder at `./dist/rollup/`. That is the
only thing you send to the analyst.

The analyst does **not** need Python, `uv`, or this repository. They only need:

1. The `./dist/rollup/` folder you built.
2. A `data/` folder with their own inputs (YLTs, EP summaries, seeds).

## How the analyst uses it

1. Put the `rollup` folder somewhere on their machine.
2. Create a `data/` folder **next to** the `rollup` folder, or **above** it.
   The CLI walks up the directory tree looking for `data/`:

   ```text
   work/
   ├── data/              ← analyst puts their inputs here
   │   ├── ep_summaries/
   │   ├── seeds/
   │   └── ylt/
   └── rollup/            ← the dist/rollup/ folder you sent
       └── rollup         ← the executable
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

Outputs are written to `work/output/`.

## See also

- [Developer guide](developer-guide.md) — pipeline development workflow
- [First run](first-run.md) — what goes in the `data/` folder
