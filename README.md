# Rollup pipeline

Catastrophe rollup pipeline that reads analyst-supplied seed data, vendor YLT
parquets, and EP summary CSVs from `data/`, then writes mart/report outputs to
root `output/`.

Active code lives in `src/rollup/`. The CLI implementation is in
`src/rollup/cli.py`.

## Serve docs locally

```bash
uv run rollup docs
uv run rollup docs --host localhost --port 4322
```

Built bundle:

```bash
./dist/rollup/rollup.exe docs
```

Follow quickstart guide.

## Build standalone bundle for analysts

Build the one-folder PyInstaller bundle from the repository root:

```bash
uv run --group build pyinstaller -y rollup.spec
```

Smoke-test the built bundle:

```bash
dist/rollup/rollup --help
dist/rollup/rollup docs
```

`dist/` is gitignored and not committed. See the building guide for analyst
deployment notes.

## Give the bundle to analysts

After the build finishes, send the analyst the whole folder:

```text
dist/rollup/
```

Do **not** send just `rollup.exe`. The analyst needs the entire `rollup` folder
because it contains the executable, bundled Python libraries, docs, and assets.

The analyst should copy the folder locally and place their `data/` folder next to
it:

```text
work/
├── data/
│   ├── ep_summaries/
│   ├── seeds/
│   └── ylt/
└── rollup/              ← copied from dist/rollup/
    ├── rollup.exe       ← Windows executable
    └── _internal/
```

On Windows, open PowerShell or Command Prompt in `work/` and run:

```powershell
rollup\rollup.exe validate
rollup\rollup.exe run
rollup\rollup.exe docs
```

Outputs are written to:

```text
work/output/
```
