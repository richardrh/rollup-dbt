# Rollup pipeline

Catastrophe rollup pipeline that reads analyst-supplied seed data, vendor YLT
parquets, and EP summary CSVs from `data/`, then writes mart/report outputs to
root `output/`.

Active code lives in `src/rollup/`. The CLI entrypoint is
`rollup = "rollup.cli:main"`.

## Run docs

```bash
uv run rollup docs
uv run rollup docs --host localhost --port 4322
```

OR

```bash
uv run zensical serve --config-file zensical.toml --dev-addr localhost:4322
```

Built bundle:

```bash
./dist/rollup/rollup.exe docs
```

Follow quickstart guide.

## Build standalone bundle

```bash
uv run --group build pyinstaller -y rollup.spec
dist/rollup/rollup --help
dist/rollup/rollup docs
```

`dist/` is gitignored and not committed. See the building guide for analyst
deployment notes.
