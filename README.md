# Rollup pipeline

Catastrophe rollup pipeline that reads analyst-supplied seed data, vendor YLT
parquets, and EP summary CSVs from `data/`, then writes mart/report outputs to
root `output/`.

Active code lives in `src/rollup/`. The CLI entrypoint is
`rollup = "rollup.cli:main"`.

## Run docs
```bash

uv run rollup docs
```

OR 
```bash

./dist/rollup/rollup.exe docs
```
Follow quickstart guide.
