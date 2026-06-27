# Operating modes

## Dataiku-first mode

Use `rollup.api.run_rollup` from Python/Dataiku. This is the primary runtime
contract and uses the same calculation path as the CLI. Dataiku callers should
not rely on current-working-directory defaults; pass an explicit job/workspace
`config_path`.

```python
from pathlib import Path
from rollup.api import run_rollup

workspace = Path("/tmp/rollup-job")
data_root = workspace / "data"
output_root = workspace / "output"
config_path = workspace / "config.toml"

result = run_rollup(
    data_root=data_root,
    output_root=output_root,
    config_path=config_path,
    write_analysis=False,
    log_file=output_root / "rollup.log",
)
```

In Dataiku, prefer an explicit `config_path` in the job workspace or managed
folder. The repository default is the tracked `config.toml`; a job can copy or
write its own `config.toml` and pass it explicitly. Pass a managed-folder path
directly as `data_root` when it already matches the required layout; otherwise
materialize inputs into a temporary workspace and persist the returned
`result.outputs` files before cleanup. The `/tmp/rollup-job` path above is only
an example; real Dataiku managed-folder and temp paths vary.

## Local CLI mode

Use the CLI for smoke testing and local operation:

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
```

Useful flags:

- `--no-stage-outputs` skips `output/stages/`.
- `--no-analysis` skips `output/analysis/ep_report.csv`.
- `--duckdb` writes `output/rollup.duckdb`.
- `--duckdb-file <path>` writes DuckDB to a custom path.
- `--log-file <path>` overrides the default `<output-root>/rollup.log`.

## Config mode

`config.toml` is tracked and loaded when no explicit config object or
`config_path` is supplied. Dataiku callers should pass `config_path` explicitly
for job-specific configs. `rollup.local.toml` can still be passed explicitly but
is no longer the runtime default. See
[Programmatic API](programmatic-api.md#config-loading) and
[Runtime guide](runtime.md#duckdb-export) for supported keys and runtime
behavior.
