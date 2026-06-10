# Operating modes

## Dataiku-first mode

Use `rollup.api.run_rollup` from Python/Dataiku. This is the primary runtime
contract and uses the same calculation path as the CLI.

```python
from rollup.api import run_rollup

result = run_rollup(
    data_root="data",
    output_root="output",
    config_path="rollup.local.toml",
    write_analysis=True,
)
```

In Dataiku, prefer an explicit `config_path` in the job workspace or managed
folder. Pass a managed-folder path directly as `data_root` when it already
matches the required layout; otherwise materialize inputs into a temporary
workspace and persist the returned `result.outputs` files before cleanup.

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

`rollup.local.toml` is loaded when no explicit config object or `config_path` is
supplied. Dataiku callers should pass `config_path` explicitly. See
[Programmatic API](programmatic-api.md#config-loading) and
[Runtime guide](runtime.md#duckdb-export) for supported keys and runtime
behavior.
