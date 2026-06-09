# Operating modes

## Dataiku-first mode

Use `rollup.api.run_rollup` from Python/Dataiku. This is the primary runtime
contract and uses the same calculation path as the CLI.

```python
from rollup.api import run_rollup

result = run_rollup(
    data_root="data",
    output_root="output",
    write_analysis=True,
)
```

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

`rollup.local.toml` is loaded when present. See [Runtime guide](runtime.md#duckdb-export)
and [Runtime guide](runtime.md#validation-behavior) for supported keys and runtime
behavior.
