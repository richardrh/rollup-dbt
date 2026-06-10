# Developer guide

## Safe local loop

```bash
uv run python -m rollup run --data-root data --output-root output --target-currency GBP --no-stage-outputs --no-analysis
uv run python -m rollup run --data-root data --output-root output --target-currency GBP
```

Use the first command for a quick validation/calculation smoke run. Use the
second when you need stage outputs and `analysis/ep_report.csv`.

## Development notes

- Keep runtime changes behind the public API in `rollup.api`.
- Add new stage outputs in the pipeline only when they help local inspection;
  Dataiku users should not need them for normal operation.
- Update [Runtime guide](runtime.md), [Calculation reference](calculation-reference.md),
  and [Data requirements](data-requirements.md) whenever output contracts,
  metric names, config keys, or required inputs change.
- If a new output should be queryable in DuckDB, update `rollup.duckdb_export`
  and document the table list.
