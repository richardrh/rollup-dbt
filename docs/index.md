# Polars Rollup Pipeline

Detailed documentation for the polars rollup pipeline. This is the deep
reference; the project [`README.md`](https://github.com/hamptonian/rollup-dbt)
is the overview + schematic + run commands.

## Run the pipeline

```bash
# from repo root
uv run -m rollup.pipeline --dry-run                  # plan only — no data read
uv run -m rollup.pipeline --yes                      # full run
uv run -m rollup.pipeline --yes --log-level INFO     # full run, with stage logs
uv run pytest -q                                     # 98 tests, ~4s
```

## Build the docs

```bash
# from repo root
uv run mkdocs serve   # live-reload dev server at http://127.0.0.1:8000
uv run mkdocs build   # write static site to site/
```

## Documentation index

| doc | what it covers |
|---|---|
| [File formats](file-formats.md) | **Quick reference.** Every input file's columns + dtypes — YLT parquets, all 12 seed CSVs, EP summaries. Pair with `--dry-run` for instant feedback. |
| [Data requirements](data-requirements.md) | **The contract for a real run.** YLT wire schemas, every seed CSV, currency-derivation rule, forecast-factor join contract, failure-mode reference. **Includes which RiskLink analyses you actually need to export.** |
| [Architecture](architecture.md) | Code organisation. `Vendor` / `Flavor` / `VariantSpec`, seed loading, schema validation layers, the audit parquets, logging. |
| [Factor chain](factor-chain.md) | The factor-chain mental model, cumulative column-naming convention, and the **5-step recipe to add a new factor**. |
| [Calculations](calculations.md) | Every january duckdb view → polars stage mapping, with the original SQL quoted. Also: `apply_rollup_scope`, the `chain.CHAIN` registry walker, EP curves, the dialsup funnel, and a per-stage status summary table. |

## Three things to know first

1. **Polars lazy single-process.** No warehouse, no Jinja, no SQL compile.
   Parquet → 12 Hisco parquets in ~5 seconds.
2. **Schema-validated at every stage boundary.** Column names are `StrEnum`
   members; every frame has a declared `pl.Schema`. Drift fails loud, with
   the frame name and the missing/wrong column.
3. **Seeds are the contract.** 12 CSVs under `data/seeds/`. The pipeline's
   pre-flight (`--dry-run`) reports schema status for each before you commit
   to a run.

## See also

- [`data/seeds/README.md`](https://github.com/hamptonian/rollup-dbt/blob/master/data/seeds/README.md) — per-seed schema, source, and population status.
- `polars/rollup/stages/factors.py` — factor-attach functions; the comment
  block at the top duplicates the 5-step recipe for people in the code.
- `polars/RH-TODO-DATA.md` — concrete TODO list for the data-export pass.
