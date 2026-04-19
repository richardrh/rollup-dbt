# Architecture

How the code is organised, and why. For the high-level overview + run
commands see [`../README.md`](../README.md); for the factor chain see
[`factor-chain.md`](factor-chain.md).

## Why `polars/` (folder) vs `rollup/` (package)

A Python package named `polars` at this repo root would shadow `import polars`
(the library). So the on-disk folder is `polars/` but the importable package
inside is `rollup/`. All internal imports read `from rollup.X import Y`.
`conftest.py` puts this folder on `sys.path`.

## Vendors — one source of truth

```python
from rollup import config

cfg = config.resolve()
verisk   = cfg.vendor("verisk")    # Vendor(name, hisco_label='AIR', n_simulations=10_000,  ylt_dir, ylt_glob, ...)
risklink = cfg.vendor("risklink")  # Vendor(name, hisco_label='RMS', n_simulations=100_000, ylt_dir, ylt_glob, ...)
```

- `n_simulations` drives the return-period math in `ep_curve_from_ylt`.
- `hisco_label` ("AIR" / "RMS") is the external contract — it appears in
  output file names `HiscoAIR_*.parquet` / `HiscoRMS_*.parquet`.
- Vendors declare their `flavors` tuple; default is `(MAIN, DIALSUP)`.

## Flavors

Two Hisco output flavours:

- `Flavor.MAIN` — the main output: capped, local-ccy, forecast-adjusted,
  euws-adjusted, fine-art correction applied. Backed by the
  `loss_uplifted_capped_localccy_{tag}_euws_fagross` column.
- `Flavor.DIALSUP` — sensitivity: the composite
  (forecast × euws × fa_gross) ratio applied to the raw YLT loss.
  Backed by the `dialsup_{tag}` column.

`fa_gross` in the column name is a **factor** in the chain, not a flavour —
same way `euws` and `localccy` are factors in the name.

## VariantSpec — what gets fanned out

```python
class VariantSpec(NamedTuple):
    vendor:        Vendor
    forecast_date: date            # comes from forecast_factors seed at runtime
    flavor:        Flavor
```

Variants are built dynamically: `vendor × forecast_date × flavor`. Each
vendor × forecast_date × flavor triple produces one Hisco parquet.

With 2 vendors, 3 forecast dates, 2 flavors → **12 variants**. Add a date to
`forecast_factors.csv` → 16 variants automatically, no code change.

## Typed columns

- `rollup/schemas/columns.py` — one `StrEnum` per logical frame.
- `rollup/schemas/frames.py` — one `pl.Schema` per logical frame, keyed by
  the enum members.
- `pl.col(C.FOO)` is the project standard — StrEnum members are strings, so
  polars accepts them directly. Attribute shorthand `pl.col.foo` only works
  for valid Python identifiers; some vendor columns have spaces.

### Dynamic column names

Year-tagged columns (`f_{tag}`, `loss_..._{tag}_*`, `dialsup_{tag}`) are NOT
enumerated — they're computed strings built per forecast date at runtime.
`MetricCol` only holds the three year-invariant members
(`LOSS_UPLIFTED`, `LOSS_UPLIFTED_CAPPED`, `LOSS_UPLIFTED_CAPPED_LOCALCCY`).

## Schema validation at every boundary

Three layers, fail fast at each:

1. **Seed load** — `rollup/seeds.py` scans each CSV through
   `pl.scan_csv(..., schema=...)`; `validate_schema` runs on the result.
   Drift → `SchemaError` with a column-level diff.
2. **Stage entry + exit** — every function in `stages/` calls
   `validate_schema` on its inputs and outputs.
3. **Plan reporter** — `config.build_plan(cfg)` sniffs each seed's actual
   CSV header against its expected columns *before* the pipeline starts,
   so a drifted seed is caught at the y/N prompt, not mid-run.

## One cached marts node, many fan-outs

The duckdb pipeline materialised `mts_tbl_ylt_combined_all_factors` because
~20 downstream views read from it. We do the same with polars lazy `.cache()`:

```python
all_factors = build_all_factors(cfg, seeds).cache()     # computed exactly once
fanout_lfs  = [fanout_hisco(all_factors, v) for v in variants]
audit_lfs   = [audit_wide(all_factors, tags),
               audit_long(all_factors, tags)] if dump_interim else []
collected   = pl.collect_all(fanout_lfs + audit_lfs)    # single optimised pass
```

The whole DAG (factors + metrics + fan-outs + audits) goes through one
optimisation pass. Shared work — and all factor computations are shared
across variants — is computed once.

## Interim audit parquets (`--dump-interim`)

Two artefacts written under `<output_dir>/debug/` when the flag is set:

- **`audit_wide.parquet`** — one row per YLT event, 43 columns, ordered so
  the factor chain reads left-to-right. Every factor sits next to the
  metric it produces. Verify arithmetic row-by-row by eye.
- **`audit_long.parquet`** — identity + `(metric_name, value)` pairs.
  Pivot-table ready for excel diffs against january's EP summaries.

## Logging

Standard `logging`. Single tree rooted at `rollup`:

- `rollup.seeds` — seed loads.
- `rollup.staging` — YLT parquet scans.
- `rollup.factors` — per-factor stages.
- `rollup.pipeline` — orchestration + event-id checks + fan-out writes.

Default level is `WARNING` (silent for a clean run). Enable info trace via
`--log-level INFO` on the CLI or `ROLLUP_LOG=INFO` env var. Format:
`HH:MM:SS  LEVEL  module  message`.

Sample INFO trace from an end-to-end run:

```
rollup.seeds       loading 10 seeds from …/seeds
rollup.pipeline    plan: 12 Hisco variants across 2 vendors × 3 forecast dates × flavours
rollup.pipeline    forecast tags from seed: ['202601', '202607', '202701']
rollup.staging     loaded risklink YLT: …/risklink_ylt_*.parquet
rollup.staging     loaded verisk YLT: …/air_ylt_*.parquet
rollup.pipeline    staging: normalised YLTs concatenated
rollup.pipeline    event-id check (verisk): 80/80 rows matched air_events
rollup.factors     currency: required_currency derived from CDS class; rate_to_gbp attached
rollup.factors     forecast: 3 factor columns attached — ['f_202601', 'f_202607', 'f_202701']
rollup.factors     euws: factor attached, rank overrides applied from seed
rollup.factors     fa_gross: aal + tail factors attached (1.0 for non-FA rows)
rollup.pipeline    metrics: 12 derived loss columns + 3 dialsup columns
rollup.pipeline    fanout: wrote HiscoAIR_202601_main.parquet (80 rows)
...
```

## Event-id orphan count

`count_event_id_orphans(ylt, air_events, vendor_filter)` runs after staging.
Counts `(year_id, event_id, model_code)` triples in the YLT that are NOT
present in `air_events`. Logs `INFO` on clean, `WARNING` on orphans.

This is observation-only — the rollup math doesn't depend on `air_events`,
so orphans don't abort the run. The count is surfaced (returned + logged)
so a downstream check (e.g. cron alert, Slack post) can act on it. If you
want to abort on orphans, wrap the call in your own guard at the call site.

## Stage modules

Each stage is a pure function: `(LazyFrame, seed(s)) → LazyFrame`. No side
effects. Composed by `build_all_factors` in `pipeline.py`. Full list:

- `rollup/stages/staging.py` — `load_raw_{verisk,risklink}_ylt`,
  `normalize_{verisk,risklink}_ylt`. The normalised YLT carries `office`
  and `lob_class` from the lobs join, so factor stages downstream don't
  need to re-join the lobs dim.
- `rollup/stages/factors.py` — `attach_currency`,
  `attach_forecast_factors`, `attach_rank`, `attach_euws`, `attach_fagross`,
  `attach_uplift`.
- `rollup/stages/ep.py` — `ep_curve_from_ylt` (auxiliary; not in the main
  fan-out chain, but used by integration tests).

See [`factor-chain.md`](factor-chain.md) for how the factor stages compose.
