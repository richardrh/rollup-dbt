# Architecture

How the code is organised, and why. For the high-level overview + run
commands see [`../polars/README.md`](../polars/README.md); for the factor chain see
[`factor-chain.md`](factor-chain.md).

## Why `polars/` (folder) vs `rollup/` (package)

A Python package named `polars` at this repo root would shadow `import polars`
(the library). So the on-disk folder is `polars/` but the importable package
inside is `rollup/`. All internal imports read `from rollup.X import Y`.
`conftest.py` puts this folder on `sys.path`.

## Vendors — one source of truth

```python
from rollup import config
from rollup.config import VendorName

cfg = config.resolve()
verisk   = cfg.vendor(VendorName.VERISK)    # Vendor(hisco_label='AIR', n_simulations=10_000,  ylt_dir, ...)
risklink = cfg.vendor(VendorName.RISKLINK)  # Vendor(hisco_label='RMS', n_simulations=100_000, ylt_dir, ...)
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
- `Flavor.DIALSUP` — currency-converted raw loss: `loss / rate_to_gbp` (no factors).
  Single column, not per-forecast-tag, because the formula is tag-independent.
  One file per vendor.

`fa_gross` in the column name is a **factor** in the chain, not a flavour —
same way `euws` and `localccy` are factors in the name.

## VariantSpec — what gets fanned out

```python
class VariantSpec(NamedTuple):
    vendor:        Vendor
    forecast_date: date            # comes from forecast_factors seed at runtime
    flavor:        Flavor
```

Variants are built dynamically: `vendor × forecast_date × flavor`. However,
dialsup is independent of forecast date (formula is `loss / rate_to_gbp`),
so only one dialsup variant is created per vendor.

Current setup: 2 vendors × 3 forecast dates (MAIN) + 2 vendors (DIALSUP) → **8 variants**.
Add a forecast date to `forecast_factors.csv` → 10 variants automatically, no code change.

## Typed columns

- `rollup/schemas/columns.py` — one `StrEnum` per logical frame.
- `rollup/schemas/frames.py` — one `pl.Schema` per logical frame, keyed by
  the enum members.
- `pl.col(C.FOO)` is the project standard — StrEnum members are strings, so
  polars accepts them directly. Attribute shorthand `pl.col.foo` only works
  for valid Python identifiers; some vendor columns have spaces.

### Dynamic column names — driven by `rollup/chain.py`

Year-tagged columns (`f_{tag}`, `loss_..._{tag}_*`, `dialsup_{tag}`) can't
live in a StrEnum because the `{tag}` suffix is data-driven from the
`forecast_factors` seed at runtime. They live in a TypedDict registry:

```python
# rollup/chain.py
class ChainStage(TypedDict):
    suffix:     str        # appended to the running column name
    factor_col: str        # AF.* column or "{tag}"-templated string
    is_per_tag: bool       # is factor_col templated by tag?
    ancillary_before: tuple[str, ...]   # e.g. RNK before EUWS_FACTOR in audit
    ancillary_after:  tuple[str, ...]   # e.g. FA_GROSS_TAIL after FA_GROSS_AAL

CHAIN: dict[str, ChainStage] = {
    "forecast": {"suffix": "",         "factor_col": "f_{tag}",
                 "is_per_tag": True,  ...},
    "euws":     {"suffix": "_euws",    "factor_col": AF.EUWS_FACTOR,
                 "is_per_tag": False, "ancillary_before": (AF.RNK,), ...},
    "fagross":  {"suffix": "_fagross", "factor_col": AF.FA_GROSS_AAL_FACTOR,
                 "is_per_tag": False, "ancillary_after":  (AF.FA_GROSS_TAIL_FACTOR,), ...},
}
```

`_compute_metrics`, `_compute_dialsup`, `_metric_cols_for`, `audit_wide`,
and `VariantSpec.loss_metric` all walk this registry — adding a new factor
stage is one entry in `CHAIN`, no other edits.

`MetricCol` (in `rollup/schemas/columns.py`) holds the three year-invariant
member names (`LOSS_UPLIFTED`, `LOSS_UPLIFTED_CAPPED`,
`LOSS_UPLIFTED_CAPPED_LOCALCCY`); `chain.CHAIN_BASE` points at the latter
as the chain's starting column.

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

The pipeline caches the combined all-factors node because many downstream
views and fan-outs read from it. This is done with polars lazy `.cache()`:

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

- **`audit_wide.parquet`** — one row per YLT event, columns ordered so the
  factor chain reads left-to-right. Every factor sits next to the metric
  it produces. Layout is registry-driven via `chain.audit_layout_cols`;
  width grows by 2 columns per `CHAIN` entry per forecast tag. Verify
  arithmetic row-by-row by eye.
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
rollup.seeds       loading 11 seeds from …/seeds
rollup.pipeline    plan: 12 Hisco variants across 2 vendors × 3 forecast dates × flavours
rollup.pipeline    forecast tags from seed: ['202601', '202607', '202701']
rollup.staging     loaded risklink YLT: …/risklink_ylt_*.parquet
rollup.staging     loaded verisk YLT: …/air_ylt_*.parquet
rollup.pipeline    staging: normalised YLTs concatenated
rollup.pipeline    event-id check (verisk): 80/80 rows matched air_events
rollup.staging     rollup_scope: filtered YLT to in_rollup=True triples
rollup.factors     currency: required_currency derived from CDS class; rate_to_gbp attached
rollup.factors     forecast: 3 factor columns attached — ['f_202601', 'f_202607', 'f_202701']
rollup.factors     euws: factor attached, rank overrides applied from seed
rollup.factors     fa_gross: aal + tail factors attached (1.0 for non-FA rows)
rollup.factors     uplift: base_model assigned; AAL computed via window functions
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
  `normalize_{verisk,risklink}_ylt`, `apply_rollup_scope`. The normalised
  YLT carries `office` + `lob_class` (from lobs) and
  `peril_name` + `region` + `peril_family` (from perils) so factor stages
  downstream have semantic dims without re-joining. `apply_rollup_scope`
  is a gate (not a factor) — it inner-joins `rollup_scope` to drop rows
  not officially in the rollup.
- `rollup/stages/factors.py` — `attach_currency`,
  `attach_forecast_factors`, `attach_rank`, `attach_euws`,
  `attach_fagross`, `attach_uplift`.
- `rollup/stages/ep.py` — `ep_curve_from_ylt` (auxiliary; not in the main
  fan-out chain, but used by integration tests).

## Domain constants — `rollup/config.py`

Closed-set domain values live as `StrEnum` so `pl.col(C) == VendorName.X`
and `pl.lit(VendorName.X)` work as drop-in replacements for raw strings:

| enum            | values                          | where it appears             |
| --------------- | ------------------------------- | ---------------------------- |
| `VendorName`    | `verisk` / `risklink`           | YLT `vendor` column, `base_model`, `analyses.vendor`, `blending_weights.vendor`, `rollup_scope.vendor` |
| `CurrencyCode`  | `GBP` / `EUR`                   | `attach_currency` derivation; values land in `required_currency` |
| `EpType`        | `AAL` / `AEP` / `OEP`           | `EP_TYPE` column emitted by `ep_curve_from_ylt` |
| `EnvVar`        | `ROLLUP_*` env var names        | every `os.getenv` / `monkeypatch.setenv` call |

Plus one non-enum constant:

- `FLOOD_FAMILY: str = "FL"` — `attach_uplift` checks
  `peril_family == FLOOD_FAMILY` (joined from `perils.csv`) to force
  `base_model='risklink'` for any flood peril. Replaces the older
  `FLOOD_PERILS = {"EU_FL", "UK_FL"}` substring set — semantic, not
  derived-string-based.

See [`factor-chain.md`](factor-chain.md) for how the factor stages compose.
