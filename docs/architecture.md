# Architecture

The rollup pipeline is a file-based batch process that uses a **flat
incremental** execution model. Each transform reads the previous stage's
output, produces only its own new rows, and is collected independently.
There are no nested `pl.concat` calls (which trigger a Polars CSE
optimizer bug on large UNION trees — see
<https://github.com/pola-rs/polars/issues/21241>).

## Data flow

```mermaid
flowchart TD
  A[Analyst drops inputs under data/] --> B[Schemas and validation]
  B --> C[Seed lookups\nlobs.csv, perils.csv, VOR factors, validation catalogues]
  B --> D[EP summary staging\nenrich and select preferred peril]
  B --> E[YLT normalization\nVerisk and RiskLink]
  C --> D
  C --> E
  D --> F[EP join and blending targets\nAAL, OEP 200, OEP 1000]
  E --> G[YLT enrichment\nLOB, peril, class, office, currency]
  F --> H[Base-model YLT\nselection, rank, blending, uplift]
  G --> H
  H --> I[FX, forecast, EUWS factors\nand EUWS overrides]
  H --> J[DIALSUP branch\nrank props + FX + forecast only]
  I --> K[Main mart fanout]
  J --> L[DIALSUP mart fanout]
  I --> O[mts_tbl_ylt_combined_all_factors]
  J --> P[mts_tbl_ylt_dialsup]
  K --> M[Wide MTS outputs]
  L --> M
  K --> N[mts_event_validation]
  L --> N
  M --> Q[EP analysis report]
```

## Pipeline phases

| Phase | What happens | Debug prefix |
| --- | --- | --- |
| Seed + validation | Read seed files, event catalogues, YLTs, and EP summaries; report schema and lookup coverage issues. | `seed_*` |
| Staging | Normalize YLT formats and stage EP summaries with LOB/peril enrichment and preferred modelled peril selection. | `stg_*` |
| Intermediate | Join EP vendors, calculate blend targets, enrich YLT rows, apply blending, FX, forecast, EUWS, and build DIALSUP. | `int_*` |
| Marts | Build main/DIALSUP fanouts, combined-all-factors output, event validation, and wide MTS. | `mts_*` |

## Forecast factor and output shaping

The forecast step expands each YLT row across all forecast dates:

1. All unique `forecast_date` values from `forecast_factors.csv` are extracted.
2. Each YLT row is cross-joined with those dates — 1 input row becomes N output rows.
3. The forecast factor is left-joined on `(class, office, forecast_date)`. Missing factors default to `1.0`.
4. The forecasted loss = `original_ylt_loss_blended_gbp * forecast_factor`.

**Long output** (`mts_tbl_ylt_combined_all_factors.parquet`): one row per (event × forecast_date), with columns for each intermediate loss stage (`original_ylt_loss`, `_blended`, `_gbp`, `_forecast`, `_euws`) and the contributing factor values (`forecast_factor`, `fx_rate`, `euws_factor`, `uplift_factor_on_base_model`).

**Wide output** (`mts_tbl_ylt_combined_all_factors_wide.parquet`): the same data pivoted so each forecast date becomes a separate column per metric — e.g. `euws_override_202601_loss`, `dialsup_gbp_forecast_202601_loss`. Dimension columns are automatically detected as all non-metric, non-forecast-date, non-loss columns present in both the main and DIALSUP frames. Rank columns (`rnk`, `rp`, `rp_bucket`) from the blending step are included because they propagate through the DIALSUP branch via `ylt_ranked.lazy()` (ensuring unique pivot index rows).

## Execution model

Each intermediate stage works independently: the transform reads the
previous stage's output (a collected `DataFrame`), executes a fresh
lazy plan with `.lazy()`, and collects the result with `.collect()`.
All stage outputs are concatenated at the end with a DataFrame-level
`pl.concat(..., how="diagonal")` — no lazy UNIONs, no nested plans,
no CSE optimizer issues.

Key properties:

- **Flat incremental**: each transform returns only the new rows it
  produces (with a new `metric` value). Stages do not re-read or wrap
  previous outputs.
- **No UNION nesting**: intermediate `.collect()` after each stage
  ensures each plan is a simple linear chain (source → joins →
  projections). The CSE optimizer is never invoked on a deeply nested
  UNION tree.
- **Rank columns propagate to DIALSUP**: `calculate_dialsup` receives
  `ylt_ranked.lazy()` (not `ylt_original`), so `rnk`, `rp`, and
  `rp_bucket` appear in both the main and DIALSUP frames. This is
  critical for the wide pivot: these columns become part of the
  automatically detected dimension set, preventing duplicate
  `(dims + forecast_date)` groups from being collapsed by
  `aggregate_function="first"`.
- **Cross-join scoped to subset**: `apply_forecast_to_ylt` cross-joins
  only the `gbp` subset (~4.9M rows) instead of the full accumulated
  pile (~14.7M rows), keeping peak intermediate rows at ~29M instead of
  ~117M.

## Pipeline transforms

| # | Step | Function | Input | Output | Key columns created | Purpose |
|---|------|----------|-------|--------|---------------------|---------|
| 1 | Normalize YLT | `normalize_ylt` | Raw Verisk/RiskLink YLT parquets | Normalized YLT | `vendor`, `modelled_lob`, `modelled_peril`, `loss`, `year_id`, `event_id` | Standardise vendor column names to canonical schema |
| 2 | Stage EP summaries | `stage_ep_summaries` | EP long CSVs + seeds | Staged EP summaries | `rollup_lob`, `rollup_peril`, `cds_cat_class_name`, `class_`, `office`, `currency` | Enrich EP data with seed lookups and select one modelled peril per (vendor, rollup_lob, rollup_peril) |
| 3 | Join EP vendors | `join_ep_summaries` | Staged EP summaries | Joined EP summaries | `verisk_loss`, `risklink_loss` | Aggregate EP losses per vendor at `(rollup_lob, rollup_peril, region_peril_id, ep_type, return_period)` grain |
| 4 | Calculate blend targets | `calculate_ep_blending_targets` | Joined EP summaries + `blending_factors.csv` | Blending targets | `target_loss`, `base_model`, `base_model_loss`, `uplift_factor_on_base_model` | Compute blended EP target and uplift factor per RP bucket, clamped to [0.1, 10.0] |
| 5 | Enrich YLT | `enrich_ylt_with_ep_summaries` | Normalized YLT + staged EP summaries | Enriched YLT | `rollup_lob`, `rollup_peril`, `region_peril_id`, `cds_cat_class_name`, `class_`, `office`, `currency` | Attach seed enrichment columns to each YLT row via inner join |
| 6A | Add rank columns | `_add_rank_columns` | Enriched YLT | Ranked YLT | `rnk`, `rp`, `rp_bucket` | Rank events within `(vendor, modelled_lob, rollup_peril)` and bucket by return period |
| 6B | Blend YLT | `apply_ep_blending_to_ylt` | Ranked YLT + blending targets | Blended YLT | `original_ylt_loss`, `original_ylt_loss_blended` | Apply uplift factor based on rank bucket |
| 7 | Build DIALSUP | `calculate_dialsup` | Ranked YLT (rank columns propagate) + Verisk events + `fx_rates.csv` + `forecast_factors.csv` | DIALSUP YLT | `dialsup_original_ylt_loss`, `dialsup_loss_gbp`, `dialsup_loss_gbp_forecast` | Build DIALSUP branch — raw loss + FX + forecast, no blending or EUWS |
| 8 | Apply FX | `apply_fx_to_ylt` | Blended YLT + `fx_rates.csv` | FX-applied YLT | `fx_rate`, `target_currency`, `original_ylt_loss_blended_gbp` | Convert blended loss to GBP |
| 9 | Apply forecast | `apply_forecast_to_ylt` | FX-applied YLT + `forecast_factors.csv` | Forecast-applied YLT | `forecast_date`, `forecast_factor`, `original_ylt_loss_blended_gbp_forecast` | Cross-join forecast dates, apply class/office multipliers |
| 10 | Apply EUWS | `apply_euws_to_ylt` | Forecast-applied YLT + Verisk events + `euws_rate_factors.csv` | EUWS-applied YLT | `model_event_id`, `event_day`, `euws_factor_raw`, `euws_factor` | Map Verisk events, apply Europe Windstorm event factors |
| 11 | Apply EUWS overrides | `apply_euws_overrides_to_ylt` | EUWS-applied YLT + `euws_rank_overrides.csv` | Override-applied YLT | `euws_override_applied`, `original_ylt_loss_blended_gbp_forecast_euws` | Override EUWS factor to configured value for top-ranked zero-factor events |
| 12 | Main fanout | `build_main_fanout` | Override-applied YLT + RiskLink flood events | Main fanout | `ModelEventID`, `ModelYear`, `CurrencyCode`, `ModelGrossLoss`, `ModelEventDay` | Format mart-ready output with standard fanout column names |
| 13 | DIALSUP fanout | `build_dialsup_fanout` | DIALSUP YLT + RiskLink flood events | DIALSUP fanout | Same fanout columns | Format mart-ready output for DIALSUP |
| 14 | Combined all-factors (long) | DataFrame-level `pl.concat(..., how="diagonal")` | All 6 stage DataFrames | `mts_tbl_ylt_combined_all_factors` | `metric` (differentiates `original`, `blended`, `gbp`, `forecast`, `euws`, `euws_override`, `dialsup_gbp_forecast`) | Row-stack all intermediate loss stages with `how="diagonal"` — columns not present in every stage become null |
| 15 | Wide MTS | `_write_combined_outputs` → pivot | Combined all-factors (long) + DIALSUP | `mts_tbl_ylt_combined_all_factors_wide` | `euws_override_YYYYMM_loss`, `dialsup_gbp_forecast_YYYYMM_loss` | Pivot forecast dates into wide columns per metric; dimensions auto-detected (all non-metric, non-forecast-date, non-loss columns present in both frames) |

Normal runs write only final outputs. Use `uv run rollup run --debug` when you
need intermediate parquet frames in `output/debug/`.

## Performance characteristics

- The flat incremental pipeline runs in ~22s (no debug) vs ~88s for the
  original column-accumulation pipeline (~4× faster).
- The accumulated UNION pattern (wrapping each stage with `pl.concat`)
  triggers a Polars CSE optimizer panic at
  `crates/polars-plan/src/plans/optimizer/cse/cache_states.rs:354:26`
  on production data — **do not use it**. The flat incremental model
  (`lazy()` → planar transform → `collect()`) avoids nesting entirely.
- Each stage's `.collect()` materializes a small linear plan. Peak
  memory is dominated by the cross-join in step 9 (~29M intermediate
  rows), which operates only on the `gbp` subset (~4.9M input rows).
