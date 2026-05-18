# Rollup Polars pipeline

This repository contains a simplified catastrophe rollup pipeline implemented in
Polars. The active orchestration is in `polars/rollup/pipeline.py`; the command
line interface is in `polars/rollup/cli.py`.

The pipeline reads seed CSVs, vendor YLT parquet files, and canonical EP summary
CSVs, then writes mart parquet outputs for downstream consumers on every run.

## Quickstart

Run from the repository root.

### Docs preview

`zensical` is installed as a dev dependency and `zensical.toml` exists, but the
configured `docs/` source directory is not present. For now, this README is the
primary docs and no working local docs server is configured.

### Drop in data first

- YLT parquet: `data/ylt/verisk/*.parquet`, `data/ylt/risklink/*.parquet`
- EP summaries: `data/ep_summaries/verisk/verisk_ep_summary.long.csv`,
  `data/ep_summaries/risklink/rms_ep_summary.long.csv`
- Seed CSVs: required files under `data/seeds/...`
- Validation catalogues: parquet files under `data/seeds/validation/`

### Run

Preferred script entrypoint from `pyproject.toml`:

```bash
uv run rollup validate
uv run rollup run
uv run rollup run --debug
```

Outputs:

- Mart fanouts: `data/output/marts/`
- Wide/report outputs: `data/output/`
- Debug frames from `--debug`: `data/output/debug/`

## Commands

Run commands from the repository root.

```bash
uv run python -m rollup.cli validate
uv run python -m rollup.cli run
uv run python -m rollup.cli run --debug
```

`pyproject.toml` also defines `rollup = "rollup.cli:main"`, so these shorter
forms are available:

```bash
uv run rollup validate
uv run rollup run
uv run rollup run --debug
```

Optional custom data root:

```bash
uv run python -m rollup.cli --data-root data validate
uv run python -m rollup.cli --data-root data run --debug
```

## Input layout

Schemas are declared in colocated `schema.yaml` files. `validate` checks existing
seed CSVs, YLT parquet files, and EP summary CSVs against those schemas.

```text
data/
  seeds/
    business/
      lobs.csv
      perils.csv
    vor/
      blending_factors.csv
      forecast_factors.csv
      fx_rates.csv
      euws_rate_factors.csv
    adjustments/
      euws_rank_overrides.csv
    validation/
      verisk_events.parquet
      risklink_flood22_model_events.parquet
    schema.yaml
  ylt/
    verisk/*.parquet
    risklink/*.parquet
    schema.yaml
  ep_summaries/**/*.long.csv
```

### Input files and purpose

| Area | File(s) | Purpose |
| --- | --- | --- |
| Business lookup | `data/seeds/business/lobs.csv` | Maps source `modelled_lob` values to `rollup_lob`, class, office, currency, and CDS class metadata used in EP and YLT enrichment. |
| Business lookup | `data/seeds/business/perils.csv` | Maps source `modelled_peril` values to `rollup_peril`, region/peril labels, and `region_peril_id`. |
| VOR factors | `data/seeds/vor/blending_factors.csv` | Supplies blend weights by rollup peril and return-period bucket. Europe Flood / `RegionPerilID = 216` uses `SubRegionPerilID = 216b`. |
| VOR factors | `data/seeds/vor/fx_rates.csv` | Converts source currency losses to GBP. Missing FX rows are not defaulted. |
| VOR factors | `data/seeds/vor/forecast_factors.csv` | Applies forecast-date factors by class, office, and forecast date. Missing factors default to `1.0`. |
| VOR factors | `data/seeds/vor/euws_rate_factors.csv` | Applies Europe Windstorm event-level rate factors after forecast and FX. |
| Adjustments | `data/seeds/adjustments/euws_rank_overrides.csv` | Overrides selected zero EUWS factors for configured top-ranked rows. |
| Validation catalogues | `data/seeds/validation/verisk_events.parquet` | Maps Verisk event/year/model fields to model event IDs and event days. |
| Validation catalogues | `data/seeds/validation/risklink_flood22_model_events.parquet` | Provides RiskLink flood model occurrence dates used to derive event-day values. |
| EP summaries | `data/ep_summaries/risklink/rms_ep_summary.long.csv` | Canonical RiskLink long EP summary used to determine selected RiskLink modelling scope and EP blend targets. |
| EP summaries | `data/ep_summaries/verisk/verisk_ep_summary.long.csv` | Canonical Verisk long EP summary used to determine selected Verisk modelling scope and EP blend targets. |
| YLTs | `data/ylt/verisk/*.parquet` | Raw Verisk YLT rows. Filtered and enriched from selected EP summary variants. |
| YLTs | `data/ylt/risklink/*.parquet` | Raw RiskLink YLT rows. Filtered and enriched from selected EP summary variants. |

### High-level input contracts

- Seeds: every `data/seeds/**/*.csv` must have a matching filename schema in
  `data/seeds/schema.yaml`.
- EP summaries: canonical long CSVs under `data/ep_summaries/**/*.long.csv` with
  columns `vendor`, `analysis_id`, `modelled_lob`, `modelled_peril`, `ep_type`,
  `return_period`, `loss`. These summaries are the source/version of truth for
  what gets modelled. There is no `selected_analyses.csv`; selected scope comes
  from the canonical long EP summaries plus the `lobs.csv` and `perils.csv`
  lookup seeds. YLT rows are then filtered and enriched using the selected EP
  summary variants.
- Verisk YLTs: parquet under `data/ylt/verisk/*.parquet`, including `Analysis`,
  `ExposureAttribute`, `ModelCode`, `YearID`, `EventID`, and `GroundUpLoss`.
- RiskLink YLTs: parquet under `data/ylt/risklink/*.parquet`, including
  `anlsid`, `yearid`, `eventid`, and `loss`.
- Validation catalogues: `verisk_events.parquet` maps Verisk event/year/model to
  model event IDs and event days. RiskLink flood event day is derived from
  `risklink_flood22_model_events.parquet` using `ModelOccurrenceDate`
  day-of-year.

## Pipeline stages

`run()` returns a `PipelineRunResult` with four stage buckets:

- `seeds`: validated seed frames and event catalogues.
- `staging`: input validation reports, normalized YLTs, and enriched/selected EP
  summaries.
- `intermediate`: joined EP summaries, blending targets, YLT blending, FX,
  forecast, EUWS, and DIALSUP frames.
- `marts`: fanout frames for final parquet output.

Debug mode writes every stage frame to `data/output/debug/` using these prefixes:

- `seed_*`
- `stg_*`
- `int_*`
- `mts_*`

Run debug mode with the script entrypoint:

```bash
uv run rollup run --debug
```

Inspect debug parquet files with DuckDB, for example:

```bash
duckdb -c "SELECT * FROM 'data/output/debug/int_ylt_blending_applied.parquet' LIMIT 10;"
duckdb -c "SELECT COUNT(*) FROM 'data/output/debug/stg_ep_summaries_selected.parquet';"
duckdb -c "SELECT vendor, rollup_peril, COUNT(*) AS rows FROM 'data/output/debug/int_ylt_combined_enriched.parquet' GROUP BY vendor, rollup_peril ORDER BY vendor, rollup_peril;"
duckdb -c "SELECT * FROM 'data/output/debug/mts_event_validation.parquet' LIMIT 20;"
duckdb -c "SELECT forecast_date, rollup_peril, COUNT(*) AS rows FROM 'data/output/debug/mts_ylt_combined_all_factors.parquet' GROUP BY forecast_date, rollup_peril ORDER BY forecast_date, rollup_peril;"
```

### EP variant selection priorities

`stage_ep_summaries()` enriches canonical EP summaries with `lobs.csv` and
`perils.csv`, then chooses one `modelled_peril` per `vendor + rollup_lob +
rollup_peril`. The selected variant is the lowest `selection_priority`; unlisted
variants default to priority `100`. If multiple variants tie at the same
priority, the sort order by `modelled_peril` decides the first candidate.

Concrete priorities:

| Vendor | Rollup peril | Priority order |
| --- | --- | --- |
| RiskLink | `Europe_EQ` | `EUxESGB EQ Adj` (1), then `EUxESGB EQ` (2) |
| RiskLink | `Europe_WS` | `EUxGB WS CVV (FlrArea)` (1), `EUxGB WS CVV` (2), `EUxGB WS (FlrArea)` (3), `EUxGB WS` (4) |
| RiskLink | `UK_WS` | `GB WSSS CVV (FlrArea)` (1), `GB WSSS CVV` (2), `GB WSSS (FlrArea)` (3), `GB WSSS` (4) |
| Verisk | `Europe_EQ` | `EU_EQ` (1) |
| Verisk | `Europe_FL` | `EU_FL` (1) |
| Verisk | `Europe_WS` | `EU_WS_GCAdj` (1), then `EU_WS` (2) |
| Verisk | `UK_FL` | `UK_FL` (1) |
| Verisk | `UK_WS` | `UK_WSSS_GCAdj` (1), then `UK_WSSS` (2) |

## Main calculations

1. Load and validate seeds, YLT parquet files, and EP summary CSVs.
2. Normalize YLTs into a common shape: `vendor`, `analysis_id`, modelled
   LOB/peril, model/year/event IDs, and `loss`.
3. Enrich EP summaries with `lobs.csv` and `perils.csv` metadata.
4. Select one EP variant per `vendor + rollup_lob + rollup_peril` using the
   priority system above.
5. Enrich YLTs from the selected EP summaries. Verisk joins by modelled
   LOB/peril; RiskLink joins by `analysis_id`.
6. Join Verisk and RiskLink EP summaries side by side as `int_ep_vendor_joined`.
7. Keep blending target points `AAL 0`, `OEP 200`, and `OEP 1000`.
8. Apply `blending_factors.csv`; Europe Flood / `RegionPerilID = 216` uses
   `SubRegionPerilID = 216b`.
9. Set `base_model` to RiskLink for `Europe_FL` and `UK_FL`, otherwise Verisk.
10. Calculate `uplift_factor_on_base_model = target_loss / base_model_loss`.
11. Keep only base-model YLT rows.
12. Rank YLT losses descending by modelled LOB and rollup peril. Return period is
    `100_000 / rnk` for RiskLink and `10_000 / rnk` for Verisk. `rp_bucket` is
    numeric `0`, `200`, or `1000`.
13. Apply blending factors to create `original_ylt_loss_blended`.
14. Convert to GBP with `fx_rates.csv`, producing `original_ylt_loss_blended_gbp`.
15. Cross join forecast dates and apply `forecast_factors.csv` by `class`,
    `office`, and `forecast_date`; missing factors default to `1.0`.
16. Apply EUWS raw factors for Europe Windstorm only using the Verisk event
    mapping.
17. Apply `euws_rank_overrides.csv` so selected zero EUWS factors become the
    configured override factor for top-ranked rows.
18. Produce DIALSUP separately from base-model YLT. DIALSUP does not apply
    blending or EUWS. Formula:
    `dialsup_loss_gbp_forecast = loss * fx_rate * forecast_factor`.
19. Build mart fanout frames and write parquet output.

## Outputs

Every run writes mart outputs. Fanout parquet files go to `data/output/marts/`:

```text
HiscoAIR_YYYYMM_main.parquet
HiscoRMS_YYYYMM_main.parquet
HiscoAIR_YYYYMM_dialsup.parquet
HiscoRMS_YYYYMM_dialsup.parquet
```

`YYYYMM` is derived from `forecast_date`. AIR corresponds to Verisk base-model
rows; RMS corresponds to RiskLink base-model rows.

Fanout schema:

- `ModelEventID`
- `ModelYear`
- `CurrencyCode`
- `ModelYOA`
- `ModelGrossLoss`
- `ModelInwardsReinstatement`
- `ModelEventDay`
- `LossClassName`

Wide/report parquet files go to `data/output/`:

- `mts_tbl_ylt_combined_all_factors.parquet`
- `mts_tbl_ylt_dialsup.parquet`
- `mts_event_validation.parquet`

## Validation and troubleshooting

- `validate` checks existing seed CSVs, YLT parquet files, and EP summary CSVs
  against configured schemas, then prints validation reports.
- It does not yet prove every schema-declared seed exists, and it does not
  schema-validate validation parquet catalogues.
- The command exits non-zero if a checked input has schema errors or a required
  scanned input area is missing.
- If a run produces empty outputs, inspect `data/output/debug/` from `run
  --debug`, especially `stg_ep_summaries_selected`, `int_ep_blending_targets`,
  and `int_ylt_blending_applied`.
- Missing FX rows are not defaulted; each source currency must have a GBP rate.
- Missing forecast factors default to `1.0`.
- EUWS factors only affect `Europe_WS`; other perils receive factor `1.0`.

## Known limitations and follow-up work

- Validate missing RiskLink event days.
- Confirm final RiskLink event-day join keys.
- Validate Verisk and RiskLink event IDs against validation parquet catalogues.
- Clean up repetitive `intermediate_frames[...] = ...` wiring.
