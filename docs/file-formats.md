# File-format reference

Quick reference for every file the pipeline reads. Each section lists the
**path**, **format**, **columns** (with dtype), and **notes**. For the
deeper *why* and the SQL recipes for populating the seeds, see
[`data-requirements.md`](data-requirements.md).

The pre-flight check (`uv run rollup --dry-run`) validates
every file listed here against its declared schema and reports any drift
with `filename | column | reason`.

---

## YLT parquets

### `data/ylt/verisk/air_ylt_*.parquet`

Multi-chunk parquet (`air_ylt_c1.parquet`, `air_ylt_c2.parquet`, …) scanned
as one lazy table. **CamelCase preserved** to match AIR Touchstone export.

| column              | dtype   | notes |
|---------------------|---------|-------|
| `Analysis`          | String  | label e.g. `EU_WS`; joins to `analyses.analysis_id` (vendor='verisk'). |
| `ExposureAttribute` | String  | the LOB on this row, e.g. `HIC_HH_UK`; joins to `lobs.modelled_lob`. |
| `CatalogTypeCode`   | String  | filtered to `'STC'` at staging. |
| `EventID`           | Int64   | event identifier; used for the EUWS join. |
| `ModelCode`         | Int64   | passed through to Hisco. |
| `YearID`            | Int64   | simulation year, 1..n_simulations. |
| `PerilSetCode`      | Int64   | not used; validated for shape. |
| `GroundUpLoss`      | Float64 | not used; `NetOfPreCatLoss` is the loss column. |
| `GrossLoss`         | Float64 | not used. |
| `NetOfPreCatLoss`   | Float64 | **the loss carried into the chain**. |
| `filename`          | String  | passthrough. |

### `data/ylt/risklink/risklink_ylt_*.parquet`

**One row per (yearid, eventid, anlsid)** — *not* a per-period summary.
Filter to `PERSPCODE='RL'` (ground-up loss) before exporting.

| column            | dtype   | notes |
|-------------------|---------|-------|
| `SimulationSetId` | Int64   | passthrough. |
| `yearid`          | Int64   | simulation year. |
| `eventid`         | Int64   | event identifier. |
| `date`            | String  | `YYYY-MM-DD`. |
| `p_value`         | Float64 | passthrough. |
| `anlsid`          | Int64   | analysis id; cast to String and joined to `analyses.analysis_id` (vendor='risklink'). |
| `name`            | String  | passthrough (e.g. `GB FL HD`). |
| `description`     | String  | passthrough. |
| `rate`            | Float64 | passthrough. |
| `meanloss`        | Float64 | passthrough. |
| `stddev`          | Float64 | passthrough. |
| `expvalue`        | Float64 | passthrough. |
| `loss`            | Float64 | **the loss carried into the chain**. |

> Per-event YLTs are only strictly required for `peril_family='FL'` analyses
> (where the base model is RiskLink). For non-flood perils, see
> [data-requirements.md → "Which RiskLink analyses do you actually need?"](data-requirements.md#which-risklink-analyses-do-you-actually-need-to-export).

---

## Seeds — `data/seeds/**/*.csv`

The pipeline auto-discovers seed CSVs by header match — file location
under `data/seeds/` doesn't matter. The 12 schemas below are the contract.

### `lobs` — `data/seeds/business/lobs.csv`

| column              | dtype  | notes |
|---------------------|--------|-------|
| `lob_id`            | Int64  | primary key. |
| `modelled_lob`      | String | natural key — what shows up in `ExposureAttribute`. |
| `rollup_lob`        | String | the rollup-level LOB. |
| `lob_type`          | String | classification. |
| `cds_cat_class_name`| String | drives currency derivation. |
| `office`            | String | drives forecast-factor join. |
| `class`             | String | drives forecast-factor join. |

### `perils` — `data/seeds/business/perils.csv`

| column         | dtype  | notes |
|----------------|--------|-------|
| `peril_id`     | Int64  | canonical PK; what every YLT row carries as `region_peril_id`. |
| `name`         | String | display label. |
| `region`       | String | `EU`, `UK`, … |
| `peril_family` | String | `WS`, `FL`, `EQ`, … **case-sensitive** — drives the flood-base-model rule. |

### `analyses` — `data/seeds/business/analyses.csv`

| column           | dtype  | notes |
|------------------|--------|-------|
| `vendor`         | String | `'verisk'` \| `'risklink'`. |
| `analysis_id`    | String | Verisk label or stringified RL analysis id. |
| `modelled_label` | String | display label e.g. `EU FL HD`. |
| `peril_id`       | Int64  | FK → `perils.peril_id`. |
| `lob_id`         | Int64  | nullable for verisk; populated for risklink. |

### `rollup_scope` — `data/seeds/business/rollup_scope.csv`

| column         | dtype   | notes |
|----------------|---------|-------|
| `modelled_lob` | String  | natural key from `lobs`. |
| `vendor`       | String  | `'verisk'` \| `'risklink'`. |
| `analysis_id`  | String  | the **modelled label** (e.g. `EU FL HD`), NOT the integer id. |
| `in_rollup`    | Boolean | `true` to include in the official rollup. |

### `blending_weights` — `data/seeds/vor/blending_weights.csv`

Long format. Generate with `uv run rollup derive-blending`
once `ep-summary-to-csv` has run.

| column        | dtype   | notes |
|---------------|---------|-------|
| `peril_id`    | Int64   | FK → `perils.peril_id`. |
| `peril_name`  | String  | denormalised display only. |
| `description` | String  | free-text reason. |
| `sub_peril`   | String  | nullable (sub-region splits). |
| `vendor`      | String  | `'verisk'` \| `'risklink'`. |
| `weight`      | Float64 | proportion; rl_weight + vk_weight should = 1.0 per peril. |

### `forecast_factors` — `data/seeds/vor/forecast_factors.csv`

Long format. Adding a forecast date is a data-only change.

| column          | dtype   | notes |
|-----------------|---------|-------|
| `class`         | String  | joins to `lobs.class`. |
| `office`        | String  | joins to `lobs.office`. |
| `office_iso2`   | String  | passthrough. |
| `forecast_date` | Date    | one of the per-tag forecast dates (e.g. `2026-01-01`). |
| `factor`        | Float64 | the multiplier. |

### `fx_rates` — `data/seeds/vor/fx_rates.csv`

| column            | dtype   | notes |
|-------------------|---------|-------|
| `currency_code`   | String  | source currency. |
| `target_currency` | String  | always `GBP` today. |
| `rate_date`       | Date    | snapshot date. |
| `rate`            | Float64 | source → target. |

### `euws_rate_factors` — `data/seeds/vor/euws_rate_factors.csv`

| column           | dtype   | notes |
|------------------|---------|-------|
| `model_event_id` | Int64   | joins to YLT `event_id` (verisk only). |
| `occ_year`       | Int64   | year of occurrence. |
| `factor`         | Float64 | per-event EUWS factor. |

### `euws_rank_overrides` — `data/seeds/adjustments/euws_rank_overrides.csv`

| column       | dtype   | notes |
|--------------|---------|-------|
| `rollup_lob` | String  | joins to `lobs.rollup_lob`. |
| `max_rank`   | Int64   | apply override when `rank ≤ max_rank`. |
| `factor`     | Float64 | replacement factor. |

### `fineart_adjustments` — `data/seeds/adjustments/fineart_adjustments.csv`

Optional. Empty = no fine-art adjustment (factor 1.0 for all rows).

| column                | dtype   | notes |
|-----------------------|---------|-------|
| `lob_id`              | Int64   | FK → `lobs.lob_id`. |
| `region_peril_id`     | Int64   | FK → `perils.peril_id`. |
| `applies_to_fa`       | Int64   | flag. |
| `rollup_region_peril` | String  | display. |
| `aal_factor`          | Float64 | applied today. |
| `tail_factor`         | Float64 | carried but not applied (future tail-loss work). |

### `air_events` — `data/seeds/validation/air_events.csv`

Verisk event catalogue. Optional stub.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | matches YLT `EventID`. |
| `model_id` | Int64  | model code. |
| `event`    | Int64  | event number. |
| `year`     | Int64  | calendar year. |
| `day`      | Int64  | day of year. |

### `risklink_events` — `data/seeds/validation/risklink_events.csv`

RiskLink event catalogue. Optional stub.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | matches YLT `eventid`. |
| `year`     | Int64  | calendar year. |
| `day`      | Int64  | day of year. |

---

## EP summaries — `data/ep_summaries/{vendor}/`

Vendor-supplied xlsx (multi-row header, wide RP columns) — **not** a
direct pipeline input. Convert to long format with:

    uv run rollup ep-summary-to-csv

The resulting `<stem>.long.csv` has `(id, rp, ep_type, lob, region_peril, gl)`
for risklink (`STG_RISKLINK_EP` schema).

Then derive blending weights:

    uv run rollup derive-blending

This rewrites `data/seeds/vor/blending_weights.csv` from the AAL totals.

---

## Outputs — `data/output/`

The pipeline writes here. **Created on run.**

| file                                              | when written | what it contains |
|---------------------------------------------------|---|---|
| `HiscoAIR_<yyyymm>_main.parquet` × N forecast dates | every run | full chain output for Verisk per forecast date |
| `HiscoAIR_dialsup.parquet`                        | every run | Verisk raw loss / FX only (no factors) |
| `HiscoRMS_<yyyymm>_main.parquet` × N              | every run | full chain output for RiskLink per forecast date |
| `HiscoRMS_dialsup.parquet`                        | every run | RiskLink raw loss / FX only |
| `mts_tbl_ylt_combined_all_factors.parquet`        | every run | **long format**, one row per (event × metric); identity + factor scalars + `(metric_name, value)` |
| `debug/audit_wide.parquet`                        | `--dump-interim` only | **wide format**, one row per event with every factor and metric side-by-side, layout matches the chain order |
| `debug/audit_long.parquet`                        | `--dump-interim` only | duplicate of the long-format file, kept under `debug/` for analyst convenience |

Today: 2 vendors × 3 forecast dates → **9 default files** + 2 debug files when `--dump-interim` is set.

### When to use `--dump-interim`

```bash
uv run rollup --yes --dump-interim
```

Adds the **wide** audit parquet under `data/output/debug/`. Wide is one row
per YLT event with columns laid out left-to-right in chain order:

`[identity dims] → raw loss → uplift → uplift_capped → localccy → (per
forecast tag: f_yyyymm → loss_..._fyyyymm) → (euws → loss_..._euws) →
(fa_gross → loss_..._fagross) → dialsup`

You can read across one row and verify each multiplication. Best when you
want to chase a specific event through the chain — the long-format file
forces you to pivot or filter to do the same.

---

## Querying the long-format combined parquet

`mts_tbl_ylt_combined_all_factors.parquet` is long because new metric
columns become data-only additions. Pivot to wide on the fly with DuckDB:

```sql
-- Wide view, all metrics:
PIVOT read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet')
ON metric_name
USING first(value);
```

`first(value)` is the cheapest aggregation — there's exactly one row per
(event, metric) so `sum`, `max`, `avg` would all return the same number.

Common variations:

```sql
-- Only specific metrics:
PIVOT read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet')
ON metric_name IN ('loss', 'loss_uplifted_capped', 'dialsup')
USING first(value);

-- Filter first, then pivot:
PIVOT (
  SELECT * FROM read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet')
  WHERE vendor = 'verisk' AND peril_family = 'FL'
)
ON metric_name USING first(value);

-- Persist as a wide parquet:
COPY (
  PIVOT read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet')
  ON metric_name USING first(value)
) TO 'data/output/wide.parquet' (FORMAT PARQUET);
```

From the DuckDB CLI:

```bash
duckdb -c "PIVOT read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet') ON metric_name USING first(value) LIMIT 10"
```

From Python:

```python
import duckdb
df = duckdb.sql("""
    PIVOT read_parquet('data/output/mts_tbl_ylt_combined_all_factors.parquet')
    ON metric_name USING first(value)
""").pl()    # → polars DataFrame
```

---

## SQL Server push — explicit, on-demand

Pushing the Hisco fanout parquets to SQL Server is an **explicit second
step**, not part of `rollup --yes`. Run the pipeline first; then push
when you're ready:

```bash
uv run rollup --yes              # writes 9 parquets to data/output/
uv run rollup push-to-sql        # lists, prompts, then pushes the 8 Hisco fanouts
```

### What `push-to-sql` does

1. Reads the connection string from `ROLLUP_MSSQL_CONN_STR` (or
   `MSSQL_CONN_STR` in `config.py` at the repo root). Aborts cleanly if absent.
2. Globs `Hisco*.parquet` under `data/output/`.
3. Prints the destination connection (credentials redacted), the target
   schema, the output dir, and the list of files with their sizes — then
   asks for `[y/N]` confirmation.
4. For each parquet: drops any existing SQL table of the same name and
   recreates it from the parquet content (`if_table_exists="replace"`).

### Connection string

Windows auth (no credentials inline — recommended):

```bash
export ROLLUP_MSSQL_CONN_STR='mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
```

SQL auth:

```bash
export ROLLUP_MSSQL_CONN_STR='mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server'
```

Or set `MSSQL_CONN_STR` in `config.py` at the repo root (gitignored — credentials never go to git). Both reports redact `user:pass@` when displayed.

### Flags

| flag | what it does |
|---|---|
| `--schema <name>` | Push into a specific SQL schema (e.g. `marts`). Default: server's default (typically `dbo`). |
| `-y` / `--yes` | Skip the y/N confirmation prompt. Required for non-TTY / scripted use; otherwise `push-to-sql` refuses to proceed without explicit confirmation. |

Example with both:

```bash
uv run rollup push-to-sql --schema marts --yes
```

### What gets pushed

- The 8 `Hisco{AIR,RMS}_*.parquet` fanout files → SQL tables of the same name (e.g. `HiscoAIR_202601_main`, `HiscoRMS_dialsup`).
- **Not pushed:** `mts_tbl_ylt_combined_all_factors.parquet` and the `--dump-interim` debug parquets — these are 21M-row audit artefacts kept parquet-only by design.

### Caveats

- **Full drop + recreate every push** (`if_table_exists="replace"`). No incremental loading. Big tables = long writes.
- pyodbc + a matching ODBC driver must be installed on the host (e.g. `ODBC Driver 17 for SQL Server`).
- Each parquet is read fully into memory before the write — for big tables, monitor the host's RAM.
- A failure on file N stops the push; tables 0..N-1 already replaced; table N may be in a partial state. Re-run after fixing the connection.

---

## Validating before a run

```bash
uv run rollup --dry-run
```

Per file the plan reports:

- ✓ when schema matches.
- ✘ with a column-level diff: `missing=[col1], wrong_dtype=[col2:Float64→Int64], unexpected=[col3]`.

If the plan is green you can run; if not, the failure tells you exactly
which file, which column, and what's wrong.
