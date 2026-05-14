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
| `Analysis`          | String  | label e.g. `EU_WS`; joins to `analyses.modelled_label` after numeric `analysis_id` filtering. |
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

The pipeline reads seed CSVs from fixed paths under `data/seeds/`. The 11
schemas below are the contract; headers and dtypes are validated before run.

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
| `analysis_id`    | String | numeric vendor analysis id, stored as text. Bundled Verisk values are placeholders. |
| `modelled_label` | String | vendor label e.g. `EU_FL` / `EU FL HD`; Verisk raw `Analysis` joins here. |
| `peril_id`       | Int64  | FK → `perils.peril_id`. |
| `lob_id`         | Int64  | nullable for verisk; populated for risklink. |

### `valid_analyses` — `data/seeds/business/valid_analyses.csv`

| column        | dtype  | notes |
|---------------|--------|-------|
| `vendor`      | String | `'verisk'` \| `'risklink'`. |
| `analysis_id` | String | numeric vendor analysis id, stored as text. Replace bundled Verisk placeholders with real IDs before production. |

Only listed analysis IDs contribute YLT rows or EP-summary rows. Peril and LOB
are still derived through `analyses.csv` and `lobs.csv`.

### `blending_weights` — `data/seeds/vor/blending_weights.csv`

Long format. Generate with `uv run rollup derive-blending`
once `ep-summary-to-csv` has run.

| column        | dtype   | notes |
|---------------|---------|-------|
| `peril_id`    | Int64   | FK → `perils.peril_id`. |
| `return_period` | Int64 | Weight bucket: `0`=AAL, `200`=1-in-200 OEP, `1000`=1-in-1000 OEP, `10000`=1-in-10000 OEP. |
| `peril_name`  | String  | denormalised display only. |
| `description` | String  | free-text reason. |
| `sub_peril`   | String  | nullable (sub-region splits). |
| `vendor`      | String  | `'verisk'` \| `'risklink'`. |
| `base_model`  | String  | `'verisk'` \| `'risklink'`; runtime lookup for fanout denominator/vendor. |
| `weight`      | Float64 | proportion; rl_weight + vk_weight should = 1.0 per peril + return_period. |

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

### `air_events` — `data/seeds/validation/verisk_events.parquet`

Verisk event catalogue. The seed loader projects the source parquet into the
canonical columns below.

Source parquet columns: `EventID`, `ModelID`, `Event`, `Year`, `Day`.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | canonical event id output as `ModelEventID`. |
| `model_id` | Int64  | model code, joined to YLT `ModelCode`. |
| `event`    | Int64  | matches YLT `EventID`. |
| `year`     | Int64  | matches YLT `YearID`. |
| `day`      | Int64  | day of year. |

### `risklink_events` — `data/seeds/validation/risklink_flood22_model_events.parquet`

RiskLink event catalogue. The seed loader derives `day` from
`ModelOccurrenceDate` and projects the source parquet into the canonical
columns below.

Source parquet columns used: `ModelEventID`, `ModelOccurrenceYear`,
`ModelOccurrenceDate`.

| column     | dtype  | notes |
|------------|--------|-------|
| `event_id` | Int64  | matches YLT `eventid`. |
| `year`     | Int64  | matches YLT `yearid`. |
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

Normal runs use the reviewed `data/seeds/vor/blending_weights.csv` seed. Use
`uv run rollup --derive-blending` to derive weights in-memory from these long
CSVs for one run and write `data/output/debug/derived_blending_weights.csv` for
audit. The explicit `derive-blending` subcommand rewrites the reviewed seed from
AAL plus 1-in-200, 1-in-1000, and 1-in-10000 OEP totals.

---

## Outputs — `data/output/`

The pipeline writes here. **Created on run.**

| file                                              | when written | what it contains |
|---------------------------------------------------|---|---|
| `HiscoAIR_<yyyymm>_main.parquet` × N forecast dates | every run | full chain output for Verisk per forecast date |
| `HiscoAIR_dialsup.parquet`                        | every run | Verisk sensitivity output: `loss × forecast × EUWS` |
| `HiscoRMS_<yyyymm>_main.parquet` × N              | every run | full chain output for RiskLink per forecast date |
| `HiscoRMS_dialsup.parquet`                        | every run | RiskLink sensitivity output: `loss × forecast × EUWS` |
| `mts_tbl_ylt_combined_all_factors.parquet`        | every run | **long format**, one row per (event × metric); identity + factor scalars + `(metric_name, value)` |
| `debug/audit_wide.parquet`                        | default; skip with `--no-audit` | **wide format**, one row per event with every factor and metric side-by-side, layout matches the chain order |
| `debug/audit_long.parquet`                        | default; skip with `--no-audit` | duplicate of the long-format file, kept under `debug/` for analyst convenience |

Today: 2 vendors × 3 forecast dates → **9 default files** + 2 debug audit files.

### Trimming small-loss rows — `MIN_LOSS = 1000` by default

Most events in a YLT contribute trivially small losses to the EP curve.
The pipeline **drops rows whose loss is below 1000 by default** —
production cuts ~65% off the parquet size with no analytical impact.

Override precedence (highest first):

1. **CLI flag** — `--min-loss 2500` for a one-off
2. **Env var** — `ROLLUP_MIN_LOSS=0` for the shell session / CI
3. **`rollup.local.toml` at the repo root** — `[run].min_loss = 500.0` for set-and-forget
4. **Code default** — `1000.0` if nothing above is set

To **disable** the filter (keep every row, e.g. for full audit work):

```bash
uv run rollup --yes --min-loss 0
# or, persistent:
cp rollup.example.toml rollup.local.toml
# edit [run] min_loss = 0.0
```

The repo ships a `rollup.example.toml` template:

```bash
cp rollup.example.toml rollup.local.toml
# edit MIN_LOSS or any other override
```

`rollup.local.toml` is **gitignored** — it never goes to git, credentials and
local paths stay private. The plan reports the active threshold:

```bash
uv run rollup --dry-run            # quiet by default
uv run rollup --yes --log-level INFO   # logs `min_loss filter: dropping rows where loss < 1000.0`
```

The filter is applied to each variant's `ModelGrossLoss` for the Hisco
fanouts, and to the `value` column for
`mts_tbl_ylt_combined_all_factors.parquet`. The debug audit parquets are *not*
filtered — they're for analyst introspection.

### Audit outputs

```bash
uv run rollup --yes             # writes debug audit outputs by default
uv run rollup --yes --no-audit  # skip debug audit outputs
```

Adds the **wide** audit parquet under `data/output/debug/`. Wide is one row
per YLT event with columns laid out left-to-right in chain order:

`[identity dims] → raw loss → uplift → uplift_capped → localccy → (per
forecast tag: f_yyyymm → loss_..._fyyyymm) → (euws → loss_..._euws) →
dialsup`

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
step**, not part of `rollup --yes`. Run the pipeline first, optionally
test the connection, then push:

```bash
uv run rollup --yes              # writes 9 parquets to data/output/
uv run rollup test-sql           # read-only probe — confirm the conn works
uv run rollup push-to-sql        # lists, prompts, then pushes the 8 Hisco fanouts
```

### `test-sql` — verify the connection before you push

```bash
uv run rollup test-sql                    # connect, report @@VERSION + database
uv run rollup test-sql --schema marts     # also check that schema exists
```

There are also automated integration tests for `test-sql` and
`push-to-sql` that boot a real SQL Server container via Docker:

```bash
uv run pytest --run-integration           # 6 integration tests, ~60-90s on first run
```

These are **skipped by default** — pass `--run-integration` (or `-m
integration`) to opt in. Requires Docker on the host. The container is
torn down on test exit; nothing persists on disk.

A successful run prints the server version, the connected database, and
(if `--schema` was given) whether that schema exists. A failure prints the
exact driver / sqlalchemy error so you know whether it's a missing driver,
a bad hostname, an auth problem, or a missing schema.

Read-only — never writes. Use this before `push-to-sql` to catch
config issues without destroying any tables.

### What `push-to-sql` does

1. Reads the connection string from `[sql].mssql_conn_str` in
   `rollup.local.toml` (or `ROLLUP_MSSQL_CONN_STR` for CI/shell overrides).
   Aborts cleanly if absent.
2. Globs `Hisco*.parquet` under `data/output/`.
3. Prints the destination connection (credentials redacted), the target
   schema, the output dir, and the list of files with their sizes — then
   asks for `[y/N]` confirmation.
4. For each parquet: drops any existing SQL table of the same name and
   recreates it from the parquet content (`if_table_exists="replace"`).

### Connection string

Persistent local config (recommended for operator machines):

```toml
[sql]
mssql_conn_str = "mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
```

Windows auth as a one-off shell override:

```bash
export ROLLUP_MSSQL_CONN_STR='mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'
```

SQL auth:

```bash
export ROLLUP_MSSQL_CONN_STR='mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server'
```

Both reports redact `user:pass@` when displayed.

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
- **Not pushed:** `mts_tbl_ylt_combined_all_factors.parquet` and the debug audit parquets — these are large audit artefacts kept parquet-only by design.

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
