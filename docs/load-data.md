# Loading your data

Step-by-step walkthrough. Follow in order; verify at each step before moving on.

## Step 0 — Create the directory layout

macOS/Linux:

```bash
mkdir -p data/ylt/verisk data/ylt/risklink data/ep_summaries/verisk data/ep_summaries/risklink data/output
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force data/ylt/verisk, data/ylt/risklink, data/ep_summaries/verisk, data/ep_summaries/risklink, data/output
```

Seed CSVs and validation parquet catalogues already exist in `data/seeds/`.

## Step 1 — Populate the run-scope seeds

Copy these CSVs into their fixed seed locations:

1. **`data/seeds/business/perils.csv`** — peril dimension (`peril_id`, `name`, `region`, `peril_family`)
2. **`data/seeds/business/analyses.csv`** — vendor analysis metadata → peril, plus RiskLink LOB
3. **`data/seeds/business/selected_analyses.csv`** — analyst-selected EP analysis IDs for this run
4. **`data/seeds/vor/blending_weights.csv`** — reviewed RiskLink / Verisk proportions

If you don't have these, see [`polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md).

Other seeds (`lobs.csv`, `forecast_factors.csv`, `euws_*`, `fx_rates.csv`, `verisk_events.parquet`, `risklink_flood22_model_events.parquet`) are dbt/reference-owned inputs. For details, see [`data/seeds/README.md`](../data/seeds/README.md).

### Step 1a — Select analysis IDs from EP summaries

`selected_analyses.csv` is authoritative when present. RiskLink selections use
the numeric `anlsid` / EP-summary `ID` as text. Verisk selections use the EP
`Analysis` label, for example `EU_WS_GCAdj`; numeric Verisk placeholders in
`analyses.csv` are metadata only and are not valid selected IDs.

Example `analyses.csv` rows:

```csv
vendor,analysis_id,modelled_label,peril_id,lob_id
verisk,900003,EU_WS,3,
risklink,13,EUxGB WS,3,16
```

Example `selected_analyses.csv` rows:

```csv
vendor,analysis_id,include
verisk,EU_WS_GCAdj,true
risklink,13,true
```

`valid_analyses.csv` remains a legacy compatibility allow-list only when
`selected_analyses.csv` is absent.

**Verify:**
```bash
uv run rollup --dry-run
```
All 11 seeds should show ✓. If any show ✘, see [Troubleshooting](troubleshooting.md).

## Step 2 — Drop Verisk YLTs into `data/ylt/verisk/`

Copy parquets with filename pattern `air_ylt_*.parquet`.

**Required schema:**

| Column | Type | Notes |
|--------|------|-------|
| `Analysis` | String | e.g. `EU_WS`, joined to `analyses.modelled_label` after numeric ID filtering |
| `ExposureAttribute` | String | LOB string (e.g. `HIC_HH_UK`) |
| `CatalogTypeCode` | String | filter: only `STC` rows kept |
| `EventID` | Int64 | event identifier |
| `ModelCode` | Int64 | model code |
| `YearID` | Int64 | simulation year |
| `PerilSetCode` | Int64 | peril set code |
| `GroundUpLoss` | Float64 | unused |
| `GrossLoss` | Float64 | unused |
| `NetOfPreCatLoss` | Float64 | **loss column used by pipeline** |
| `filename` | String | passthrough |

**Verify schema:**
```bash
uv run python -c "import polars as pl; print(pl.scan_parquet('data/ylt/verisk/air_ylt_*.parquet').collect_schema())"
```

**Verify load:**
```bash
uv run rollup --dry-run
```

## Step 3 — Drop RiskLink YLTs into `data/ylt/risklink/`

Copy parquets with filename pattern `risklink_ylt*.parquet` (lowercase columns).
Extra source columns are allowed but ignored; these are the required columns.

**Required schema:**

| Column | Type | Notes |
|--------|------|-------|
| `yearid` | Int64 | simulation year (lowercase) |
| `eventid` | Int64 | event id (lowercase) |
| `p_value` | Float64 | |
| `anlsid` | Int64 | analysis id — joined to `analyses.csv` |
| `meanloss` | Float64 | |
| `stddev` | Float64 | |
| `expvalue` | Float64 | |
| `loss` | Float64 | **loss column used by pipeline** |

**Need RiskLink?** Yes if modeling flood perils (`peril_family = 'FL'`); optional for wind/EQ.

## Step 4 — Review blending weights

Normal `uv run rollup` runs use the fixed, reviewed
`data/seeds/vor/blending_weights.csv` seed. Provide this from the
blending-factor table that specifies model shares (for example 50/50 or 80/20).
EP summaries do not calculate or override these weights.

**Step 4a — Convert Excel to long CSV (RiskLink only):**

macOS/Linux:

```bash
cp rms_analysis_list.xlsx data/ep_summaries/risklink/
uv run rollup ep-summary-to-csv
```

Windows PowerShell:

```powershell
Copy-Item rms_analysis_list.xlsx data/ep_summaries/risklink/
uv run rollup ep-summary-to-csv
```

**Step 4b — Prepare Verisk CSVs manually:**

Produce long CSVs with columns: `rp`, `ep_type`, `analysis`, `lob`, `gl`.
Copy to `data/ep_summaries/verisk/` (files must end in `.long.csv`).
The `analysis` values are Verisk labels (for example `EU_WS`) and must match
`analyses.modelled_label`, not the numeric placeholder/production ID.

`data/seeds/vor/blending_weights.csv` uses this schema:

| column | meaning |
|---|---|
| `peril_id` | FK into `perils.csv` |
| `return_period` | Weight bucket: `0`=AAL, `200`=1-in-200, `1000`=1-in-1000, `10000`=1-in-10000 |
| `base_model` | The model to use as denominator: `"risklink"` for FL perils, `"verisk"` otherwise |
| `verisk_weight` | Verisk/AIR blending proportion |
| `risklink_weight` | RiskLink/RMS blending proportion |

At runtime each YLT event is ranked largest-to-smallest within
`(vendor, lob_id, peril_id)`, converted to `rp = n_sim / rank`, bucketed to
`0`, `200`, `1000`, or `10000`, then joined to the matching blending weight.

Weights are applied exactly as provided by the seed.

## Step 5 — Full verification

```bash
uv run rollup --dry-run
```

All sections should show ✓. If any show ✘, see [Troubleshooting](troubleshooting.md).

## Step 6 — Run the pipeline

```bash
uv run rollup              # interactive wizard
uv run rollup --yes        # non-interactive
```

Output: Hisco parquets plus audit/debug parquets in `data/output/` (~15–40 seconds depending on data size).
The interactive wizard confirms input paths, forecast factor coverage, minimum loss, audit outputs, and optional SQL push.

**Inspect output:**
```bash
uv run duckdb "SELECT * FROM 'data/output/HiscoAIR_202601_main.parquet' LIMIT 5;"
```

## Customization

```bash
uv run rollup --yes --min-loss 0           # keep all rows
uv run rollup --yes --log-level INFO       # see factor-chain trace
uv run rollup --yes --no-audit             # skip debug parquets
```

Or set persistent values in `rollup.local.toml`: `[run].min_loss = 500`, `[logging].level = "INFO"`, etc.

## If something breaks

See [Troubleshooting](troubleshooting.md) for the 7 most common failure modes and fixes.
