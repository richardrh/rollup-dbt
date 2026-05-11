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

All seed CSVs already exist in `data/seeds/` as templates.

## Step 1 — Populate the 4 blocker seeds

Copy these CSVs into `data/seeds/vor/`:

1. **`perils.csv`** — peril dimension (peril_id, name, region, peril_family)
2. **`analyses.csv`** — maps (vendor, analysis_id) to peril_id
3. **`rollup_scope.csv`** — which (lob, vendor, analysis) pairs are in scope
4. **`blending_weights.csv`** — per-peril RiskLink / Verisk proportions

If you don't have these, see [`polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md).

Other seeds (`lobs.csv`, `forecast_factors.csv`, `euws_*`, `fx_rates.csv`, `air_events.csv`, `fineart_adjustments.csv`) are dbt-owned or stubs. For details, see [`data/seeds/README.md`](../data/seeds/README.md).

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
| `Analysis` | String | e.g. `EU_WS`, joined to `analyses.csv` |
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

Copy parquets with filename pattern `risklink_ylt_*.parquet` (lowercase columns).

**Required schema:**

| Column | Type | Notes |
|--------|------|-------|
| `SimulationSetId` | Int64 | |
| `yearid` | Int64 | simulation year (lowercase) |
| `eventid` | Int64 | event id (lowercase) |
| `date` | String | event date |
| `p_value` | Float64 | |
| `anlsid` | Int64 | analysis id — joined to `analyses.csv` |
| `name` | String | |
| `description` | String | |
| `rate` | Float64 | |
| `meanloss` | Float64 | |
| `stddev` | Float64 | |
| `expvalue` | Float64 | |
| `loss` | Float64 | **loss column used by pipeline** |

**Need RiskLink?** Yes if modeling flood perils (`peril_family = 'FL'`); optional for wind/EQ.

## Step 4 — Deriving blending weights from EP summaries

EP summaries (long-format CSVs) are used to derive per-peril blending proportions
and the base model, stored in `data/seeds/vor/blending_weights.csv`.

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

**Step 4c — Regenerate blending_weights.csv:**

```bash
uv run rollup derive-blending
```

Reads `*.long.csv` files under `data/ep_summaries/{vendor}/`. For each target
bucket (`0`=AAL, `200`=1-in-200 OEP, `1000`=1-in-1000 OEP), computes:

```python
rl_prop = rl_aal / (rl_aal + vk_aal)   # when total > 0; else 0.5
vk_prop = 1 - rl_prop
```

The seed is written to `data/seeds/vor/blending_weights.csv` with schema:

| column | meaning |
|---|---|
| `peril_id` | FK into `perils.csv` |
| `return_period` | Weight bucket: `0`=AAL, `200`=1-in-200, `1000`=1-in-1000 |
| `vendor` | `"verisk"` or `"risklink"` |
| `base_model` | The model to use as denominator: `"risklink"` for FL perils, `"verisk"` otherwise |
| `weight` | Blending proportion for this vendor |

At runtime each YLT event is ranked largest-to-smallest within
`(vendor, lob_id, peril_id)`, converted to `rp = n_sim / rank`, bucketed to
`0`, `200`, or `1000`, then joined to the matching blending weight.

Re-run `derive-blending` whenever the EP summaries are refreshed.

## Step 5 — Full verification

```bash
uv run rollup --dry-run
```

All sections should show ✓. If any show ✘, see [Troubleshooting](troubleshooting.md).

## Step 6 — Run the pipeline

```bash
uv run rollup --yes
```

Output: 9 parquets in `data/output/` (~15–40 seconds depending on data size).

**Inspect output:**
```bash
uv run duckdb "SELECT * FROM 'data/output/HiscoAIR_202601_main.parquet' LIMIT 5;"
```

## Customization

```bash
uv run rollup --yes --min-loss 0           # keep all rows
uv run rollup --yes --log-level INFO       # see factor-chain trace
uv run rollup --yes --dump-interim         # write debug parquets
```

Or set in `config.py`: `MIN_LOSS = 500`, `LOG = "INFO"`, etc.

## If something breaks

See [Troubleshooting](troubleshooting.md) for the 7 most common failure modes and fixes.
