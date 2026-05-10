# Loading your data

Step-by-step walkthrough. Follow in order; verify at each step before moving on.

## Step 0 — Create the directory layout

```bash
mkdir -p data/{ylt/{verisk,risklink},ep_summaries/{verisk,risklink},output}
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

## Step 4 — EP summaries (optional)

For blending weight recomputation only. Convert Excel to long CSV:

```bash
cp rms_analysis_list.xlsx data/ep_summaries/risklink/
uv run rollup ep-summary-to-csv
```

For Verisk, produce long CSVs manually with columns: `rp`, `ep_type`, `analysis`, `lob`, `gl`. Copy to `data/ep_summaries/verisk/` (must end in `.long.csv`).

Regenerate weights:
```bash
uv run rollup derive-blending
uv run rollup --dry-run
```

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
python3 << 'EOF'
import polars as pl
df = pl.read_parquet("data/output/HiscoAIR_202601_main.parquet")
print(f"Shape: {df.shape}, Columns: {df.columns}")
EOF
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
