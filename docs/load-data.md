# Loading your data into the pipeline

This guide walks you through getting your raw vendor data into the pipeline, step by step. Follow the steps in order. Each step ends with a verify command — don't move on until verify passes.

**Target audience:** a developer with the repo cloned, vendor data files from the modelling team, and a terminal ready. Assumes you've completed the setup in [Getting started — your first run](first-run.md).

---

## Step 0 — Create the directory layout

The pipeline expects data in a specific directory structure. If you haven't created it yet, run:

```bash
mkdir -p data/{seeds/{business,vor,adjustments,validation},ylt/{verisk,risklink},ep_summaries/{verisk,risklink},output}
```

Verify the layout:

```bash
find data -maxdepth 2 -type d | sort
```

You should see:

```
data
data/output
data/ep_summaries
data/ep_summaries/risklink
data/ep_summaries/verisk
data/seeds
data/seeds/adjustments
data/seeds/business
data/seeds/validation
data/seeds/vor
data/ylt
data/ylt/risklink
data/ylt/verisk
```

---

## Step 1 — Populate the seed CSVs

Seeds are reference data CSVs that live in `data/seeds/`. There are 11 seeds total, split across four sub-folders. All must exist (even if some are header-only stubs), or the pipeline will refuse to run.

### What are seeds?

Seeds are small lookup tables — analyses, perils, FX rates, forecast factors, etc. The pipeline uses them to standardize and adjust the raw vendor data. See [Data requirements](data-requirements.md) for the complete schema of each seed.

### The 4 blocker seeds (you must populate these)

These seeds are required, and the pipeline cannot run without them populated. They describe the analysis catalogue and rollup scope:

1. **`data/seeds/vor/perils.csv`** — the peril dimension (peril_id, name, region, peril_family)
2. **`data/seeds/vor/analyses.csv`** — maps (vendor, analysis_id) to peril_id
3. **`data/seeds/vor/rollup_scope.csv`** — which (lob, vendor, analysis) combinations are in scope
4. **`data/seeds/vor/blending_weights.csv`** — per-peril blend weights between RiskLink and Verisk

If you have these from the modelling team, copy them into the `data/seeds/vor/` folder now. If you don't, see [RH-TODO-DATA.md](../polars/RH-TODO-DATA.md) for guidance on producing them.

### The other 7 seeds (stubs or dbt-owned)

The remaining seeds are either:
- **dbt-owned** (`lobs.csv`, `forecast_factors.csv`, `euws_rate_factors.csv`, `euws_rank_overrides.csv`) — already in the repo with real data, no action needed.
- **Stubs to refresh** (`fx_rates.csv`, `air_events.csv`, `fineart_adjustments.csv`) — exist as header-only files. Replace with real data before a production run.

For details on each, see [`data/seeds/README.md`](../data/seeds/README.md).

### Verify the seeds are valid

Run the dry-run plan:

```bash
uv run rollup --dry-run
```

Look at the `[seeds]` section in the output. You should see:

```
▣ seeds
  ✓ perils
  ✓ analyses
  ✓ rollup_scope
  ✓ blending_weights
  ✓ lobs
  ✓ forecast_factors
  ✓ fx_rates
  ✓ euws_rate_factors
  ✓ euws_rank_overrides
  ✓ air_events
  ✓ fineart_adjustments
```

All 11 should show ✓. If any show ✘, the message will tell you which one and why (e.g., "missing column: peril_family"). Check [Data requirements](data-requirements.md) for the schema and fix the CSV.

---

## Step 2 — Drop in the Verisk YLT parquets

YLTs (year loss tables) are the raw loss simulation outputs from the vendor models. They arrive as parquet files and contain one row per simulation year × event.

### Where to put them

Copy your Verisk parquets into `data/ylt/verisk/`. The filename must start with `air_ylt_`:

```bash
# Example: if you have three Verisk chunks
cp air_ylt_chunk1.parquet data/ylt/verisk/
cp air_ylt_chunk2.parquet data/ylt/verisk/
cp air_ylt_chunk3.parquet data/ylt/verisk/
```

### Expected schema

The parquet must have exactly these 12 columns with these types. The case and spelling must match exactly:

| Column | Type | Notes |
|--------|------|-------|
| `Analysis` | String | e.g. `EU_WS`, `UK_FL` — joined to `analyses.csv` |
| `ExposureAttribute` | String | the LOB string (e.g. `HIC_HH_UK`) |
| `CatalogTypeCode` | String | filter — only `STC` rows are kept |
| `EventID` | Int64 | event identifier |
| `ModelCode` | Int64 | model code |
| `YearID` | Int64 | simulation year |
| `PerilSetCode` | Int64 | peril set code |
| `GroundUpLoss` | Float64 | (not used by pipeline) |
| `GrossLoss` | Float64 | (not used by pipeline) |
| `NetOfPreCatLoss` | Float64 | **this is the loss column the pipeline uses** |
| `filename` | String | source filename (passthrough) |

### Verify the schema

Check the schema of your parquets:

```bash
uv run python -c "import polars as pl; print(pl.scan_parquet('data/ylt/verisk/air_ylt_*.parquet').collect_schema())"
```

Compare the output against the table above. If column names or types don't match, the parquets need to be re-exported from the vendor system or converted before use.

### Verify the data was found

Run the dry-run plan:

```bash
uv run rollup --dry-run
```

Look for the `[ylt verisk]` section. You should see something like:

```
▶ ylt verisk
  ✓ air_ylt_chunk1.parquet (1.2M rows, 45 MB)
  ✓ air_ylt_chunk2.parquet (0.9M rows, 35 MB)
  ✓ air_ylt_chunk3.parquet (1.1M rows, 42 MB)
```

If you see ✘ or the section is missing, it means:
- No files match `data/ylt/verisk/air_ylt_*.parquet`. Check the filename pattern.
- The schema is wrong. See "Verify the schema" above.

---

## Step 3 — Drop in the RiskLink YLT parquets

RiskLink (RMS) parquets follow the same pattern as Verisk, but go in a different folder and have a different schema.

### Where to put them

Copy your RiskLink parquets into `data/ylt/risklink/`. The filename must start with `risklink_ylt_`:

```bash
# Example
cp risklink_ylt_chunk1.parquet data/ylt/risklink/
cp risklink_ylt_chunk2.parquet data/ylt/risklink/
```

### Expected schema

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
| `loss` | Float64 | **this is the loss column the pipeline uses** |

Note: columns are lowercase, unlike Verisk. This is the wire format from RMS.

### Do I need RiskLink data?

**Short answer:** You need it for flood perils. You might not need it for wind/EQ/other perils.

The pipeline uses RiskLink as the base model for all flood perils (`peril_family = 'FL'`), because RMS is the certified flood modeller. For wind, earthquake, and other perils, Verisk is the base model.

So:
- **If you're modelling flood** (Europe Flood, UK Flood, regional flood sub-perils), you need RiskLink YLTs.
- **If you're modelling wind/EQ only**, RiskLink YLTs are optional but recommended for blending.

For details, see [Data requirements](data-requirements.md).

### Verify the schema

```bash
uv run python -c "import polars as pl; print(pl.scan_parquet('data/ylt/risklink/risklink_ylt_*.parquet').collect_schema())"
```

### Verify the data was found

```bash
uv run rollup --dry-run
```

Look for `[ylt risklink]`. If you see ✘ and RiskLink is required for your perils, check:
1. Is the filename pattern `risklink_ylt_*.parquet`?
2. Is the schema correct?

If you don't have RiskLink data and it's optional for your scope, you can skip this step. The pipeline will work without it (though RMS analysis outputs will be empty).

---

## Step 4 — Drop in the EP summaries (optional)

EP summaries are vendor-provided spreadsheets with Exceedance Probability (EP) curves. They are used to:
1. Derive blending weights between the two vendors (via `rollup derive-blending`)
2. Validate the fit of the output loss curves

### Do I need EP summaries?

No, they are not required to run the pipeline. But if you want to recompute blending weights from the latest EP curves, you'll need them. See [RH-TODO-DATA.md](../polars/RH-TODO-DATA.md#ep-summaries--long-format-csvs) for how to collect them.

### Format: long-format CSVs

EP summaries arrive as Excel files (`.xlsx`) from the vendors. The pipeline reads them as long-format CSVs (one row per LOB × analysis × return period). Conversion is automatic.

### RiskLink EP summaries

Copy your RMS xlsx files here:

```bash
cp rms_analysis_list.xlsx data/ep_summaries/risklink/
```

Then convert to long CSV:

```bash
uv run rollup ep-summary-to-csv
```

This reads the `.xlsx`, parses the multi-row header and wide return-period columns, and writes a sibling `.long.csv` file. The long format is:

```
id,rp,ep_type,lob,region_peril,gl
1,0,AAL,HIC_HH_UK,GB FL HD,1806464.0
1,100,OEP,HIC_HH_UK,GB FL HD,19365339.0
1,1000,OEP,HIC_HH_UK,GB FL HD,62873626.0
```

### Verisk EP summaries

Verisk doesn't have an automated converter yet. You need to produce the long CSV directly:

```
rp,ep_type,analysis,lob,gl
0,AAL,EU_WS,HIC_HH_UK,5421000.0
100,OEP,EU_WS,HIC_HH_UK,18200000.0
0,AAL,GB_FL,HIC_HH_UK,1650000.0
100,OEP,GB_FL,HIC_HH_UK,17800000.0
```

Copy the CSVs to `data/ep_summaries/verisk/`. They must end in `.long.csv`.

### Regenerate blending weights (optional)

If you've added or refreshed EP summaries, recompute the blending weights:

```bash
uv run rollup derive-blending
```

This computes per-peril AAL totals from the EP summaries and writes `data/seeds/vor/blending_weights.csv` with proportions:

```
rl_proportion = rl_aal / (rl_aal + vk_aal)
vk_proportion = 1 - rl_proportion
```

Then re-run the dry-run to confirm the new weights are loaded:

```bash
uv run rollup --dry-run
```

---

## Step 5 — Full verification

Run the complete dry-run plan:

```bash
uv run rollup --dry-run
```

This will show you the plan for the entire pipeline without executing anything. You should see:

```
▣ seeds
  ✓ perils
  ✓ analyses
  ✓ rollup_scope
  ✓ blending_weights
  ... (other seeds)

▶ ylt verisk
  ✓ air_ylt_chunk1.parquet (1.2M rows)
  ✓ air_ylt_chunk2.parquet (0.9M rows)
  ... 

▶ ylt risklink
  ✓ risklink_ylt_chunk1.parquet (1.5M rows)
  ✓ risklink_ylt_chunk2.parquet (2.1M rows)
  ...

▣ hisco (main)
  ✓ HiscoAIR_202601_main
  ✓ HiscoRMS_202601_main
  ... (6 total)

▣ hisco (dialsup)
  ✓ HiscoAIR_202601_dialsup
  ✓ HiscoRMS_202601_dialsup

▣ hisco (combined long)
  ✓ HiscoAll_202601_long
```

**If all sections show ✓ green:** you're ready to run. Go to Step 6.

**If any section shows ✘ red:** the message will tell you the problem. Common issues:
- **"missing file"** — check the filename pattern and folder path
- **"schema mismatch"** — check column names and types against [Data requirements](data-requirements.md)
- **"blocker seed not populated"** — seed is empty or missing required columns; see Step 1 above

---

## Step 6 — Run the full pipeline

Once all sections show ✓, run the pipeline for real:

```bash
uv run rollup -y
```

The `-y` flag skips the interactive confirmation prompt. The pipeline will:

1. **Validate** — re-check all files (≈1 second)
2. **Load** — read YLT parquets and seed CSVs (≈2–5 seconds)
3. **Factor chain** — apply adjustments: currency conversion, forecast scaling, EUWS, fine-art, uplift (≈10–30 seconds depending on data size)
4. **Output write** — write 9 parquets to `data/output/` (≈5 seconds)

Total time: typically 15–40 seconds depending on YLT size.

You'll see progress messages like:

```
validating schema...
loading seeds...
  perils (11 rows)
  analyses (42 rows)
  ...
loading ylt verisk (3.2M rows)
loading ylt risklink (3.6M rows)
building factor chain...
  attach_rollup_scope
  attach_fx
  attach_forecast
  ... (6 factors)
writing hisco parquets...
  HiscoAIR_202601_main (1.2M rows) ✓
  HiscoRMS_202601_main (2.1M rows) ✓
  ... (9 total)
done: 9 files, 12,456,789 rows total
```

### Verify the output

Check that 9 parquets were written:

```bash
ls -lh data/output/Hisco*.parquet
```

You should see 9 files:

```
HiscoAIR_202601_main.parquet
HiscoRMS_202601_main.parquet
HiscoAIR_202607_main.parquet
HiscoRMS_202607_main.parquet
HiscoAIR_202701_main.parquet
HiscoRMS_202701_main.parquet
HiscoAIR_202601_dialsup.parquet
HiscoRMS_202601_dialsup.parquet
HiscoAll_202601_long.parquet
```

Inspect one to see its shape:

```bash
python3 << 'EOF'
import polars as pl
df = pl.read_parquet("data/output/HiscoAIR_202601_main.parquet")
print(f"Shape: {df.shape}")
print(f"Columns: {df.columns}")
print(f"First row:\n{df.head(1)}")
EOF
```

---

## Troubleshooting

### The dry-run shows ✘ for a seed

**Cause:** the seed CSV is missing, empty, or has the wrong columns.

**Fix:**
1. Check the path. Seeds live in `data/seeds/` sub-folders, not in root.
2. Run `uv run rollup --dry-run` to see the exact error message.
3. Compare your CSV columns against [Data requirements](data-requirements.md).
4. Ensure all 11 seeds exist, even if some are stub-only (header + 0 data rows).

For blocker seeds (`perils`, `analyses`, `rollup_scope`, `blending_weights`), see [RH-TODO-DATA.md](../polars/RH-TODO-DATA.md#blocker-seeds--pipeline-refuses-to-run-without-these) for the export procedure.

### The dry-run shows ✘ for Verisk or RiskLink YLTs

**Cause:** no parquets found, or schema mismatch.

**Fix:**
1. Check the folder path. Verisk → `data/ylt/verisk/`, RiskLink → `data/ylt/risklink/`.
2. Check the filename pattern. Verisk must be `air_ylt_*.parquet`, RiskLink must be `risklink_ylt_*.parquet`.
3. Check the schema:
   ```bash
   uv run python -c "import polars as pl; print(pl.scan_parquet('data/ylt/verisk/air_ylt_*.parquet').collect_schema())"
   ```
4. Compare against the table in Step 2 or Step 3. Column names and types must match exactly.

### The pipeline runs but produces 0 rows

**Cause:** the `rollup_scope.csv` is empty or all rows have `in_rollup = false`, so every row is filtered out.

**Fix:** check your `data/seeds/vor/rollup_scope.csv`. It should have rows like:

```
modelled_lob,vendor,analysis_id,in_rollup
HIC_HH_UK,verisk,EU_WS,true
HIC_HH_UK,verisk,GB_FL,true
HIC_HH_UK,risklink,GB_FL,true
```

At least one row per (vendor, analysis) pair must have `in_rollup = true`. If all are `false`, the pipeline filters out everything.

### "I'm getting an error message — where do I look?"

1. **Read the error message.** Most are precise — e.g., "SchemaError: unexpected column 'fake_col' in HiscoAIR_202601_main".
2. **Run `uv run rollup --dry-run`** to see if it's a missing/bad seed or YLT.
3. **Check [Troubleshooting](troubleshooting.md)** for the 7 most common failure modes.
4. **Check [Data requirements](data-requirements.md)** for the canonical schema of seeds and YLTs.

---

## Next steps

Once the pipeline runs successfully:

- **View the output:** See [Getting started — your first run](first-run.md#inspect-the-output) to inspect the 9 parquets.
- **Push to SQL Server (optional):** See [Getting started](first-run.md#optional-configure-sql-server-push-if-applicable) to configure and push to a SQL database.
- **Understand the calculations:** See [Calculations](calculations.md) and [Factor chain](factor-chain.md) for the math behind each stage.
- **Customize the run:** Use command-line flags to disable loss filters, change log level, or export audit parquets:
  ```bash
  uv run rollup --yes --min-loss 0           # keep all rows (no loss cutoff)
  uv run rollup --yes --log-level INFO       # see factor-chain trace
  uv run rollup --yes --dump-interim         # write debug parquets to data/output/debug/
  ```

---

## Questions?

Refer to:
- **Setup / first-run issues** → [Getting started](first-run.md)
- **Detailed schema reference** → [Data requirements](data-requirements.md)
- **Error messages / failure modes** → [Troubleshooting](troubleshooting.md)
- **How the pipeline works** → [Architecture](architecture.md), [Factor chain](factor-chain.md), [Calculations](calculations.md)
