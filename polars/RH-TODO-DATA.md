# RH — data export TODO

The pipeline is ready. This is the punch list for getting real data into
the seeds + YLT folders so a production run actually produces non-zero
Hisco losses. Tick each item as you go.

The detailed schemas + copy-pasteable duckdb `COPY` SQL for every item
below live in [`../docs/data-requirements.md`](../docs/data-requirements.md).
This file is the checklist; data-requirements.md is the reference.

The pre-flight reporter (`uv run python -m rollup.pipeline --dry-run`) is
the source of truth for "am I ready" — it walks the seed list, validates
each schema, and **blocks the run** if any of the four blocker seeds is
empty. Run it after every export to verify.

---

## 0. One-time setup

- [ ] Open a duckdb shell on the january database that has
      `loader.main.dim_region_perils`, `loader.main.dim_rl_analysis`,
      `reference.lobs`, `reference.air_events`,
      `reference.fineart_gross_to_net_adjustment2`,
      `reference.blending_factors`. (The schema dump in
      `jan-rollup/duckdb_schema/table_definitions.csv` lists every table.)

- [ ] Confirm `polars/seeds/{lobs,euws_rate_factors,euws_rank_overrides,
      forecast_factors,fx_rates}.csv` are populated (these come from dbt
      and shipped already). `--dry-run` will mark them ✓ if so.

---

## 1. BLOCKERS — the four splits of `dim_region_perils`

Without these the pipeline aborts at preflight (or, if forced, drops every
YLT row through `apply_rollup_scope`). Export each as a CSV and drop into
`polars/seeds/`. Schema and SQL in
[`../docs/data-requirements.md` §B.1–§B.4](../docs/data-requirements.md).

### 1a. `perils.csv` — peril dimension

- [ ] Export with the `COPY (SELECT DISTINCT ...) FROM loader.main.dim_region_perils`
      query in §B.1 of data-requirements.md.
- [ ] Verify `peril_family` column contains exactly `"FL"` for every flood
      peril (case-sensitive — `attach_uplift` uses
      `config.FLOOD_FAMILY = "FL"` to force `base_model='risklink'` for
      these rows). If your source data has `"Flood"` / `"Fl"` / `"flood"`
      you'll need a CASE in the export to normalise.
- [ ] Confirm `peril_id` matches the `blending_factor_region_peril_id`
      values that the analyses + blending_weights tables will reference.
- [ ] Run `--dry-run` — `perils` should report `✓ schema OK NN rows`.

### 1b. `analyses.csv` — vendor analysis → peril (+ lob for RiskLink)

- [ ] Run the verisk-rows `COPY` query (§B.2) → produces
      `/tmp/analyses_verisk.csv`.
- [ ] Run the risklink-rows `COPY` query (§B.2) → produces
      `/tmp/analyses_risklink.csv`.
- [ ] Concatenate: `cat analyses_verisk.csv > polars/seeds/analyses.csv && tail -n +2 analyses_risklink.csv >> polars/seeds/analyses.csv` —
      OR rewrite as one `UNION ALL` query and dump direct.
- [ ] Sanity: `lob_id` column should be NULL for every verisk row,
      populated for every risklink row.
- [ ] `--dry-run` reports `✓` for `analyses`.

### 1c. `rollup_scope.csv` — official-rollup gate

- [ ] Run the `COPY (SELECT ... CROSS JOIN ... CASE lob_type ...)` query
      in §B.3. Replaces january's `applies_to_{mga,prop,fa}` flag fan-out.
- [ ] Sanity: `analysis_id` column holds the **modelled label** (e.g.
      `"EU_WS"`, `"UK_WSSS_GCAdj"`), NOT the raw RiskLink integer
      `rl_analysis_id`. The pipeline joins this against the YLT's
      `MODELLED_REGION_PERIL` column after staging.
- [ ] Spot-check: for at least one (lob, vendor, analysis) triple you
      know is in scope, confirm `in_rollup` is `true`. If `in_rollup`
      is `false` for everything, the pipeline drops every row.
- [ ] `--dry-run` reports `✓` for `rollup_scope`.

### 1d. `blending_weights.csv` — long-format blend weights

- [ ] Run the `COPY (SELECT ... 'verisk' ... UNION ALL ... 'risklink' ...)`
      long-pivot query in §B.4. Replaces wide `air_blend` / `rms_blend`
      / `kat_risk_blend` columns from `blending_factors`.
- [ ] Optionally fill `peril_name` and `description` columns with
      human-readable text (the pipeline does NOT join on them; they exist
      so you can read the CSV and understand what each row is).
      Easiest: in the SQL, join `dim_region_perils` and project
      `rollup_region_peril AS peril_name`, plus an empty `description`
      string.
- [ ] `--dry-run` reports `✓` for `blending_weights`.

---

## 2. RECOMMENDED — silences a runtime warning

### 2a. `air_events.csv` — Verisk event catalogue

- [ ] Export with the `COPY (SELECT ... FROM reference.air_events)` query
      in §B.5.
- [ ] Without this, every run logs
      `WARNING event-id orphans for vendor=verisk: N/N YLT rows have no
      match in air_events`. Pipeline still runs, just noisy.
- [ ] Future use: when `ModelEventDay` is wired up in the AIR fan-out
      (currently hardcoded to 0 in `fanout_hisco`), this same table
      provides `Day` per event.

---

## 3. OPTIONAL — improves accuracy if you have fine-art LOBs

### 3a. `fineart_adjustments.csv` — fine-art gross-to-net adjustment

- [ ] Export with the `COPY` query in §B.6.
- [ ] Without this, `attach_fagross` returns `fa_gross_aal_factor = 1.0`
      for every row (multiplicative pass-through), so fine-art LOBs flow
      through without the gross-to-net adjustment.
- [ ] `--dry-run` will report `(stub)` for `fineart_adjustments` if
      empty — that's fine, it's not a blocker.

---

## 4. REPLACE the placeholder FX snapshot

- [ ] `polars/seeds/fx_rates.csv` currently has 6 hand-crafted rows
      (GBP→GBP=1.0, EUR→GBP=0.88, USD→GBP=0.80). Replace with real rates
      from your FX source for the snapshot date.
- [ ] **Every currency code that can fall out of `attach_currency`'s
      derivation rule (`UK→GBP`, `EU→EUR`, fallback `GBP`) MUST have a
      row with `target_currency='GBP'`** — otherwise the pipeline aborts
      with `MissingFxRateError` rather than silently using rate 1.0.
- [ ] If you extend the derivation rule (e.g. add `US→USD`), add the new
      member to `CurrencyCode` in `polars/rollup/config.py` AND a
      matching row in `fx_rates.csv`.

---

## 5. PLACE the YLT parquets

- [ ] Verisk YLT chunks → `data/ylt/verisk/air_ylt_*.parquet`. The glob
      reads all chunks as one lazy table, so multiple files are fine
      (e.g. `air_ylt_c1.parquet` + `air_ylt_c2.parquet`).
- [ ] RiskLink YLT chunks → `data/ylt/risklink/risklink_ylt_*.parquet`.
- [ ] Override paths via env vars if needed:
      `ROLLUP_YLT_VERISK_DIR`, `ROLLUP_YLT_RISKLINK_DIR` (set to absolute
      paths).
- [ ] Confirm wire schema matches §A of data-requirements.md (CamelCase
      columns for AIR, lowercase for RiskLink). If your parquets have
      different column names, rename at export rather than touching the
      pipeline.

---

## 6. SMOKE TEST — run the pipeline

- [ ] `uv run python -m rollup.pipeline --dry-run`
      → expect `Seeds: 11/11 valid`, `YLTs: 2/2 vendors have data`.
      If anything is `✘`, fix it before going further.

- [ ] `uv run python -m rollup.pipeline --yes`
      → 12 Hisco parquets written under `data/output/`.

- [ ] Verify outputs are not all zero:
      ```python
      import polars as pl
      df = pl.read_parquet("data/output/HiscoAIR_202601_main.parquet")
      assert df.filter(pl.col("ModelGrossLoss") > 0).height > 0
      ```

- [ ] If `ModelGrossLoss = 0` everywhere: re-run with `--dump-interim` and
      open `data/output/debug/audit_wide.parquet`. The leftmost zero
      column tells you which join silently dropped data. The most common
      cause is a mismatch between `analyses.modelled_label` and
      `rollup_scope.analysis_id`.

---

## 7. POST-DATA cleanup (defer until after you have real numbers)

These are "nice to have" once the main run works:

- [ ] Wire `ModelEventDay` properly in `fanout_hisco` (currently 0).
      Needs a left-join on `air_events` for verisk variants and a
      separate flood-events seed for the risklink-flood variants. Track
      in a separate ticket once `air_events.csv` is populated.
- [ ] Apply `fa_gross_tail_factor` for high-RP rows (currently
      audit-only). Needs the rp-bucket logic from january's
      `attach_fagross` view; track separately.
- [ ] Reproduce `verify.*` invariants from january as pytest assertions
      (see `docs/calculations.md` §8).

---

## Quick failure-mode reference

The full table is in §G of `data-requirements.md`. Skim it BEFORE you ask
"why is my run zero". Most common cases:

| symptom                                              | most likely cause                                                          |
| ---------------------------------------------------- | -------------------------------------------------------------------------- |
| `seed 'X' missing at /…/X.csv`                       | seed file isn't where the loader expected — check `ROLLUP_SEEDS_DIR`       |
| `[seed.X] missing columns: [...]`                    | seed CSV header drifted — rename headers to match `rollup/schemas/columns.py` |
| `MissingFxRateError: ... ['EUR']`                    | add the missing currency row to `fx_rates.csv`                             |
| `event-id orphans for vendor=verisk: N/N YLT rows`   | populate `air_events.csv` (item 2a above)                                  |
| Zero rows across all variants                        | `rollup_scope.csv` is empty or every row has `in_rollup=false`             |
| `f_{tag}` column is 1.0 for every row of an LOB      | `office` string mismatch between `lobs.csv` and `forecast_factors.csv`     |
| Verisk EU-flood rows in AIR fanout (should be RMS)   | `peril_family` value isn't exactly `"FL"` for the flood peril in `perils.csv` |
