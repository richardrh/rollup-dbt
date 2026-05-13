# Rollup Polars Pipeline Demo

Audience: product / underwriting / engineering stakeholders  
Duration: 10-15 minutes  
Goal: show that the pipeline is usable, explainable, tested, and January-aligned.

---

## 1. What this product does

**Turns vendor catastrophe YLTs into Hisco-ready rollup outputs.**

- Reads RiskLink and Verisk YLT parquet files
- Applies official rollup scope and factor chain
- Produces Hisco fanout parquets plus audit outputs
- Supports parquet-only operation and optional SQL Server push

Speaker note: “This replaces spreadsheet/manual rollup steps with a repeatable pipeline.”

---

## 2. Why this matters

Before:

- Manual joins and spreadsheet checks
- Hard to prove what factor changed a number
- Risk of drift from January reference logic

Now:

- One CLI command gives a checked plan and reproducible outputs
- Each factor is visible in audit files
- Deterministic tests prove key calculations

---

## 3. Demo flow

Run these from repo root:

```bash
uv run rollup --dry-run -y
uv run rollup --yes --min-loss 0
uv run pytest polars/tests/test_deterministic_e2e.py -q
```

Optional if Docker SQL Server is available:

```bash
uv run pytest polars/tests/test_sql_integration.py -q --run-integration
```

---

## 4. CLI plan: confidence before running

The dry-run shows:

- Seed files and row counts
- Vendor YLT files and schema checks
- Required EP-summary long CSV availability for default blending derivation
- Forecast factor dates and scoped coverage
- Output directory
- SQL configured or parquet-only mode

Speaker note: “Operators can see missing data before spending time on a run.”

---

## 5. Factor chain at a glance

MAIN output:

```text
raw loss
→ blending uplift
→ cap
→ local currency / FX
→ forecast
→ EUWS
→ fine-art gross
```

DIALSUP output:

```text
raw loss × forecast × EUWS × fine-art gross
```

Speaker note: “DIALSUP intentionally bypasses uplift, cap, and FX to match January intent.”

---

## 6. January alignment delivered

Aligned behaviours now include:

- Verisk STC filtering
- Return-period blending buckets
- DIALSUP January-style formula
- Fine-art AAL vs tail factor by rank bucket
- Sub-peril blending override with generic fallback
- EP ranking deterministic on tied losses

---

## 7. Deterministic proof test

The demo test creates fake files with easy numbers:

```text
Verisk AAL   = 1,000
RiskLink AAL = 500
50/50 blend  = 750
uplift       = 750 / 1,000 = 0.75
```

Then proves event outputs:

```text
100 → 75
200 → 150
300 → 225
```

Speaker note: “This is not a smoke test. It checks exact values through the CLI pipeline.”

---

## 8. Auditability

Audit outputs are written by default. The pipeline writes:

- `audit_wide.parquet` — one row per event with factors left-to-right
- `audit_long.parquet` — metric/value format for analysis
- `mts_tbl_ylt_combined_all_factors.parquet` — default combined audit output

Use `--no-audit` only when debug parquets are not needed.

Speaker note: “When a number looks wrong, we can inspect every factor that produced it.”

---

## 9. Quality gates currently passing

Latest validation on this branch:

```text
Default suite: 201 passed, 97 skipped
Fuzz suite:    90 passed
SQL suite:     7 passed
```

Skipped by default:

- fuzz/property tests require `--run-fuzz`
- SQL Server integration requires `--run-integration`

---

## 10. What is still explicitly not hidden

Known next hardening items:

- Make SQL push atomic with staging/swap
- Add more uniqueness guards on joins
- Replace silent fallbacks with warnings/errors where appropriate
- Decide when to delete legacy January/reference folders

Speaker note: “The product is demoable now, and the remaining risk list is known and trackable.”

---

## 11. Close

Takeaway:

**The rollup is now a repeatable, inspectable Polars product with January-aligned calculations and automated proof tests.**

Demo close command:

```bash
uv run pytest polars/ -q
```
