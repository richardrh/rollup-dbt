# Troubleshooting — common errors and fixes

Quick reference for the nine most common pipeline failures. Each lists the symptom, root cause, and the fix command or action.

---

## 1. `rollup --yes` aborts with "fix the failing checks above"

**Symptom:** Plan shows `✘` marks; pipeline refuses to run.

**Cause:** A seed or YLT input is missing or schema-drifted (column names or types don't match the expected schema).

**Fix:** Re-run `uv run rollup --dry-run` to see exactly what failed. The `✘` lines tell you which file and what's wrong (missing column, wrong type, etc.).

---

## 2. Plan shows `✘ <seed_name> | missing` or `✘ unexpected=[col]`

**Symptom:** Seed file is flagged as missing or has unexpected columns.

**Cause:** Seed file deleted, moved, or has a column rename since the last export from duckdb.

**Fix:** Check expected schema in `polars/rollup/schemas/frames.py` or in `data/seeds/README.md`. Restore the original column names, or re-export the seed from the duckdb source with the correct schema.

---

## 3. `rollup --yes` aborts but YLT directory has files

**Symptom:** YLT directory (`data/ylt/{verisk,risklink}/`) contains parquets, but the plan aborts before reading them.

**Cause:** YLT directory exists but glob doesn't match — filename pattern is wrong, or files are not parquet format.

**Fix:** Filenames must match the pattern exactly:
- Verisk: `air_ylt_*.parquet`
- RiskLink: `risklink_ylt_*.parquet`

Check your files in `data/ylt/verisk/` and `data/ylt/risklink/` against these patterns.

---

## 4. `error: ROLLUP_MSSQL_CONN_STR is not set`

**Symptom:** `rollup push-to-sql` or `rollup test-sql` fails immediately with connection string error.

**Cause:** No SQL Server connection configured.

**Fix:** Copy the example config and fill it in:
```bash
cp polars/config.example.py polars/config.py
# Edit config.py and set MSSQL_CONN_STR
```

Alternatively, set the environment variable directly:
```bash
export ROLLUP_MSSQL_CONN_STR="Driver=ODBC Driver 17 for SQL Server;Server=...;UID=...;PWD=..."
```

---

## 5. `rollup test-sql` fails with `OperationalError`

**Symptom:** Connection attempt fails with `OperationalError` (e.g. timeout, host not found).

**Cause:** Server unreachable, wrong server name, network/DNS issue, or firewall blocking.

**Fix:** 
1. Verify the server name in `polars/config.py` or your connection string is correct.
2. Try connecting with another tool (SQL Server Management Studio, `sqlcmd`, or `mssql-cli`) to confirm the server itself is reachable.
3. Check network/firewall settings if other tools can't reach it either.

---

## 6. `rollup test-sql` fails with `InterfaceError` mentioning driver

**Symptom:** Connection fails with `InterfaceError` complaining about ODBC driver.

**Cause:** ODBC driver not installed on this machine.

**Fix:** Install "ODBC Driver 17 for SQL Server" from Microsoft:
- https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server

Then verify your connection string in `polars/config.py` uses the correct driver name:
```python
driver=ODBC+Driver+17+for+SQL+Server
```

---

## 7. `MissingFxRateError: currency 'EUR' not in fx_rates.csv`

**Symptom:** Pipeline aborts immediately at startup with `MissingFxRateError` mentioning a currency code.

**Cause:** The `fx_rates.csv` seed is missing a required currency. The pipeline derives currency from the LOB name (e.g. `' EU '` → EUR, `' UK '` → GBP). If a peril category contains a currency code not covered by `fx_rates.csv`, the pipeline fails before building the factor chain — **fast** (≈1 second).

**Fix:** Add the missing currency row to `data/seeds/fx_rates.csv`:

```csv
currency_code,target_currency,rate_date,rate
EUR,GBP,2026-01-01,0.88
GBP,GBP,2026-01-01,1.0
```

Both `GBP→GBP` (rate = 1.0) and `EUR→GBP` rows are required at minimum. See [`data/seeds/README.md`](../data/seeds/README.md) for the full seed spec.

---

## 8. `SchemaError: unexpected column 'fake_col' in sql_push.HiscoAIR_202601_main`

**Symptom:** `rollup push-to-sql` fails during the push with a `SchemaError` naming an unexpected column in one of the parquets.

**Cause:** A rogue column made it into the Hisco fanout parquets (likely a temporary debugging column left in the code). The pipeline validates the schema **before** touching SQL Server — so this is caught cleanly at the parquet level, not buried in a SQL error.

**Fix:** 
1. Check the named column in your output parquet: is it something you added for debugging? If so, remove it from the code and re-run the pipeline.
2. If it's an expected column that's been recently added, check `polars/rollup/schemas/frames.py::HISCO_FANOUT` schema — it may need updating.
3. Delete `data/output/*.parquet` and re-run: `uv run rollup --yes`

This is a safety gate — unexpected columns in SQL pushes can corrupt the downstream schema contract, so they fail fast.

---

## 9. Output parquets exist but contain zero rows

**Symptom:** Hisco parquets are written to `data/output/` but every row has `ModelGrossLoss = 0`.

**Cause:** A join earlier in the factor chain failed silently (returned 0 rows). Or: `valid_analyses.csv` is empty / does not include the numeric vendor analysis IDs selected for the run.

**Fix:**
1. Verify `data/seeds/business/valid_analyses.csv` lists the intended `(vendor, analysis_id)` rows.
2. Cross-check that `analyses.csv` includes the same numeric IDs and maps them to perils.
3. For Verisk, ensure raw `Analysis` / EP `analysis` labels match `analyses.modelled_label`; the bundled `90000x` IDs are placeholders and must be replaced before production.
4. Run with default audit enabled: `uv run rollup --yes`. Inspect `data/output/debug/audit_wide.parquet` — the leftmost zero column tells you which join failed.

---
