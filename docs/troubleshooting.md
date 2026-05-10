# Troubleshooting — common errors and fixes

Quick reference for the six most common pipeline failures. Each lists the symptom, root cause, and the fix command or action.

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

## 7. Output parquets exist but contain zero rows

**Symptom:** Hisco parquets are written to `data/output/` but every row has `ModelGrossLoss = 0`.

**Cause:** A join earlier in the factor chain failed silently (returned 0 rows). Or: `rollup_scope.csv` is empty or no rows have `in_rollup=True`, so nothing passed the scope filter.

**Fix:**
1. Verify `data/seeds/business/rollup_scope.csv` has rows with `in_rollup=true`.
2. Cross-check that the `analyses.csv` `analysis_id` values include the `anlsid`s present in your YLT files.
3. Run with `--dump-interim` to write audit parquets: `uv run rollup --yes --dump-interim`. Inspect `data/output/debug/audit_wide.parquet` — the leftmost zero column tells you which join failed.

---
