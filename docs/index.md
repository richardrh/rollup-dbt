# Polars Rollup Pipeline

Turns vendor catastrophe-loss YLT parquets into Hisco loss parquets, optionally pushing them to SQL Server.

## Run it

1. **Clone the repo and install:**

   macOS/Linux:

   ```bash
   git clone <repo>
   cd rollup-dbt
   uv sync
   cp config.example.py config.py
   ```

   Windows PowerShell:

   ```powershell
   git clone <repo>
   Set-Location rollup-dbt
   uv sync
   Copy-Item config.example.py config.py
   ```

2. **Drop your data into `data/`:** see [Loading your data](load-data.md)

3. **Run:**
   ```bash
   uv run rollup --yes
   ```

4. **Output appears in `data/output/`:** parquets ready for Hisco ingestion or SQL push.

**First time?** Walk through [Quickstart](first-run.md) (5 minutes) — includes setup, a test run, and how to inspect output.

## When something breaks

[Troubleshooting](troubleshooting.md) — the seven most common failures and their fixes.

## Reference (for engineers)

- [File formats](file-formats.md) — every input file's columns + dtypes
- [Data requirements](data-requirements.md) — the contract for a real run
- [Architecture](architecture.md) — code organisation
- [Factor chain](factor-chain.md) — how to add a new factor
- [Calculations](calculations.md) — the math
- [Operating modes](operating-modes.md) — analysis-selection design options

## Build the docs locally

```bash
uv run zensical serve   # live-reload on http://localhost:8000
```
