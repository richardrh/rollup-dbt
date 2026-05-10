# Getting started — your first run

This is the simplest possible walkthrough. By the end of this section, you'll have run the pipeline on test data and opened the docs site.

**Target audience:** a junior analyst who's never used this tool before. No prior knowledge of catastrophe modeling, vendor systems, or the broader Hisco infrastructure assumed.

---

## What does this tool do? (30 seconds)

The rollup pipeline takes annual catastrophe loss simulation data from two insurance modeling vendors (RiskLink and Verisk) and combines them into one standard format that downstream Hisco systems consume.

The input is raw parquet files from the vendors (called YLTs — your loss tables). The output is refined parquet files with loss adjustments applied (currency conversion, forecast scaling, fine-art corrections, and more).

Optionally, it can push those outputs to SQL Server for analysis teams to query.

---

## What you need before starting

- **Python 3.13 or later** — check with `python --version`
- **`uv` (universal Python package manager)** — check with `uv --version`. If not installed: https://docs.astral.sh/uv/getting-started/installation/
- **The repo cloned** — you're reading this, so ✓
- **(Optional) SQL Server access** — only needed if you want to push outputs to SQL Server. Install "ODBC Driver 17 for SQL Server" from Microsoft if you plan to do this.

---

## Setup (5 minutes)

From the repo root (`rollup-dbt/`), run these commands once:

```bash
# Install dependencies
uv sync

# Copy the configuration template
cp config.example.py config.py

# (Optional) Set up tab completion for faster typing
eval "$(register-python-argcomplete rollup)"

# To make tab completion persist across shell sessions, add this line to your ~/.bashrc or ~/.zshrc
```

That's it. Your environment is ready.

---

## Create the data directory structure

The pipeline expects data in a specific layout. Create it:

```bash
mkdir -p data/ylt/{verisk,risklink}
mkdir -p data/ep_summaries/{verisk,risklink}
mkdir -p data/output
# data/seeds/ already exists in the repo with seed CSV templates
```

For a step-by-step walkthrough of populating your data into these directories, see [Loading your data](load-data.md).

---

## Run a dry run (see the plan, no execution)

The `--dry-run` flag shows you exactly what the pipeline will do without actually doing it. This is fast (~1 second) and tells you if any required files are missing.

```bash
uv run rollup --dry-run
```

You'll see a colorful plan with checkmarks and error marks. It will look something like:

```
▣ seeds
  ✓ lobs (perils.csv)
  ✓ perils (perils.csv)
  ...
▶ YLTs
  ✓ verisk  (10 files, 2M rows)
  ✘ risklink (MISSING — expected data/ylt/risklink/*.parquet)
```

This tells you which files are present and which are missing. On a real run with real vendor data, you'd drop your parquets in `data/ylt/verisk/` and `data/ylt/risklink/` to resolve the missing ones.

For now, on synthetic test data, you might see a few ✘ marks — that's expected and safe to ignore.

---

## Run the full pipeline

When you're ready, run the real pipeline:

```bash
uv run rollup --yes
```

The `--yes` flag skips the interactive confirmation prompt. Here's what happens:

1. **Validation** — checks all required files exist and have the right columns (≈1 second)
2. **Data load** — reads YLT parquets and seed CSVs (≈2 seconds)
3. **Factor chain** — applies adjustments (currency, forecast, euws, fine art, etc.) (≈10-30 seconds depending on data size)
4. **Output write** — writes 8 Hisco parquets to `data/output/` (≈5 seconds)

Once done, you'll see messages like:

```
wrote HiscoAIR_202601_main.parquet (1,234,567 rows)
wrote HiscoRMS_202601_main.parquet (2,345,678 rows)
...
done: 8 files, 15,234,567 rows total
```

---

## Inspect the output

The 8 output files are in `data/output/`. Let's peek at one:

```bash
# From the repo root
python3 << 'EOF'
import polars as pl
df = pl.read_parquet("data/output/HiscoAIR_202601_main.parquet")
print(f"Shape: {df.shape}")
print(f"\nFirst 5 rows:")
print(df.head())
print(f"\nColumn names:")
print(df.columns)
EOF
```

This shows you the structure of the parquets — columns, data types, sample rows.

For a deep dive into what each column means, see [File formats](file-formats.md).

---

## Optional: Configure SQL Server push (if applicable)

If you want to push outputs to a SQL Server database:

1. **Set your connection string** in `config.py`:
   ```python
   MSSQL_CONN_STR = "mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
   ```
   Replace `server` and `database` with your actual server name and database.

2. **Test the connection**:
   ```bash
   uv run rollup test-sql
   ```
   This is read-only — it just tries to connect and reports success or failure.

3. **Push the outputs**:
   ```bash
   uv run rollup push-to-sql --yes
   ```
   This drops and recreates the 8 Hisco tables in your database.

For more details on SQL connection strings, see [Troubleshooting](troubleshooting.md).

---

## Open the docs site

This pipeline has comprehensive documentation. Open it in your browser:

```bash
uv run rollup docs
```

This builds the docs site and opens it in your default browser. You'll see:

- **Home** — this overview
- **File formats** — quick reference for every input and output
- **Data requirements** — the detailed contract between you and the pipeline (what columns every seed CSV must have, etc.)
- **Architecture** — how the code is organized
- **Operating modes** — how the pipeline decides which analyses to run
- **Factor chain** — how loss adjustments are applied
- **Calculations** — the detailed math behind each stage
- **Troubleshooting** — fixes for the 7 most common failure modes

To live-reload while editing the docs:

```bash
uv run rollup docs --serve
```

Then open http://localhost:8000 in your browser.

---

## When things go wrong

The pipeline is designed to fail fast and loudly. Before troubleshooting, run the pre-flight check:

```bash
uv run rollup --dry-run
```

This will show you exactly which files are missing or have schema drift.

If you hit an error, there are three places to look:

1. **The error message itself** — most are precise enough to act on. E.g., `SchemaError: unexpected column 'fake_col' in HiscoAIR_202601_main`
2. **The `--dry-run` output** — usually catches problems before they happen
3. **[Troubleshooting](troubleshooting.md)** — covers the 7 most common failure modes with fixes

Some common ones:

- **"fix the failing checks above"** — a seed file is missing or has the wrong columns. Check `--dry-run`.
- **"ROLLUP_MSSQL_CONN_STR is not set"** — you tried to push to SQL without configuring the connection. Edit `config.py` or set the environment variable.
- **"no Hisco*.parquet files found"** — you tried to push before running the pipeline. Run `uv run rollup --yes` first.

For a complete reference, see [Troubleshooting](troubleshooting.md).

---

## Common next steps

**I want to run on real vendor data**

See [`polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md) — it's a simple checklist of the files you need to collect from the vendors and where to put them.

**I want to understand the math**

See [Calculations](calculations.md) and [Factor chain](factor-chain.md).

**I want to customize the loss filter or logging**

Use command-line flags:
```bash
uv run rollup --yes --min-loss 0        # keep all rows (no loss cutoff)
uv run rollup --yes --log-level INFO    # see factor attachment logs
uv run rollup --yes --dump-interim      # write debug parquets to data/output/debug/
```

Or set them in `config.py`:
```python
MIN_LOSS = 500  # drop rows with loss < 500
LOG = "INFO"    # set log level
```

**I want to add a new factor (e.g., a new adjustment)**

See [Factor chain](factor-chain.md) — it has a 5-step recipe.

**I want to run the test suite**

```bash
uv run pytest polars/ -q         # run all unit tests (~5s)
uv run pytest polars/ --co       # list all tests
```

---

## Key concepts (glossary)

- **YLT** — "your loss table"; raw parquet export from the vendor modeling system
- **Seed** — reference CSV file (analyses.csv, perils.csv, etc.) that the pipeline uses to standardize data
- **Factor** — a multiplier applied to loss (e.g., currency conversion, forecast adjustment)
- **Flavor** — output variant; either `main` (fully adjusted loss) or `dialsup` (raw loss for sensitivity)
- **Hisco** — the standard loss format the downstream systems consume
- **AIR / RMS** — the two vendors; output file names are `HiscoAIR_*.parquet` and `HiscoRMS_*.parquet`
- **Rollup scope** — the official set of (LOB, vendor, analysis) combinations that are live each cycle

---

## Questions or issues?

If something doesn't work:

1. Check the error message
2. Run `uv run rollup --dry-run` to see what the pipeline thinks is available
3. Read [Troubleshooting](troubleshooting.md)
4. Check the docs site (`uv run rollup docs`)

Most questions are answered in the [Troubleshooting](troubleshooting.md) or [Data requirements](data-requirements.md) sections.
