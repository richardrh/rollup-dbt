# Quickstart

Get up and running in 5 minutes.

## Prerequisites

- Python 3.13+ (`python --version`)
- `uv` package manager (`uv --version`, or [install](https://docs.astral.sh/uv/getting-started/installation/))
- Repo cloned

## 1. Install

macOS/Linux:

```bash
uv sync
cp rollup.example.toml rollup.local.toml
```

Windows PowerShell:

```powershell
uv sync
Copy-Item rollup.example.toml rollup.local.toml
```

## 2. Create data directories

macOS/Linux:

```bash
mkdir -p data/ylt/verisk data/ylt/risklink data/ep_summaries/verisk data/ep_summaries/risklink data/output
```

Windows PowerShell:

```powershell
New-Item -ItemType Directory -Force data/ylt/verisk, data/ylt/risklink, data/ep_summaries/verisk, data/ep_summaries/risklink, data/output
```

Full walkthrough: [Loading your data](load-data.md).

## 3. Dry run (check the plan)

```bash
uv run rollup --dry-run
```

Shows which files are present and which are missing. YLT parquets and EP-summary
`*.long.csv` files are required for the default run. Before production, replace
the bundled Verisk placeholder IDs (`900001`–`900007`) in
`data/seeds/business/{analyses,valid_analyses}.csv` with real numeric Verisk
analysis IDs; keep raw labels like `EU_WS` in `analyses.modelled_label`.

## 4. Run the pipeline

Interactive wizard:

```bash
uv run rollup
```

Non-interactive run:

```bash
uv run rollup --yes
```

Takes ~30 seconds. Output parquets and audit/debug parquets appear in `data/output/`.
Use `--no-audit` to skip `data/output/debug/`.
Normal runs use the reviewed `data/seeds/vor/blending_weights.csv`; use `--derive-blending` only to opt into run-time EP-summary blending.

## 5. Inspect output

```bash
uv run duckdb "SELECT * FROM 'data/output/HiscoAIR_202601_main.parquet' LIMIT 5;"
```

## Something broke?

See [Troubleshooting](troubleshooting.md).

## Next steps

- **Real vendor data:** see [`polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md)
- **Understand the math:** [Calculations](calculations.md) and [Factor chain](factor-chain.md)
- **Push to SQL:** set `[sql].mssql_conn_str` in `rollup.local.toml`, then `uv run rollup push-to-sql --yes`
- **Add a new factor:** [Factor chain](factor-chain.md)
- **Run tests:** `uv run pytest polars/ -q`
