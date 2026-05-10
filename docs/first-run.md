# Quickstart

Get up and running in 5 minutes.

## Prerequisites

- Python 3.13+ (`python --version`)
- `uv` package manager (`uv --version`, or [install](https://docs.astral.sh/uv/getting-started/installation/))
- Repo cloned

## 1. Install

```bash
uv sync
cp config.example.py config.py
```

## 2. Create data directories

```bash
mkdir -p data/ylt/{verisk,risklink}
mkdir -p data/output
```

Full walkthrough: [Loading your data](load-data.md).

## 3. Dry run (check the plan)

```bash
uv run rollup --dry-run
```

Shows which files are present and which are missing. Safe to ignore missing files on first run.

## 4. Run the pipeline

```bash
uv run rollup --yes
```

Takes ~30 seconds. Output parquets appear in `data/output/`.

## 5. Inspect output

```bash
python3 << 'EOF'
import polars as pl
df = pl.read_parquet("data/output/HiscoAIR_202601_main.parquet")
print(f"Shape: {df.shape}\nColumns: {df.columns}")
EOF
```

## Something broke?

See [Troubleshooting](troubleshooting.md).

## Next steps

- **Real vendor data:** see [`polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md)
- **Understand the math:** [Calculations](calculations.md) and [Factor chain](factor-chain.md)
- **Push to SQL:** set `MSSQL_CONN_STR` in `config.py`, then `uv run rollup push-to-sql --yes`
- **Add a new factor:** [Factor chain](factor-chain.md)
- **Run tests:** `uv run pytest polars/ -q`
