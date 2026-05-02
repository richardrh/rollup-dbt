"""Local config — copy to `config.py` at the repo root.

`config.py` is gitignored. Anything set here is read by `rollup.config.resolve()`
when the matching env var (`ROLLUP_*`) isn't set. Env vars always win, so CI
overrides remain trivial.

To use this template:

    cp config.example.py config.py
    # edit values to match your environment

The keys below mirror the `EnvVar` enum in `polars/rollup/config.py`. Anything
omitted falls back to the repo defaults (paths under `data/`, no SQL push,
no min-loss filter).
"""

# --- Production loss-cut threshold ---------------------------------------- #
# Drop output rows whose loss is below this value. The code default is
# 1000.0 — set this attribute (or ROLLUP_MIN_LOSS / --min-loss) only when
# you want a different threshold. 0.0 disables the filter entirely.
# MIN_LOSS = 1000.0


# --- SQL Server push -------------------------------------------------------- #
# Connection string used by `rollup test-sql` and `rollup push-to-sql`.
# Both read `ROLLUP_MSSQL_CONN_STR` first, then this attribute.
#
# Windows auth (no credentials inline — recommended):
# MSSQL_CONN_STR = "mssql+pyodbc://server/database?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
#
# SQL auth:
# MSSQL_CONN_STR = "mssql+pyodbc://user:pass@server/database?driver=ODBC+Driver+17+for+SQL+Server"


# --- Data paths ------------------------------------------------------------- #
# Default layout under <repo>/data/ is usually correct. Override here if the
# data directory lives elsewhere.
#
# DATA_DIR         = "/mnt/storage/rollup/data"
# SEEDS_DIR        = "/mnt/storage/rollup/seeds"
# OUTPUT_DIR       = "/mnt/storage/rollup/output"
# YLT_VERISK_DIR   = "/mnt/storage/rollup/data/ylt/verisk"
# YLT_VERISK_GLOB  = "air_ylt_*.parquet"
# YLT_RISKLINK_DIR = "/mnt/storage/rollup/data/ylt/risklink"
# YLT_RISKLINK_GLOB= "risklink_ylt_*.parquet"
# EP_VERISK_DIR    = "/mnt/storage/rollup/data/ep_summaries/verisk"
# EP_RISKLINK_DIR  = "/mnt/storage/rollup/data/ep_summaries/risklink"


# --- Logging ---------------------------------------------------------------- #
# Default WARNING. Override per run with --log-level or ROLLUP_LOG.
# LOG = "INFO"
