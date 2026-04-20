# docs

Detailed documentation for the polars rollup pipeline. The polars project
[`README.md`](../polars/README.md) is the overview + schematic + run commands;
everything deeper lives here.

## Index

| doc | what it covers |
|---|---|
| [`data-requirements.md`](data-requirements.md) | **The contract for a real run.** YLT wire schemas, every seed CSV (with duckdb COPY statements where needed), currency-derivation rule, forecast-factor join contract, failure-mode reference table. |
| [`architecture.md`](architecture.md) | Code organisation. `Vendor` / `Flavor` / `VariantSpec`, seed loading, schema validation layers, the audit parquets, logging. |
| [`factor-chain.md`](factor-chain.md) | The factor chain mental model, cumulative column-naming convention, and the **5-step recipe to add a new factor**. |
| [`calculations.md`](calculations.md) | Every january duckdb view → polars stage mapping, with the original SQL quoted. Also: `apply_rollup_scope`, the `chain.CHAIN` registry walker, EP curves, the dialsup funnel, and a per-stage status summary table. |

## Also relevant (outside docs/)

- [`../polars/seeds/README.md`](../polars/seeds/README.md) — per-seed schema, source, and population status.
- [`../polars/rollup/stages/factors.py`](../polars/rollup/stages/factors.py) — the factor-attach functions. The comment block at the top duplicates the 5-step recipe for people in the code.
- [`../polars/RH-TODO-DATA.md`](../polars/RH-TODO-DATA.md) — concrete TODO list for the data-export pass.
