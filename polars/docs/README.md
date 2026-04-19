# docs

Detailed documentation for the polars rollup pipeline. The project-root
[`README.md`](../README.md) is the overview + schematic + run commands;
everything deeper lives here.

## Index

| doc | what it covers |
|---|---|
| [`data-requirements.md`](data-requirements.md) | **The contract for a real run.** YLT wire schemas, every seed CSV (with duckdb COPY statements where needed), currency-derivation rule, forecast-factor join contract, failure-mode reference table. |
| [`architecture.md`](architecture.md) | Code organisation. `Vendor` / `Flavor` / `VariantSpec`, seed loading, schema validation layers, the audit parquets, logging. |
| [`factor-chain.md`](factor-chain.md) | The factor chain mental model, cumulative column-naming convention, and the **5-step recipe to add a new factor**. |
| [`calculations.md`](calculations.md) | Every january duckdb view → polars stage mapping, with the original SQL quoted. Also: `apply_rollup_scope`, the `chain.CHAIN` registry walker, EP curves, the dialsup funnel, and a per-stage status summary table. |

## Also relevant (outside docs/)

- [`../seeds/README.md`](../seeds/README.md) — per-seed schema, source, and population status.
- [`../rollup/stages/factors.py`](../rollup/stages/factors.py) — the factor-attach functions. The comment block at the top duplicates the 5-step recipe for people in the code.
