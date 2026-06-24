# Troubleshooting

## Input validation failed before runtime

Validnator owns input/schema validation. Read the Validnator report, then check
the corresponding file under `data/` before re-running the runtime.

## Missing or empty outputs

- Confirm the CLI summary points at the expected absolute `output_root`.
- Check `<output-root>/rollup.log` unless you supplied `--log-file`.
- If stage outputs were disabled with `--no-stage-outputs`, `output/stages/` is
  expected to be absent.
- If analysis was disabled with `--no-analysis`, `analysis/ep_report.csv` is
  expected to be absent.

## DuckDB file missing

DuckDB export is disabled by default. Use `--duckdb`, `--duckdb-file`, or
`[outputs].write_duckdb = true`.

## Unexpected exception

Unexpected runtime errors are intentionally not hidden. Re-run with the default
log file and inspect the traceback plus the latest stage/mart output status.

## Known current follow-up

`Pen` and `Cherish` RiskLink rows can have null `modelled_lob` and
`modelled_peril` despite EP summaries containing `MGA_Pen` and `MGA_Cherish`.
Track this as a `build_enriched_ylt` RiskLink enrichment follow-up.
