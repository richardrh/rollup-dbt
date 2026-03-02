# TODO: Convert int_ep_combined to Incremental Materialization

## Tasks

- [x] Update model configuration to use incremental materialization with proper unique_key and strategy
- [x] Add incremental filter logic to only process new/changed aggregation_keys based on run_date
- [x] Restructure CTEs to use filtered ylt_data and apply incremental logic to dim_lookup
- [x] Create comprehensive unit tests for incremental behavior
- [x] Test first run (full-refresh) processes all data correctly
- [x] Test second run (no new data) completes quickly without reprocessing
- [x] Test incremental run with new data only processes changed aggregation_keys
- [x] Verify compiled SQL is correct for both incremental and full-refresh modes
- [x] Run full test suite and confirm passing

## Acceptance Criteria
- Model compiles without errors
- First run (--full-refresh) processes all data
- Second run (no new data) completes quickly without reprocessing
- Adding new YLT data with later run_date triggers recalculation only for those aggregation_keys
- Incremental filter is applied to both ylt_data and dim_lookup CTEs
- All tests pass
