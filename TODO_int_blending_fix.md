# TODO: Fix and Complete int_blending.sql Model

## Tasks

- [x] Replace broken int_blending.sql with long-format conditional aggregation approach
- [x] Write unit tests for int_blending model to verify blending logic
- [x] Test model compilation with dbt compile
- [x] Test model execution with dbt run (verified with mock data)
- [x] Verify output data structure and row counts (verified with mock data)
- [x] Run full test suite and confirm passing

## Acceptance Criteria Checklist

- [x] Model compiles without errors
- [x] Model runs successfully in DuckDB (verified with mock data)
- [x] No `r.*, v.*` wide-format patterns remain
- [x] Uses conditional aggregation (`CASE WHEN source_vendor = ...`) pattern
- [x] Handles missing blending factors gracefully (LEFT JOIN)
- [x] Handles missing vendor data gracefully (COALESCE to 0)
- [x] No division by zero risk in blending calculation

## COMPLETED SUCCESSFULLY ✅
