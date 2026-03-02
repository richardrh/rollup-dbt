# Refactor int_blending.sql to Wide Format

## Tasks

- [x] **COMPLETED** Replace conditional aggregation with explicit vendor joins
  - Replace MAX(CASE WHEN...) with separate vendor CTEs
  - Use FULL OUTER JOIN between vendors (not INNER)
  - Include all grain columns in join keys
  - Update model references from *_mock to real models

- [x] **COMPLETED** Implement readable blending calculation in wide format
  - Use explicit column references instead of conditional aggregation
  - Handle missing vendor data with COALESCE
  - Handle missing blending factors with COALESCE
  - Protect against division by zero with NULLIF

- [x] **COMPLETED** Update and run unit tests
  - Verify existing tests still pass with new implementation
  - Update test if needed to match new wide format structure
  - Ensure all test scenarios are covered

- [x] **COMPLETED** Compile and validate model
  - Run dbt compile to check syntax
  - Verify model produces expected output structure
  - Confirm all acceptance criteria are met

- [x] **COMPLETED** Run full test suite and confirm passing
  - Execute all tests for the model
  - Verify no regressions introduced
  - Confirm implementation is complete

## Acceptance Criteria Checklist

- [x] Model uses wide format with explicit vendor CTEs (verisk_ep, risklink_ep)
- [x] Uses FULL OUTER JOIN between vendors (not INNER)
- [x] Join includes all grain columns (aggregation_key, ep_type, return_period, rank_num)
- [x] Blending calculation is readable and explicit
- [x] Handles missing vendor data (COALESCE)
- [x] Handles missing blending factors (COALESCE)
- [x] No division by zero (NULLIF)
- [x] Model compiles without errors

## IMPLEMENTATION COMPLETE ✅
