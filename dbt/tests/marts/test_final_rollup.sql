/*
    Tests for final_rollup mart model.
    Verifies that the final output has valid structure and forecast values.
*/

with rollup_data as (
    select * from {{ ref('final_rollup') }}
),

validation_tests as (
    -- Test 1: All required dimensions are non-null
    select
        'Core dimensions not null' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from rollup_data
    where office is null
       or class is null
       or modelled_lob is null
       or peril is null
       or ep_type is null

    union all

    -- Test 2: Forecasted values are non-negative
    select
        'Forecasted values non-negative' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from rollup_data
    where forecasted_value < 0

    union all

    -- Test 3: Forecast factor is positive when present
    select
        'Forecast factor positive' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from rollup_data
    where forecast_factor is not null and forecast_factor <= 0

    union all

    -- Test 4: Metric names are valid
    select
        'Metric names valid' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from rollup_data
    where metric_name not in ('risklink_annual_loss', 'verisk_annual_loss', 'blended_annual_loss')

    union all

    -- Test 5: EP types are valid
    select
        'EP types valid' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from rollup_data
    where ep_type not in ('AEP', 'OEP', 'AAL')
)

select * from validation_tests where test_result = 'FAIL'
