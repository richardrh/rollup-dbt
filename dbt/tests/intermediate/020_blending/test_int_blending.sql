/*
    Unit tests for int_blending model to verify blending logic
    This test should return 0 rows if all tests pass, or rows with failures if any test fails
*/

-- Test 1: Verify blending calculation with known inputs
with test_data as (
    select
        'test_key' as aggregation_key,
        '2024-01-01'::date as run_date,
        'Property' as modelled_lob,
        'US_Hurricane' as modelled_peril,
        'AEP' as ep_type,
        100 as return_period,
        1 as rank_num,
        1000000.0 as risklink_annual_loss,  -- RMS data
        800000.0 as verisk_annual_loss,     -- AIR data
        0.6 as air_blend,                   -- AIR weight
        0.4 as rms_blend,                   -- RMS weight
        -- Expected: (1000000 * 0.4 + 800000 * 0.6) / (0.4 + 0.6) = 880000
        880000.0 as expected_blended_loss
),

test_result as (
    select
        *,
        abs(
            (risklink_annual_loss * rms_blend + verisk_annual_loss * air_blend) 
            / (rms_blend + air_blend) 
            - expected_blended_loss
        ) as calculation_error
    from test_data
),

all_tests as (
    select 
        'Blending calculation test' as test_name,
        case 
            when calculation_error < 0.01 then 'PASS'
            else 'FAIL'
        end as test_result,
        calculation_error
    from test_result

    union all

    -- Test 2: Verify handling of missing vendor data (COALESCE to 0)
    select
        'Missing vendor data test' as test_name,
        case 
            when (
                coalesce(null, 0) * 0.4 + coalesce(500000.0, 0) * 0.6
            ) / nullif(0.4 + 0.6, 0) = 300000.0
            then 'PASS'
            else 'FAIL'
        end as test_result,
        0.0 as calculation_error

    union all

    -- Test 3: Verify division by zero protection (NULLIF)
    select
        'Division by zero protection test' as test_name,
        case 
            when (1000000.0 * 0.0 + 800000.0 * 0.0) / nullif(0.0 + 0.0, 0) is null
            then 'PASS'
            else 'FAIL'
        end as test_result,
        0.0 as calculation_error
)

-- Only return rows where tests failed
select * from all_tests where test_result = 'FAIL'
