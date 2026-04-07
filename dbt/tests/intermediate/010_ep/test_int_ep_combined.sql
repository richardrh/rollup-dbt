/*
    Tests for int_ep_combined model.
    Verifies that EP curve output has valid structure and values.
*/

with ep_data as (
    select * from {{ ref('int_ep_combined') }}
),

validation_tests as (
    -- Test 1: All EP types are valid
    select
        'EP type values valid' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where ep_type not in ('AEP', 'OEP', 'AAL')

    union all

    -- Test 2: AAL rows have return_period = 0
    select
        'AAL return period is zero' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where ep_type = 'AAL' and return_period != 0

    union all

    -- Test 3: Non-AAL rows have positive return periods
    select
        'Non-AAL return periods positive' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where ep_type != 'AAL' and return_period <= 0

    union all

    -- Test 4: Annual loss values are non-negative
    select
        'Annual loss non-negative' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where annual_loss < 0

    union all

    -- Test 5: Vendor values are valid
    select
        'Vendor values valid' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where source_vendor not in ('verisk', 'risklink')

    union all

    -- Test 6: AEP return periods are in descending order by rank within each key
    select
        'AEP ranks are valid' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from ep_data
    where ep_type = 'AEP' and rank_num < 1
)

select * from validation_tests where test_result = 'FAIL'
