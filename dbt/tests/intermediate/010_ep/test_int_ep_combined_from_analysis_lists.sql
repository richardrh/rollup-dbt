/*
    Tests for int_ep_combined_from_analysis_lists model.
    Verifies that vendor-pivoted analysis list data has valid structure.
*/

with analysis_data as (
    select * from {{ ref('int_ep_combined_from_analysis_lists') }}
),

validation_tests as (
    -- Test 1: All risklink values are non-negative
    select
        'RiskLink AAL non-negative' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from analysis_data
    where risklink_aal < 0

    union all

    -- Test 2: All verisk values are non-negative
    select
        'Verisk AAL non-negative' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from analysis_data
    where verisk_aal < 0

    union all

    -- Test 3: modelled_lob is never null
    select
        'modelled_lob not null' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from analysis_data
    where modelled_lob is null

    union all

    -- Test 4: peril is never null
    select
        'peril not null' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from analysis_data
    where peril is null

    union all

    -- Test 5: At least one vendor has data per row
    select
        'At least one vendor has data' as test_name,
        case when count(*) = 0 then 'PASS' else 'FAIL' end as test_result
    from analysis_data
    where risklink_aal is null and verisk_aal is null
)

select * from validation_tests where test_result = 'FAIL'
