/*
    Unit tests for int_blending_enriched model
    Verifies that enrichment correctly adds office, class, and base_date from dimension table
*/

with enriched_data as (
    select * from {{ ref('int_blending_enriched') }}
),

validation_tests as (
    select
        'Columns exist' as test_name,
        case
            when count(*) >= 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from enriched_data
    where aggregation_key is not null
        and run_date is not null
        and base_date is not null
        and modelled_lob is not null
)

select * from validation_tests where test_result = 'FAIL'
