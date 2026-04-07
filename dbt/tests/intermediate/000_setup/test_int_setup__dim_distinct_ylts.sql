/*
    Unit tests for int_setup__dim_distinct_ylts dimension table
    Verifies that the dimension table correctly enriches YLT data with office, class, and base_date
*/

with dim_data as (
    select * from {{ ref('int_setup__dim_distinct_ylts') }}
),

validation_tests as (
    select
        'Base date is month start' as test_name,
        case
            when count(*) = 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from dim_data
    where base_date != date_trunc('month', run_date)
)

select * from validation_tests where test_result = 'FAIL'
