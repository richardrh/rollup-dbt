/*
    Unit tests for int_blending_long model
    Verifies that unpivoting correctly transforms wide format to long format
*/

with long_data as (
    select * from {{ ref('int_blending_long') }}
),

validation_tests as (
    select
        'Metric name values valid' as test_name,
        case
            when count(*) = 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from long_data
    where metric_name not in ('risklink_annual_loss', 'verisk_annual_loss', 'blended_annual_loss')

    union all

    select
        'Metric value not null' as test_name,
        case
            when count(*) = 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from long_data
    where metric_value is null
)

select * from validation_tests where test_result = 'FAIL'
