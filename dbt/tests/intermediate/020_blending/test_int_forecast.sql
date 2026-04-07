/*
    Unit tests for int_forecast model
    Verifies that forecast factors are correctly applied to metrics
*/

with forecast_data as (
    select * from {{ ref('int_forecast') }}
),

validation_tests as (
    select
        'Metric name values valid' as test_name,
        case
            when count(*) = 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from forecast_data
    where metric_name not in ('risklink_annual_loss', 'verisk_annual_loss', 'blended_annual_loss')

    union all

    select
        'Forecast factor applied correctly' as test_name,
        case
            when count(*) = 0 then 'PASS'
            else 'FAIL'
        end as test_result
    from forecast_data
    where forecast_factor is not null
        and abs(forecasted_value - (original_value * forecast_factor)) > 0.01
)

select * from validation_tests where test_result = 'FAIL'
