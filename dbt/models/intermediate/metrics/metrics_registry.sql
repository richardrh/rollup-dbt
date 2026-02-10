{{ config(materialized='view') }}

with base_metrics as (
    select 'original_ylt_loss' as metric_code, 'base' as metric_group, null as forecast_date, false as is_euws, 'base' as currency_type, 'Base modeled YLT loss' as description
    union all
    select 'original_ylt_loss_uplifted', 'uplifted', null, false, 'base', 'Uplifted YLT loss'
    union all
    select 'original_ylt_loss_uplifted_capped', 'uplifted_capped', null, false, 'base', 'Uplifted and capped YLT loss'
    union all
    select 'original_ylt_loss_uplifted_capped_localccy', 'localccy', null, false, 'local', 'Uplifted capped YLT in local currency'
),
forecast_variants as (
    select
        concat('original_ylt_loss_uplifted_capped_localccy_', forecast_date) as metric_code,
        'forecast' as metric_group,
        forecast_date,
        false as is_euws,
        'local' as currency_type,
        concat('Forecast ', forecast_date, ' uplifted capped local currency') as description
    from {{ ref('forecast_factors') }}
    group by forecast_date
),
forecast_euws as (
    select
        concat('original_ylt_loss_uplifted_capped_localccy_', forecast_date, '_euws') as metric_code,
        'euws' as metric_group,
        forecast_date,
        true as is_euws,
        'local' as currency_type,
        concat('Forecast ', forecast_date, ' EUWS uplifted capped local currency') as description
    from {{ ref('forecast_factors') }}
    group by forecast_date
),
all_metrics as (
    select * from base_metrics
    union all
    select * from forecast_variants
    union all
    select * from forecast_euws
)
select
    metric_code,
    metric_group,
    forecast_date,
    is_euws,
    currency_type,
    description,
    md5(metric_code) as metric_id
from all_metrics
