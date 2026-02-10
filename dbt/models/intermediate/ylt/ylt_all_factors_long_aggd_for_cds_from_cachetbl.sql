{{ config(materialized='view') }}

select
    base_model,
    model_eventid,
    yearid,
    eventid,
    required_currency as ccy,
    0 as yoa,
    cds_cat_class_name,
    metric_id,
    metric_group,
    forecast_date,
    is_euws,
    currency_type,
    metric,
    sum("value") as "value"
from {{ ref('ylt_all_factors_long_from_cachetbl') }}
group by
    base_model,
    model_eventid,
    yearid,
    eventid,
    required_currency,
    cds_cat_class_name,
    metric_id,
    metric_group,
    forecast_date,
    is_euws,
    currency_type,
    metric
