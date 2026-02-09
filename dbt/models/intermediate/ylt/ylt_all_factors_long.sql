{{ config(materialized='view') }}

with unpivoted as (
    select
        lob_id,
        region_peril_id,
        rollup_lob,
        rollup_region_peril,
        vendor,
        cds_cat_class_name,
        yearid,
        eventid,
        base_model,
        model_eventid,
        required_currency,
        metric,
        "value"
from {{ ref('ylt_with_blending_factors_fx_forecasted_euws') }}
    unpivot ("value" for metric in (
        {{ metric_code_list() }}
    )) as unpiv
)
select
    unpivoted.*,
    md5(unpivoted.metric) as metric_id,
    dm.metric_group,
    dm.forecast_date,
    dm.is_euws,
    dm.currency_type
from unpivoted
    inner join {{ ref('metrics_registry') }} as dm
    on dm.metric_code = unpivoted.metric
