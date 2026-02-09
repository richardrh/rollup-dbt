{{ config(materialized='view') }}

select
    ylt.model_eventid as ModelEventID,
    ylt.yearid as ModelYear,
    ylt.ccy as CurrencyCode,
    0 as ModelYOA,
    "value" as ModelGrossLoss,
    0 as ModelInwardsReinstatement,
    ae."Day" as ModelEventDay,
    ylt.cds_cat_class_name as LossClassName,
    metric,
    metric_id,
    base_model
from {{ ref('ylt_all_factors_long_aggd_for_cds_from_cachetbl') }} as ylt
    inner join {{ ref('metrics_registry') }} as dm
    on dm.metric_code = ylt.metric
left join {{ source('reference', 'air_events') }} as ae
    on ylt.model_eventid = ae.EventID
where
    dm.metric_code = 'original_ylt_loss'
    and ModelGrossLoss > 0
