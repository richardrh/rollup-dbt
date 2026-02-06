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
    base_model
from {{ ref('mts_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl') }} as ylt
left join {{ source('reference', 'air_events') }} as ae
    on ylt.model_eventid = ae.EventID
where
    metric in ('original_ylt_loss')
    and ModelGrossLoss > 0
