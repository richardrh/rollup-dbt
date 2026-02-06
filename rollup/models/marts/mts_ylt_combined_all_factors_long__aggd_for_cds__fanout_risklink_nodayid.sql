{{ config(materialized='view') }}

select
    ylt.eventid as ModelEventID,
    ylt.yearid as ModelYear,
    ylt.ccy as CurrencyCode,
    0 as ModelYOA,
    "value" as ModelGrossLoss,
    0 as ModelInwardsReinstatement,
    ae."Day" as ModelEventDay,
    ylt.cds_cat_class_name as LossClassName,
    metric
from {{ ref('mts_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl') }} as ylt
left join {{ source('reference', 'air_events') }} as ae
    on ylt.model_eventid = ae.EventID
where
    ylt.base_model = 'risklink'
    and metric in (
        'original_ylt_loss_uplifted_capped_localccy_202601_euws_fagross',
        'original_ylt_loss_uplifted_capped_localccy_202607_euws_fagross',
        'original_ylt_loss_uplifted_capped_localccy_202701_euws_fagross',
        'original_ylt_loss_uplifted_capped_localccy_202601_euws',
        'original_ylt_loss_uplifted_capped_localccy_202607_euws',
        'original_ylt_loss_uplifted_capped_localccy_202701_euws'
    )
    and ModelGrossLoss > 0
