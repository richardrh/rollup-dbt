{{ config(materialized='view') }}

select
    ylt.ModelEventID,
    ylt.ModelYear,
    CurrencyCode,
    ModelYOA,
    ModelGrossLoss,
    ModelInwardsReinstatement,
    date_part('doy', cast("ref".ModelOccurrenceDate as timestamp)) as ModelEventDay,
    LossClassName,
    metric
from {{ ref('mts_ylt_combined_all_factors_long__aggd_for_cds__fanout_risklink_nodayid') }} as ylt
inner join {{ source('reference', 'flood_rl22_model_events') }} as "ref"
    on "ref".ModelEventID = ylt.ModelEventID
    and "ref".ModelOccurrenceYear = ylt.ModelYear
