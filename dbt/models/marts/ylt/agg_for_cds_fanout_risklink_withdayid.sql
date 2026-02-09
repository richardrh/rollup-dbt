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
    metric,
    metric_id
from {{ ref('agg_for_cds_fanout_risklink_nodayid') }} as ylt
inner join {{ source('reference', 'flood_rl22_model_events') }} as "ref"
    on "ref".ModelEventID = ylt.ModelEventID
    and "ref".ModelOccurrenceYear = ylt.ModelYear
