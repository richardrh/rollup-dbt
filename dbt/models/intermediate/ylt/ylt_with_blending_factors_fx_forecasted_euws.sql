{{ config(materialized='table') }}

with ylt as (
    select
        *
from {{ ref('ylt_with_blending_factors_fx_applied') }}
),
mcl as (
    select
        ae.EventID as model_eventid,
        ae."Day",
        ae."Year",
        ae.ModelID,
        ae."Event" as eventid,
        coalesce(f.Factor, 1.0) as Factor
    from {{ source('reference', 'air_events') }} as ae
    left join {{ source('reference', 'euws_rate_factors') }} as f
        on ae.EventID = f.ModelEventID
)
select
    ylt.*,
    mcl.model_eventid as model_eventid,
    case
        when rollup_lob = 'HIC_HH_UK' and rnk <= 100 then 1.0
        else coalesce(mcl.Factor, 1.0)
    end as euws_factor,
    (original_ylt_loss_uplifted_capped * euws_factor) as original_ylt_loss_uplifted_capped_euws,
    (original_ylt_loss_uplifted_capped_localccy * euws_factor) as original_ylt_loss_uplifted_capped_localccy_euws,
    (original_ylt_loss_uplifted_capped_localccy_202601 * euws_factor) as original_ylt_loss_uplifted_capped_localccy_202601_euws,
    (original_ylt_loss_uplifted_capped_localccy_202607 * euws_factor) as original_ylt_loss_uplifted_capped_localccy_202607_euws,
    (original_ylt_loss_uplifted_capped_localccy_202701 * euws_factor) as original_ylt_loss_uplifted_capped_localccy_202701_euws
from ylt
left join mcl
    on mcl."Year" = ylt.yearid
    and mcl.EventID = ylt.eventid
    and mcl.ModelID = ylt.model_code
