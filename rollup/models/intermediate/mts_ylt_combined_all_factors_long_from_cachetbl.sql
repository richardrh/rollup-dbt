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
    from {{ ref('mts_ylt_combined_with_blending_factors_fx_forecasted_euws_applied') }}
    unpivot ("value" for metric in (
        'original_ylt_loss',
        'original_ylt_loss_uplifted',
        'original_ylt_loss_uplifted_capped',
        'original_ylt_loss_uplifted_capped_localccy',
        'original_ylt_loss_uplifted_capped_localccy_202601',
        'original_ylt_loss_uplifted_capped_localccy_202607',
        'original_ylt_loss_uplifted_capped_localccy_202701',
        'original_ylt_loss_uplifted_capped_localccy_202601_euws',
        'original_ylt_loss_uplifted_capped_localccy_202607_euws',
        'original_ylt_loss_uplifted_capped_localccy_202701_euws'
    )) as unpiv
),
adjusted as (
    SELECT
        unpivoted.*,
        COALESCE(l1.adjustment_factor,
             l2.adjustment_factor,
             l3.adjustment_factor,
             l4.adjustment_factor,
             l5.adjustment_factor,
             1.0) AS adjustment_factor
    from unpivoted
    left join {{ ref('adjustment_factors_l1__event') }} as l1 on
        ((l1.lob_id = unpivoted.lob_id)
            AND (l1.region_peril_id = unpivoted.region_peril_id)
            AND (l1.vendor = unpivoted.vendor)
         AND (l1.metric = unpivoted.metric)
         AND (l1.yearid = unpivoted.yearid)
         AND (l1.eventid = unpivoted.eventid)
         AND (l1.model_eventid = unpivoted.model_eventid))
    left join {{ ref('adjustment_factors_l2__metric') }} as l2 on
        ((l2.lob_id = unpivoted.lob_id)
            AND (l2.region_peril_id = unpivoted.region_peril_id)
            AND (l2.vendor = unpivoted.vendor)
            AND (l2.metric = unpivoted.metric))
    left join {{ ref('adjustment_factors_l3__vendor') }} as l3 on
        ((l3.lob_id = unpivoted.lob_id)
            AND (l3.region_peril_id = unpivoted.region_peril_id)
            AND (l3.vendor = unpivoted.vendor))
    left join {{ ref('adjustment_factors_l4__lob_vendor') }} as l4 on
        ((l4.lob_id = unpivoted.lob_id)
            AND (l4.vendor = unpivoted.vendor))
    left join {{ ref('adjustment_factors_l5__lob') }} as l5 on
        ((l5.lob_id = unpivoted.lob_id))
)
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
    adjustment_factor,
    metric,
    ("value" * adjustment_factor) as "value"
from adjusted
