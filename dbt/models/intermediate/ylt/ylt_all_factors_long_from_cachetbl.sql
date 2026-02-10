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
with_metric as (
    select
        unpivoted.*,
        md5(metric) as metric_id,
        dm.metric_group,
        dm.forecast_date,
        dm.is_euws,
        dm.currency_type
    from unpivoted
    inner join {{ ref('metrics_registry') }} as dm
        on dm.metric_code = unpivoted.metric
),
adjusted as (
    select
        with_metric.*,
        COALESCE(l1.adjustment_factor,
             l2.adjustment_factor,
             l3.adjustment_factor,
             l4.adjustment_factor,
             l5.adjustment_factor,
             1.0) AS adjustment_factor
    from with_metric
    left join {{ ref('vor__custom__event') }} as l1 on
        ((l1.lob_id = with_metric.lob_id)
            AND (l1.region_peril_id = with_metric.region_peril_id)
            AND (l1.vendor = with_metric.vendor)
         AND (l1.metric = with_metric.metric)
         AND (l1.yearid = with_metric.yearid)
         AND (l1.eventid = with_metric.eventid)
         AND (l1.model_eventid = with_metric.model_eventid))
    left join {{ ref('vor__custom__lob_region_eventid') }} as l2 on
        ((l2.lob_id = with_metric.lob_id)
            AND (l2.region_peril_id = with_metric.region_peril_id)
            AND (l2.vendor = with_metric.vendor)
            AND (l2.metric = with_metric.metric))
    left join {{ ref('vor__custom__lob_region_vendor') }} as l3 on
        ((l3.lob_id = with_metric.lob_id)
            AND (l3.region_peril_id = with_metric.region_peril_id)
            AND (l3.vendor = with_metric.vendor))
    left join {{ ref('vor__custom__lob_vendor') }} as l4 on
        ((l4.lob_id = with_metric.lob_id)
            AND (l4.vendor = with_metric.vendor))
    left join {{ ref('vor__custom__lob') }} as l5 on
        ((l5.lob_id = with_metric.lob_id))
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
    metric_id,
    metric_group,
    forecast_date,
    is_euws,
    currency_type,
    adjustment_factor,
    metric,
    ("value" * adjustment_factor) as "value"
from adjusted
