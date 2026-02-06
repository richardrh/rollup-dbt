{{ config(materialized='view') }}

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
