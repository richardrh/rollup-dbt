{{ config(materialized='view') }}

with ylt as (
    select
        *
    from {{ ref('int_ylt_combined_ranked_bucketed_valid') }}
),
factors as (
    select
        *
    from {{ ref('int_blending_factors_with_forecast_ccy_ylt_ready') }}
)
select
    ylt.*,
    factors.risklink_proportion,
    factors.verisk_proportion,
    factors.risklink_blended_contribution,
    factors.verisk_blended_contribution,
    factors.base_model,
    factors.uplift_factor_on_base_model,
    factors.uplift_factor_on_base_model_capped,
    factors.f_202601,
    factors.f_202607,
    factors.f_202701,
    factors.required_currency,
    factors.rate_to_gbp,
    ylt.loss as original_ylt_loss,
    (original_ylt_loss * uplift_factor_on_base_model) as original_ylt_loss_uplifted,
    (original_ylt_loss * uplift_factor_on_base_model_capped) as original_ylt_loss_uplifted_capped,
    (original_ylt_loss_uplifted_capped / rate_to_gbp) as original_ylt_loss_uplifted_capped_localccy,
    (original_ylt_loss_uplifted_capped_localccy * f_202601) as original_ylt_loss_uplifted_capped_localccy_202601,
    (original_ylt_loss_uplifted_capped_localccy * f_202607) as original_ylt_loss_uplifted_capped_localccy_202607,
    (original_ylt_loss_uplifted_capped_localccy * f_202701) as original_ylt_loss_uplifted_capped_localccy_202701
from ylt
inner join factors
    on factors.base_model = ylt.vendor
    and factors.rollup_lob = ylt.rollup_lob
    and factors.rollup_region_peril = ylt.rollup_region_peril
    and factors.rp = ylt.rp_bucket
