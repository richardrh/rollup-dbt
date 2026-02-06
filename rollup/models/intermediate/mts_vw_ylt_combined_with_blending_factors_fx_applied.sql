-- loader.main.mts_vw_ylt_combined_with_blending_factors_fx_applied source

CREATE VIEW mts_vw_ylt_combined_with_blending_factors_fx_applied AS WITH ylt AS (
SELECT
    *
FROM
    loader.main.int_vw_ylt_combined_ranked_bucketed_valid),
factors AS (
SELECT
    *
FROM
    loader.main.int_vw_blending_factors_with_forecast_ccy_ylt_ready
)SELECT
    ylt.*,
    factors.rl_proportion,
    factors.vk_proportion,
    factors.rl_blended_contribution,
    factors.vk_blended_contribution,
    factors.base_model,
    factors.uplift_factor_on_base_model,
    factors.uplift_factor_on_base_model_capped,
    factors.f_202601,
    factors.f_202607,
    factors.f_202701,
    factors.required_currency,
    factors.rate_to_gbp,
    ylt.loss AS original_ylt_loss,
    (original_ylt_loss * uplift_factor_on_base_model) AS original_ylt_loss_uplifted,
    (original_ylt_loss * uplift_factor_on_base_model_capped) AS original_ylt_loss_uplifted_capped,
    (original_ylt_loss_uplifted_capped / rate_to_gbp) AS original_ylt_loss_uplifted_capped_localccy,
    (original_ylt_loss_uplifted_capped_localccy * f_202601) AS original_ylt_loss_uplifted_capped_localccy_202601,
    (original_ylt_loss_uplifted_capped_localccy * f_202607) AS original_ylt_loss_uplifted_capped_localccy_202607,
    (original_ylt_loss_uplifted_capped_localccy * f_202701) AS original_ylt_loss_uplifted_capped_localccy_202701
FROM
    ylt
INNER JOIN factors ON
    (((factors.base_model = ylt.vendor)
        AND (factors.rollup_lob = ylt.rollup_lob)
            AND (factors.rollup_region_peril = ylt.rollup_region_peril)
                AND (factors.rp = ylt.rp_bucket)));
