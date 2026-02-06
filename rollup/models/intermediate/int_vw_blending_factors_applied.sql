-- loader.main.int_vw_blending_factors_applied source

CREATE VIEW int_vw_blending_factors_applied AS WITH vendor_losses AS (
SELECT
    *
FROM
    loader.main.int_vw_blending__vendor_proportions_all_rps_pre_factors
WHERE
    ((ep_type = 'AAL')
        OR ((ep_type IN ('OEP', 'AEP'))
            AND (rp IN (200, 1000, 10000))))
ORDER BY
    rl_loss DESC
)SELECT
    losses.*,
    bf.RegionPeril,
    bf.SubRegionPeril,
    bf.AIRBlend,
    bf.RMSBlend,
    (COALESCE(rl_loss, 1) * RMSBlend) AS rl_blended_contribution,
    (COALESCE(vk_loss, 1) * AIRBlend) AS vk_blended_contribution,
    (rl_blended_contribution + vk_blended_contribution) AS blended_target_loss,
    CASE
        WHEN ((rollup_region_peril IN ('EU_FL', 'UK_FL'))) THEN ('rl')
        ELSE 'vk'
    END AS base_model,
    CASE
        WHEN ((base_model = 'rl')) THEN (losses.rl_loss)
        ELSE losses.vk_loss
    END AS base_model_loss,
    CAST(COALESCE((blended_target_loss / base_model_loss)) AS FLOAT) AS uplift_factor_on_base_model,
    CAST(greatest(0.1, least(10.0, uplift_factor_on_base_model)) AS FLOAT) AS uplift_factor_on_base_model_capped
FROM
    vendor_losses AS losses
INNER JOIN reference.blending_factors AS bf ON
    (((bf.RegionPerilID = losses.blending_factor_region_peril_id)
        AND (bf.SubRegionPerilID = losses.blending_factor_sub_region_peril_id)));
