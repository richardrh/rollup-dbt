-- loader.main.int_vw_blending__vendor_proportions_all_rps_pre_factors source

CREATE VIEW int_vw_blending__vendor_proportions_all_rps_pre_factors AS WITH filtered_ep AS (
SELECT
    vendor,
    lob_id,
    modelled_lob,
    rollup_lob,
    lob_type,
    region_peril_id,
    modelled_region_peril,
    rollup_region_peril,
    adjustments,
    official_rollup,
    ep_type,
    rp,
    blending_factor_region_peril_id,
    blending_factor_sub_region_peril_id,
    cds_cat_class_name,
    round(gl, 0) AS gl
FROM
    vw_ep
WHERE
    ((official_rollup = 1)
        AND (ep_type IN ('AAL', 'OEP'))
            AND (rp IN (0, 200, 1000, 10000)))
ORDER BY
    rollup_region_peril),
rl AS (
SELECT
    *
FROM
    filtered_ep
WHERE
    (vendor = 'rl')),
vk AS (
SELECT
    *
FROM
    filtered_ep
WHERE
    (vendor = 'vk')
)SELECT
    rl.lob_id,
    rl.modelled_lob,
    rl.rollup_lob,
    rl.lob_type,
    rl.region_peril_id AS rl_region_peril_id,
    vk.region_peril_id AS vk_region_peril_id,
    rl.modelled_region_peril,
    rl.rollup_region_peril,
    rl.adjustments,
    rl.official_rollup,
    rl.blending_factor_region_peril_id,
    rl.blending_factor_sub_region_peril_id,
    rl.cds_cat_class_name,
    rl.ep_type,
    rl.rp,
    rl.gl AS rl_loss,
    vk.gl AS vk_loss,
    (rl.gl / (rl.gl + vk.gl)) AS rl_proportion,
    (vk.gl / (rl.gl + vk.gl)) AS vk_proportion,
    (rl.gl / vk.gl) AS ratio_to_verisk
FROM
    rl
INNER JOIN vk ON
    (((rl.rollup_lob = vk.rollup_lob)
        AND (rl.rollup_region_peril = vk.rollup_region_peril)
            AND (rl.ep_type = vk.ep_type)
                AND (rl.rp = vk.rp)));
