-- loader.main.vw_ep source

CREATE VIEW vw_ep AS WITH modelled_lobs AS (
SELECT
    modelled_lob,
    rollup_lob,
    lob_type,
    cds_cat_class_name,
    id AS id
FROM
    reference.lobs),
region_perils AS (
SELECT
    vendor,
    modelled_region_peril,
    cleaned_region_peril,
    rollup_region_peril,
    region,
    peril,
    adjustments,
    applies_to_mga,
    applies_to_prop,
    applies_to_fa,
    blending_factor_region_peril_id,
    blending_factor_sub_region_peril_id,
    id
FROM
    dim_region_perils)(
SELECT
    'rl' AS vendor,
    rp,
    ep_type,
    modelled_lob,
    rollup_lob,
    lob_type,
    modelled_region_peril,
    cleaned_region_peril,
    rollup_region_peril,
    region,
    peril,
    adjustments,
    CASE
        WHEN ((lob_type = 'mga')) THEN (applies_to_mga)
        WHEN ((lob_type = 'prop')) THEN (applies_to_prop)
        WHEN ((lob_type = 'fa')) THEN (applies_to_fa)
        ELSE 0
    END AS official_rollup,
    rp.id AS region_peril_id,
    lobs.id AS lob_id,
    blending_factor_region_peril_id,
    blending_factor_sub_region_peril_id,
    cds_cat_class_name,
    gl
FROM
    stg_rl_ep AS rms
INNER JOIN modelled_lobs AS lobs ON
    ((lobs.modelled_lob = rms.lob))
INNER JOIN region_perils AS rp ON
    (((rp.modelled_region_peril = rms.region_peril)
        AND (rp.vendor = 'rl'))))
UNION ALL (
SELECT
'vk' AS vendor,
rp,
ep_type,
modelled_lob,
rollup_lob,
lob_type,
modelled_region_peril,
cleaned_region_peril,
rollup_region_peril,
region,
peril,
adjustments,
CASE
    WHEN ((lob_type = 'mga')) THEN (applies_to_mga)
    WHEN ((lob_type = 'prop')) THEN (applies_to_prop)
    WHEN ((lob_type = 'fa')) THEN (applies_to_fa)
    ELSE 0
END AS official_rollup,
rp.id AS region_peril_id,
lobs.id AS lob_id,
blending_factor_region_peril_id,
blending_factor_sub_region_peril_id,
cds_cat_class_name,
gl
FROM
stg_vk_ep AS ep
INNER JOIN modelled_lobs AS lobs ON
((lobs.modelled_lob = ep.lob))
INNER JOIN region_perils AS rp ON
(((rp.modelled_region_peril = ep.analysis)
    AND (rp.vendor = 'vk'))));
