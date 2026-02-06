-- loader.main.int_vw_vk_ylt source

CREATE VIEW int_vw_vk_ylt AS
SELECT
    lobs.id AS lob_id,
    lobs.modelled_lob,
    lobs.rollup_lob,
    lobs.lob_type,
    lobs.cds_cat_class_name,
    rps.id AS region_peril_id,
    rps.modelled_region_peril,
    rps.cleaned_region_peril,
    rps.rollup_region_peril,
    model_code,
    yearid,
    eventid,
    net_pre_cat_loss AS loss
FROM
    stg_vk_ylt AS stg_ylt
INNER JOIN reference.lobs AS lobs ON
    ((lobs.modelled_lob = stg_ylt.lob))
INNER JOIN dim_region_perils AS rps ON
    ((rps.modelled_region_peril = stg_ylt.analysis))
WHERE
    ((rps.vendor = 'vk')
        AND (catalog_type_code = 'STC'));
