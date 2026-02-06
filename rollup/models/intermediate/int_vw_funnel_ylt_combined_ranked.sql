-- loader.main.int_vw_funnel_ylt_combined_ranked source

CREATE VIEW int_vw_funnel_ylt_combined_ranked AS WITH ylt AS ((
SELECT
    'vk' AS vendor,
    lob_id,
    region_peril_id,
    rollup_region_peril,
    rollup_lob,
    cds_cat_class_name,
    model_code,
    yearid,
    eventid,
    loss
FROM
    int_vw_vk_ylt)
UNION ALL (
SELECT
'rl' AS vendor,
lob_id,
region_peril_id,
rollup_region_peril,
rollup_lob,
cds_cat_class_name,
0 AS model_code,
yearid,
eventid,
loss
FROM
int_vw_rl_ylt)
)SELECT
    row_number() OVER (PARTITION BY vendor,
    lob_id,
    region_peril_id
ORDER BY
    loss DESC) AS rnk,
    vendor,
    lob_id,
    region_peril_id,
    rollup_region_peril,
    rollup_lob,
    cds_cat_class_name,
    model_code,
    yearid,
    eventid,
    loss
FROM
    ylt;
