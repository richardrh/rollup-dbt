-- loader.main.mts_vw_ep_combined_all_factors_from_cachetbl source

CREATE VIEW mts_vw_ep_combined_all_factors_from_cachetbl AS WITH loss_per_year AS (
SELECT
    row_number() OVER (PARTITION BY cds_cat_class_name,
    base_model,
    rollup_region_peril,
    metric
ORDER BY
    sum("value") DESC) AS rnk,
    'AEP' AS ep_type,
    cds_cat_class_name,
    base_model,
    rollup_region_peril,
    yearid,
    metric,
    sum("value") AS "value"
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long_from_cachetbl
GROUP BY
    cds_cat_class_name,
    base_model,
    rollup_region_peril,
    yearid,
    metric),
max_loss_per_year AS (
SELECT
    row_number() OVER (PARTITION BY cds_cat_class_name,
    base_model,
    rollup_region_peril,
    metric
ORDER BY
    max("value") DESC) AS rnk,
    'OEP' AS ep_type,
    cds_cat_class_name,
    base_model,
    rollup_region_peril,
    yearid,
    metric,
    sum("value") AS "value"
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long_from_cachetbl
GROUP BY
    cds_cat_class_name,
    base_model,
    rollup_region_peril,
    yearid,
    metric),
avg_annual_loss AS (
SELECT
    0 AS rnk,
    'AAL' AS ep_type,
    cds_cat_class_name,
    base_model,
    rollup_region_peril,
    0 AS yearid,
    metric,
    CASE
        WHEN ((base_model = 'rl')) THEN ((sum("value") / 100000))
        WHEN ((base_model = 'vk')) THEN ((sum("value") / 10000.0))
        ELSE NULL
    END AS "value"
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long_from_cachetbl
GROUP BY
    1,
    2,
    3,
    4,
    5,
    6,
    7),
all_ep AS (((
SELECT
    *
FROM
    loss_per_year)
UNION ALL (
SELECT
*
FROM
max_loss_per_year))
UNION ALL (
SELECT
*
FROM
avg_annual_loss)
)SELECT
    CASE
        WHEN ((rnk = 0)) THEN (0)
        WHEN ((base_model = 'rl')) THEN ((100000 / rnk))
        WHEN ((base_model = 'vk')) THEN ((10000 / rnk))
        ELSE NULL
    END AS rp,
    * EXCLUDE (yearid)
FROM
    all_ep
WHERE
    (((base_model = 'vk')
        AND (rnk IN (0, 1, 10, 50, 100, 200, 500)))
        OR ((base_model = 'rl')
            AND (rnk IN (0, 10, 100, 500, 1000, 2000, 5000))));
