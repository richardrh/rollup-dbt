-- loader.main.mts_vw_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl source

CREATE VIEW mts_vw_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl AS
SELECT
    base_model,
    model_eventid,
    yearid,
    eventid,
    required_currency AS ccy,
    0 AS yoa,
    cds_cat_class_name,
    metric,
    sum("value") AS "value"
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long_from_cachetbl
GROUP BY
    base_model,
    model_eventid,
    yearid,
    eventid,
    required_currency,
    cds_cat_class_name,
    metric;
