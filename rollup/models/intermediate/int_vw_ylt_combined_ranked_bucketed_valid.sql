-- loader.main.int_vw_ylt_combined_ranked_bucketed_valid source

CREATE VIEW int_vw_ylt_combined_ranked_bucketed_valid AS WITH valid_analysis AS (
SELECT
    *
FROM
    int_vw_analysis_is_valid),
ranked_ylt AS (
SELECT
    *
FROM
    loader.main.int_vw_funnel_ylt_combined_ranked_bucketed
)SELECT
    ylt.*,
    va.official_rollup
FROM
    ranked_ylt AS ylt
INNER JOIN valid_analysis AS va ON
    (((va.lob_id = ylt.lob_id)
        AND (va.region_peril_id = ylt.region_peril_id)));
