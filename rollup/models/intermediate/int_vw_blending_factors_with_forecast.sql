-- loader.main.int_vw_blending_factors_with_forecast source

CREATE VIEW int_vw_blending_factors_with_forecast AS
SELECT
    bfa.*,
    ff.office AS forecast_factor_office,
    ff."class" AS forecast_factor_class,
    COALESCE(ff.f_202601, 1.0) AS f_202601,
    COALESCE(ff.f_202607, 1.0) AS f_202607,
    COALESCE(ff.f_202701, 1.0) AS f_202701
FROM
    int_vw_blending_factors_applied AS bfa
LEFT JOIN reference.forecast_factors_with_lobs_to_apply AS ff ON
    ((ff.lob_id = bfa.lob_id));-- loader.main.int_vw_blending_factors_with_forecast source

CREATE VIEW int_vw_blending_factors_with_forecast AS
SELECT
    bfa.*,
    ff.office AS forecast_factor_office,
    ff."class" AS forecast_factor_class,
    COALESCE(ff.f_202601, 1.0) AS f_202601,
    COALESCE(ff.f_202607, 1.0) AS f_202607,
    COALESCE(ff.f_202701, 1.0) AS f_202701
FROM
    int_vw_blending_factors_applied AS bfa
LEFT JOIN reference.forecast_factors_with_lobs_to_apply AS ff ON
    ((ff.lob_id = bfa.lob_id));
