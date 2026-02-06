-- loader.main.int_vw_blending_factors_with_forecast_ccy_ylt_ready source

CREATE VIEW int_vw_blending_factors_with_forecast_ccy_ylt_ready AS
SELECT
    *
FROM
    loader.main.int_vw_blending_factors_with_forecast_ccy
WHERE
    ((official_rollup = 1)
        AND (ep_type IN ('AAL', 'OEP')));
