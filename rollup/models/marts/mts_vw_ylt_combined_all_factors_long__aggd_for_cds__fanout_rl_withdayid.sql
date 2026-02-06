-- loader.marts.mts_vw_ylt_combined_all_factors_long__aggd_for_cds__fanout_rl_withdayid source

CREATE VIEW marts.mts_vw_ylt_combined_all_factors_long__aggd_for_cds__fanout_rl_withdayid AS
SELECT
    ylt.ModelEventID,
    ylt.ModelYear,
    CurrencyCode,
    ModelYOA,
    ModelGrossLoss,
    ModelInwardsReinstatement,
    date_part('doy', CAST("ref".ModelOccurrenceDate AS TIMESTAMP)) AS ModelEventDay,
    LossClassName,
    metric
FROM
    loader.marts.mts_vw_ylt_combined_all_factors_long__aggd_for_cds__fanout_rl_nodayid AS ylt
INNER JOIN loader.reference.flood_rl22_model_events AS "ref" ON
    ((("ref".ModelEventID = ylt.ModelEventID)
        AND ("ref".ModelOccurrenceYear = ylt.ModelYear)));
