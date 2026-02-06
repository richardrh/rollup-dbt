-- loader.marts.mts_vw_ylt_combined_all_factors_long__aggd_for_cds__fanout_rl_nodayid source

CREATE VIEW marts.mts_vw_ylt_combined_all_factors_long__aggd_for_cds__fanout_rl_nodayid AS
SELECT
    ylt.eventid AS ModelEventID,
    ylt.yearid AS ModelYear,
    ylt.ccy AS CurrencyCode,
    0 AS ModelYOA,
    "value" AS ModelGrossLoss,
    0 AS ModelInwardsReinstatement,
    ae."Day" AS ModelEventDay,
    ylt.cds_cat_class_name AS LossClassName,
    metric
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl AS ylt
LEFT JOIN reference.air_events AS ae ON
    ((ylt.model_eventid = ae.EventID))
WHERE
    ((ylt.base_model = 'rl')
        AND (metric IN ('original_ylt_loss_uplifted_capped_localccy_202601_euws_fagross', 'original_ylt_loss_uplifted_capped_localccy_202607_euws_fagross', 'original_ylt_loss_uplifted_capped_localccy_202701_euws_fagross', 'original_ylt_loss_uplifted_capped_localccy_202601_euws', 'original_ylt_loss_uplifted_capped_localccy_202607_euws', 'original_ylt_loss_uplifted_capped_localccy_202701_euws'))
            AND (modelgrossloss > 0));
