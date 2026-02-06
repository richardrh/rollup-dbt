-- loader.marts.mts_vw_ylt_combined_all_factors_long__aggd_dials_up source

CREATE VIEW marts.mts_vw_ylt_combined_all_factors_long__aggd_dials_up AS
SELECT
    ylt.model_eventid AS ModelEventID,
    ylt.yearid AS ModelYear,
    ylt.ccy AS CurrencyCode,
    0 AS ModelYOA,
    "value" AS ModelGrossLoss,
    0 AS ModelInwardsReinstatement,
    ae."Day" AS ModelEventDay,
    ylt.cds_cat_class_name AS LossClassName,
    metric,
    base_model
FROM
    loader.main.mts_vw_ylt_combined_all_factors_long__aggd_for_cds_from_cachetbl AS ylt
LEFT JOIN reference.air_events AS ae ON
    ((ylt.model_eventid = ae.EventID))
WHERE
    ((metric IN ('original_ylt_loss'))
        AND (modelgrossloss > 0));
