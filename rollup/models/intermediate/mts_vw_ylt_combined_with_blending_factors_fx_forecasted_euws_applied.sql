-- loader.main.mts_vw_ylt_combined_with_blending_factors_fx_forecasted_euws_applied source

CREATE VIEW mts_vw_ylt_combined_with_blending_factors_fx_forecasted_euws_applied AS WITH ylt AS (
SELECT
    *
FROM
    loader.main.mts_vw_ylt_combined_with_blending_factors_fx_applied),
mcl AS (
SELECT
    ae.EventID AS model_eventid,
    ae."Day",
    ae."Year",
    ae.ModelID,
    ae."Event" AS eventid,
    COALESCE(f.Factor, 1.0) AS Factor
FROM
    reference.air_events AS ae
LEFT JOIN reference.euws_rate_factors AS f ON
    ((ae.EventID = f.ModelEventID))
)SELECT
    ylt.*,
    mcl.model_eventid AS model_eventid,
    CASE
        WHEN (((rollup_lob = 'HIC_HH_UK')
            AND (rnk <= 100))) THEN (1.0)
        ELSE COALESCE(mcl.Factor, 1.0)
    END AS euws_factor,
    (original_ylt_loss_uplifted_capped * euws_factor) AS original_ylt_loss_uplifted_capped_euws,
    (original_ylt_loss_uplifted_capped_localccy * euws_factor) AS original_ylt_loss_uplifted_capped_localccy_euws,
    (original_ylt_loss_uplifted_capped_localccy_202601 * euws_factor) AS original_ylt_loss_uplifted_capped_localccy_202601_euws,
    (original_ylt_loss_uplifted_capped_localccy_202607 * euws_factor) AS original_ylt_loss_uplifted_capped_localccy_202607_euws,
    (original_ylt_loss_uplifted_capped_localccy_202701 * euws_factor) AS original_ylt_loss_uplifted_capped_localccy_202701_euws
FROM
    ylt
LEFT JOIN mcl ON
    (((mcl."Year" = ylt.yearid)
        AND (mcl.EventID = ylt.eventid)
            AND (mcl.ModelID = ylt.model_code)));
