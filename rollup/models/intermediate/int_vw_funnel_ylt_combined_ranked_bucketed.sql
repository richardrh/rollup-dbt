-- loader.main.int_vw_funnel_ylt_combined_ranked_bucketed source

CREATE VIEW int_vw_funnel_ylt_combined_ranked_bucketed AS
SELECT
    CASE
        WHEN ((vendor = 'rl')) THEN (CAST((100000.0 / rnk) AS INTEGER))
        WHEN ((vendor = 'vk')) THEN (CAST((10000.0 / rnk) AS INTEGER))
        ELSE NULL
    END AS rp,
    CASE
        WHEN ((rp < 200)) THEN (0)
        WHEN (((rp >= 200)
        AND (rp < 1000))) THEN (200)
        WHEN (((rp >= 1000)
        AND (rp < 10000))) THEN (1000)
        WHEN ((rp >= 10000)) THEN (10000)
        ELSE NULL
    END AS rp_bucket,
    *
FROM
    int_vw_funnel_ylt_combined_ranked
ORDER BY
    vendor,
    lob_id,
    region_peril_id,
    rnk;
