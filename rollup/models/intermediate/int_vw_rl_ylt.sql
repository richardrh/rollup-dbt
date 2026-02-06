-- loader.main.int_vw_rl_ylt source

CREATE VIEW int_vw_rl_ylt AS
SELECT
    lobs.id AS lob_id,
    lobs.modelled_lob,
    lobs.rollup_lob,
    lobs.lob_type,
    lobs.cds_cat_class_name,
    dra.rl_analysis_id,
    rps.id AS region_peril_id,
    rps.modelled_region_peril,
    rps.cleaned_region_peril,
    rps.rollup_region_peril,
    yearid,
    eventid,
    loss
FROM
    stg_rl_ylt
INNER JOIN dim_rl_analysis AS dra ON
    ((dra.rl_analysis_id = stg_rl_ylt.anlsid))
INNER JOIN dim_region_perils AS rps ON
    ((rps.modelled_region_peril = dra.region_peril))
INNER JOIN reference.lobs AS lobs ON
    ((lobs.modelled_lob = dra.lob));
