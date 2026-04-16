-- air.main.ylt_vw source

CREATE VIEW ylt_vw AS
SELECT
    Analysis AS analysis,
    ExposureAttribute AS lob,
    ModelCode AS model_code,
    PerilSetCode AS perilset_code,
    YearID AS yearid,
    EventID AS eventid,
    main."trim"(upper(CatalogTypeCode)) AS catalog_type_code,
    GrossLoss AS gross_loss,
    NetOfPreCatLoss AS net_pre_cat_loss
FROM
    ylt
WHERE
    (catalog_type_code ~~ '%STC%')
ORDER BY
    analysis,
    lob;
