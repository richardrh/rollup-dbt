-- loader.main.int_vw_blending_factors_with_forecast_ccy source

CREATE VIEW int_vw_blending_factors_with_forecast_ccy AS WITH fx AS (
SELECT
    id AS fx_rate_id,
    CurrencyCode AS currency_code,
    "Rate to GBP" AS rate_to_gbp
FROM
    reference.fx_rates
WHERE
    (CurrencyCode IN ('USD', 'EUR', 'GBP'))),
bf AS (
SELECT
    *,
    CASE
        WHEN ((cds_cat_class_name ~~ '% UK %')) THEN ('GBP')
        WHEN ((cds_cat_class_name ~~ '% EU %')) THEN ('EUR')
        ELSE 'GBP'
    END AS required_currency
FROM
    int_vw_blending_factors_with_forecast
)SELECT
    bf.*,
    fx.rate_to_gbp
FROM
    bf
INNER JOIN fx ON
    ((fx.currency_code = bf.required_currency));
