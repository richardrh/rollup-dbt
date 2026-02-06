{{ config(materialized='view') }}

with fx as (
    select
        id as fx_rate_id,
        CurrencyCode as currency_code,
        "Rate to GBP" as rate_to_gbp
    from {{ source('reference', 'fx_rates') }}
    where CurrencyCode in ('USD', 'EUR', 'GBP')
),
bf as (
    select
        *,
        case
            when cds_cat_class_name like '% UK %' then 'GBP'
            when cds_cat_class_name like '% EU %' then 'EUR'
            else 'GBP'
        end as required_currency
    from {{ ref('int_blending_factors_with_forecast') }}
)
select
    bf.*,
    fx.rate_to_gbp
from bf
inner join fx
    on fx.currency_code = bf.required_currency
