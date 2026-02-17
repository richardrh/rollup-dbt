{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('fx_rates') }}
),


renamed as (
    select


        {{ dbt_utils.generate_surrogate_key([
            'base_currency',
            'target_ccy',
            'date',
            'rate'
        ]) }} as fx_rate_currency_id,

        base_ccy,
        target_ccy,
        date,
        rate

    from source
)

select * from renamed
