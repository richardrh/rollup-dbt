{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('fx_rates') }}
)

, renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'base_ccy',
            'target_ccy'
        ]) }} as fx_rate_id,

        base_ccy,
        target_ccy,
        rate

    from source
)

select * from renamed
