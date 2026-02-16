{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('fx_rates') }}
),


renamed as (
    select

        base_ccy,
        target_ccy,
        rate

    from source
)

select * from renamed
