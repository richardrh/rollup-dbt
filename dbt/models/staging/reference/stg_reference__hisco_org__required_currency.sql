{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__required_currency') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'currency'
        ]) }} as required_currency_id,

        currency

    from source
)

select * from renamed
