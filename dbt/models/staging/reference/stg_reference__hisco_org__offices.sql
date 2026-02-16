{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__offices') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'office_id',
            'country'
        ]) }} as id,

        office_id as original_office_id,
        office_id as office,
        country,
        region,
        city

    from source
)

select * from renamed
