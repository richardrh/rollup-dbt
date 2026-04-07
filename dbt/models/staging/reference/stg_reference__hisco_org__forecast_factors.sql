{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__forecast_factors') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'class',
            'office',
            'base_date',
            'forecast_date'
        ]) }} as forecast_factor_id,

        class,
        office,
        office_iso2,
        base_date,
        forecast_date,
        forecast_factor

    from source
)

select * from renamed
