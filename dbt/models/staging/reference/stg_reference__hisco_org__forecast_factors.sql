{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('forecast_factors') }}
),


renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'modelled_lob',
            'forecast_date',
            'office',
            'class',
            'factor'
        ]) }} as forecast_factor_id,


        modelled_lob
        forecast_date,
        office,
        class,
        factor

    from source
)

select * from renamed
