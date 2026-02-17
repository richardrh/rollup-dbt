{{ config(materialized='view') }}

with renamed as (

    select
        lob_id,
        office,
        class,
        forecast_date,
        forecast_factor
    from {{ ref('forecast_factors') }}

)

select * from renamed
