{{ config(materialized='view') }}

with renamed as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'model_code',
            'lob',
            'analysis'
        ]) }} as forecast_factor_id,

        *
        from {{ source('cat_models_raw', 'stg_verisk__ylts') }}
)

select * from renamed



