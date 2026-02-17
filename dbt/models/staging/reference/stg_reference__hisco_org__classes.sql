{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__classes') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'class_id',
            'class_name',
        ]) }} as id,

        class_id as original_class_id,
        class_name,
        class_type

    from source
)

select * from renamed
