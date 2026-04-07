{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__lobs') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'lob_id',
            'rollup_lob'
        ]) }} as lob_id,

        lob_id as original_lob_id,
        modelled_lob,
        rollup_lob,
        lob_type,
        cds_cat_class_name,
        office,
        class

    from source
)

select * from renamed
