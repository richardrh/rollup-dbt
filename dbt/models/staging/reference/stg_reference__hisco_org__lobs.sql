{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__lobs') }}
),

class_types as (
    select
        class_name,
        class_type
    from {{ ref('hisco_org__classes') }}
),

renamed as (
    select

        {{ dbt_utils.generate_surrogate_key([
            'lob_id',
            'rollup_lob'
        ]) }} as id,

        lob_id as original_lob_id,
        lob_id as modelled_lob,
        rollup_lob,
        class_types.class_type as lob_type,
        cds_cat_class_name,
        nds_class_name_gbp,
        nds_class_name_usd

    from source
    left join class_types
        on class_types.class_name = source.cds_cat_class_name
)

select * from renamed
