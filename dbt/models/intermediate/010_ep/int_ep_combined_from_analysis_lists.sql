
{{ config(materialized='view') }}

with source as (
    select * from {{ ref('stg_cat_modelling_results__analysis_lists') }}
)

{{ config(materialized='view') }}

with source as (
    select *
    from {{ ref('hisco_org__lobs') }}
),

, lobs as (

    select

    lob_id,
    original_lob_id,
    modelled_lob,
    rollup_lob,
    lob_type,
    cds_cat_class_name,
    office,
    class

    from {{ ref('stg_reference__hisco_org_lobs') }}

)

select * from lobs
