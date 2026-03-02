{{ config(materialized='view') }}

with mock_analysis_data as (
    select
        'pk_001' as pk,
        'verisk' as source_vendor,
        '2024-01-01'::date as run_date,
        'ANALYSIS_001' as analysis_id,
        'Property' as modelled_lob,
        'US_Hurricane' as modelled_peril,
        true as is_official
    
    union all
    
    select
        'pk_002' as pk,
        'risklink' as source_vendor,
        '2024-01-01'::date as run_date,
        'ANALYSIS_001' as analysis_id,
        'Property' as modelled_lob,
        'US_Hurricane' as modelled_peril,
        true as is_official
        
    union all
    
    select
        'pk_003' as pk,
        'verisk' as source_vendor,
        '2024-01-01'::date as run_date,
        'ANALYSIS_002' as analysis_id,
        'Property' as modelled_lob,
        'US_Earthquake' as modelled_peril,
        true as is_official
)

select * from mock_analysis_data
