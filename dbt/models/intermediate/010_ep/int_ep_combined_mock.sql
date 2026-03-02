{{ config(materialized='view') }}

/*
    Mock EP combined model for testing blending logic.
    This is a simplified version that creates test data with the expected structure.
*/

with mock_ep_data as (
    select
        'test_agg_key_1' as aggregation_key,
        '2024-01-01'::date as run_date,
        'verisk' as source_vendor,
        'ANALYSIS_001' as analysis_id,
        'AEP' as ep_type,
        100 as return_period,
        1 as rank_num,
        1000000.0 as annual_loss
    
    union all
    
    select
        'test_agg_key_1' as aggregation_key,
        '2024-01-01'::date as run_date,
        'risklink' as source_vendor,
        'ANALYSIS_001' as analysis_id,
        'AEP' as ep_type,
        100 as return_period,
        1 as rank_num,
        1200000.0 as annual_loss
        
    union all
    
    select
        'test_agg_key_2' as aggregation_key,
        '2024-01-01'::date as run_date,
        'verisk' as source_vendor,
        'ANALYSIS_002' as analysis_id,
        'OEP' as ep_type,
        250 as return_period,
        1 as rank_num,
        500000.0 as annual_loss
)

select * from mock_ep_data
