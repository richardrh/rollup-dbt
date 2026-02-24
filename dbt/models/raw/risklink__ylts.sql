{{ config(materialized='view', schema='raw') }}


select *
from read_parquet('{{ var("cat_results_path") }}/risklink/elts/*.parquet',
    filename=true)
