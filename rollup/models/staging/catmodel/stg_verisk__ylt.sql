{{ config(materialized='view') }}

select
    lob,
    analysis,
    model_code,
    yearid,
    eventid,
    net_pre_cat_loss,
    catalog_type_code
from {{ source('catmodel', 'verisk_ylt') }}
