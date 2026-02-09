{{ config(materialized='view') }}

select
    anlsid,
    yearid,
    eventid,
    loss
from {{ source('catmodel', 'risklink_ylt') }}
