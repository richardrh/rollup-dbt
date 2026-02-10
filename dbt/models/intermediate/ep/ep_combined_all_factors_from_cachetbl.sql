{{ config(materialized='view') }}

with loss_per_year as (
    select
        row_number() over (
            partition by cds_cat_class_name, base_model, rollup_region_peril, metric
            order by sum("value") desc
        ) as rnk,
        'AEP' as ep_type,
        cds_cat_class_name,
        base_model,
        rollup_region_peril,
        yearid,
        metric_id,
        metric,
        sum("value") as "value"
    from {{ ref('ylt_all_factors_long_from_cachetbl') }}
    group by
        cds_cat_class_name,
        base_model,
        rollup_region_peril,
        yearid,
        metric_id,
        metric
),
max_loss_per_year as (
    select
        row_number() over (
            partition by cds_cat_class_name, base_model, rollup_region_peril, metric
            order by max("value") desc
        ) as rnk,
        'OEP' as ep_type,
        cds_cat_class_name,
        base_model,
        rollup_region_peril,
        yearid,
        metric_id,
        metric,
        sum("value") as "value"
    from {{ ref('ylt_all_factors_long_from_cachetbl') }}
    group by
        cds_cat_class_name,
        base_model,
        rollup_region_peril,
        yearid,
        metric_id,
        metric
),
avg_annual_loss as (
    select
        0 as rnk,
        'AAL' as ep_type,
        cds_cat_class_name,
        base_model,
        rollup_region_peril,
        0 as yearid,
        metric_id,
        metric,
        case
            when base_model = 'risklink' then (sum("value") / 100000)
            when base_model = 'verisk' then (sum("value") / 10000.0)
            else null
        end as "value"
    from {{ ref('ylt_all_factors_long_from_cachetbl') }}
    group by
        1, 2, 3, 4, 5, 6, 7, 8
),
all_ep as (
    select * from loss_per_year
    union all
    select * from max_loss_per_year
    union all
    select * from avg_annual_loss
)
select
    case
        when rnk = 0 then 0
        when base_model = 'risklink' then (100000 / rnk)
        when base_model = 'verisk' then (10000 / rnk)
        else null
    end as rp,
    * exclude (yearid)
from all_ep
where
    (base_model = 'verisk' and rnk in (0, 1, 10, 50, 100, 200, 500))
    or (base_model = 'risklink' and rnk in (0, 10, 100, 500, 1000, 2000, 5000))
