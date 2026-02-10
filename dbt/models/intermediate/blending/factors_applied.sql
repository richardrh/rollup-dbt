{{ config(materialized='view') }}

with vendor_losses as (
    select
        *
from {{ ref('vendor_proportions_all_rps') }}
    where
        ep_type = 'AAL'
        or (ep_type in ('OEP', 'AEP') and rp in (200, 1000, 10000))
    order by risklink_loss desc
)
select
    losses.*,
    bf.RegionPeril,
    bf.SubRegionPeril,
    bf.AIRBlend,
    bf.RMSBlend,
    (coalesce(risklink_loss, 1) * RMSBlend) as risklink_blended_contribution,
    (coalesce(verisk_loss, 1) * AIRBlend) as verisk_blended_contribution,
    (risklink_blended_contribution + verisk_blended_contribution) as blended_target_loss,
    case
        when rollup_region_peril in ('EU_FL', 'UK_FL') then 'risklink'
        else 'verisk'
    end as base_model,
    case
        when base_model = 'risklink' then losses.risklink_loss
        else losses.verisk_loss
    end as base_model_loss,
    cast(coalesce((blended_target_loss / base_model_loss)) as float) as uplift_factor_on_base_model,
    cast(greatest(0.1, least(10.0, uplift_factor_on_base_model)) as float) as uplift_factor_on_base_model_capped
from vendor_losses as losses
inner join {{ source('reference', 'blending_factors') }} as bf
    on bf.RegionPerilID = losses.blending_factor_region_peril_id
    and bf.SubRegionPerilID = losses.blending_factor_sub_region_peril_id
