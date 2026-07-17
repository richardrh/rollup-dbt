from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    validate_mapping_key,
)

MODEL = "int_ep_blending_weights"


def validate(seeds: dict[str, pl.LazyFrame]) -> None:
    validate_mapping_key(MODEL, "seeds", seeds, "blending_factors")
    schema = collect_lazy_schema(
        MODEL, "seeds.blending_factors", seeds["blending_factors"]
    )
    require_columns(
        MODEL,
        "seeds.blending_factors",
        schema,
        [RawCol.RegionPerilID, RawCol.SubRegionPerilID, RawCol.SubRegionPeril],
    )
    require_dtype_family(
        MODEL, "seeds.blending_factors", schema, RawCol.AIRBlend, "numeric"
    )
    require_dtype_family(
        MODEL, "seeds.blending_factors", schema, RawCol.RMSBlend, "numeric"
    )


def transform(seeds: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    validate(seeds)
    blending_factors = seeds["blending_factors"]
    frame = blending_factors.select(
        pl.col(RawCol.RegionPerilID).alias(Col.region_peril_id),
        pl.col(RawCol.SubRegionPerilID).alias(Col.blend_subregion_peril_id),
        pl.col(RawCol.SubRegionPeril).alias(Col.sub_region_peril),
        pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
        pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
    )
    validate_output(MODEL, frame)
    return frame
