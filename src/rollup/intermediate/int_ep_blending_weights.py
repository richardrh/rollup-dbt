from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "int_ep_blending_weights"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.region_peril_id: pl.Int64,
            Col.blend_subregion_peril_id: pl.String,
            Col.sub_region_peril: pl.String,
            Col.verisk_weight: pl.Float64,
            Col.risklink_weight: pl.Float64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(seeds: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    if "blending_factors" not in seeds:
        raise ValueError(
            f"{MODEL}: input 'seeds' missing required key 'blending_factors'"
        )
    blending_factors = seeds["blending_factors"]
    frame = blending_factors.select(
        pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
        pl.col(RawCol.SubRegionPerilID)
        .cast(pl.String)
        .alias(Col.blend_subregion_peril_id),
        pl.col(RawCol.SubRegionPeril).cast(pl.String).alias(Col.sub_region_peril),
        pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
        pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
    )
    validate(frame)
    return frame
