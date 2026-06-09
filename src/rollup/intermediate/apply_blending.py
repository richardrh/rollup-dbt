from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.intermediate.build_enriched_ylt import ENRICHED_YLT_OUTPUT_SCHEMA
from rollup.schemas import require_columns


BLENDING_INPUT_SCHEMA = ENRICHED_YLT_OUTPUT_SCHEMA
BLENDING_FACTORS_SCHEMA = pl.Schema({Col.region_peril_id: pl.Int64})
RAW_BLENDING_FACTORS_SCHEMA = pl.Schema({RawCol.RegionPerilID: pl.Int64})
BLENDED_YLT_SCHEMA = pl.Schema(
    {
        **BLENDING_INPUT_SCHEMA,
        "blended_loss": pl.Float64,
    }
)


def apply_blending(enriched: pl.LazyFrame, blending: pl.DataFrame) -> pl.LazyFrame:
    require_columns(enriched, BLENDING_INPUT_SCHEMA)

    if blending.is_empty():
        blended = enriched.with_columns(pl.col(Col.loss).alias("blended_loss"))
        require_columns(blended, BLENDED_YLT_SCHEMA)
        return blended
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    factor_schema = (
        RAW_BLENDING_FACTORS_SCHEMA if RawCol.RegionPerilID in columns else BLENDING_FACTORS_SCHEMA
    )
    require_columns(blending, factor_schema, check_dtypes=False)
    air_col = RawCol.AIRBlend if RawCol.AIRBlend in columns else "verisk_weight"
    rms_col = RawCol.RMSBlend if RawCol.RMSBlend in columns else "risklink_weight"
    verisk_blend_weight = (
        pl.col(air_col).cast(pl.Float64).fill_null(1.0) if air_col in columns else pl.lit(1.0)
    )
    risklink_blend_weight = (
        pl.col(rms_col).cast(pl.Float64).fill_null(1.0) if rms_col in columns else pl.lit(1.0)
    )
    weights = blending.lazy().select(
        pl.col(region_col).cast(pl.Int64).alias(Col.region_peril_id),
        verisk_blend_weight.alias("verisk_blend_weight"),
        risklink_blend_weight.alias("risklink_blend_weight"),
    )
    blended = enriched.join(weights, on=Col.region_peril_id, how="left").with_columns(
        pl.when(pl.col(Col.vendor) == "verisk")
        .then(pl.col("verisk_blend_weight"))
        .otherwise(pl.col("risklink_blend_weight"))
        .fill_null(1.0)
        .alias("blend_weight")
    ).with_columns((pl.col(Col.loss) * pl.col("blend_weight")).alias("blended_loss"))
    require_columns(blended, BLENDED_YLT_SCHEMA)
    return blended
