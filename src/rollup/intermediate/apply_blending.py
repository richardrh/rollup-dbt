from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.build_enriched_ylt import ENRICHED_YLT_OUTPUT_SCHEMA


BLENDING_INPUT_SCHEMA = ENRICHED_YLT_OUTPUT_SCHEMA
BLENDING_FACTORS_SCHEMA = pa.DataFrameSchema(
    {Col.region_peril_id: pa.Column(pl.Int64, nullable=True)},
    strict=False,
)
RAW_BLENDING_FACTORS_SCHEMA = pa.DataFrameSchema(
    {RawCol.RegionPerilID: pa.Column(pl.Int64, nullable=True)},
    strict=False,
)
BLENDED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **BLENDING_INPUT_SCHEMA.columns,
        "blended_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)


def apply_blending(enriched: pl.LazyFrame, blending: pl.DataFrame) -> pl.LazyFrame:
    BLENDING_INPUT_SCHEMA.validate(enriched)

    if blending.is_empty():
        return enriched.with_columns(pl.col(Col.loss).alias("blended_loss"))
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    factor_schema = (
        RAW_BLENDING_FACTORS_SCHEMA if RawCol.RegionPerilID in columns else BLENDING_FACTORS_SCHEMA
    )
    factor_schema.validate(blending)
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
    return blended
