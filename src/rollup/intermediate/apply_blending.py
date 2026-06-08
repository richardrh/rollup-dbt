from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol


def apply_blending(enriched: pl.LazyFrame, blending: pl.DataFrame) -> pl.LazyFrame:
    if blending.is_empty():
        return enriched.with_columns(pl.col(Col.loss).alias("blended_loss"))
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    air_col = RawCol.AIRBlend if RawCol.AIRBlend in columns else "verisk_weight"
    rms_col = RawCol.RMSBlend if RawCol.RMSBlend in columns else "risklink_weight"
    weights = blending.lazy().select(
        pl.col(region_col).cast(pl.Int64).alias(Col.region_peril_id),
        _optional_weight(air_col, columns).alias("verisk_blend_weight"),
        _optional_weight(rms_col, columns).alias("risklink_blend_weight"),
    )
    return enriched.join(weights, on=Col.region_peril_id, how="left").with_columns(
        pl.when(pl.col(Col.vendor) == "verisk")
        .then(pl.col("verisk_blend_weight"))
        .otherwise(pl.col("risklink_blend_weight"))
        .fill_null(1.0)
        .alias("blend_weight")
    ).with_columns((pl.col(Col.loss) * pl.col("blend_weight")).alias("blended_loss"))


def _optional_weight(column: str, columns: list[str]) -> pl.Expr:
    if column in columns:
        return pl.col(column).cast(pl.Float64).fill_null(1.0)
    return pl.lit(1.0)
