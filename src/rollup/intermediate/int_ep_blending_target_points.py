from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col
from rollup.config import RollupConfig

MODEL = "int_ep_blending_target_points"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.rollup_lob: pl.String,
            Col.rollup_peril: pl.String,
            Col.region_peril_id: pl.Int64,
            Col.blend_subregion_peril_id: pl.String,
            Col.base_model: pl.String,
            Col.ep_type: pl.String,
            Col.return_period: pl.Int64,
            Col.risklink_loss: pl.Float64,
            Col.verisk_loss: pl.Float64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(
    joined_ep_summaries: pl.LazyFrame, config: RollupConfig | None = None
) -> pl.LazyFrame:
    config = config or RollupConfig()
    target_predicate = pl.lit(False)
    for point in config.blending.target_points:
        target_predicate = target_predicate | (
            (pl.col(Col.ep_type) == point.ep_type)
            & (pl.col(Col.return_period) == point.return_period)
        )
    frame = joined_ep_summaries.filter(target_predicate).select(
        Col.rollup_lob,
        Col.rollup_peril,
        pl.col(Col.region_peril_id).cast(pl.Int64),
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.ep_type,
        pl.col(Col.return_period).cast(pl.Int64),
        pl.col(Col.risklink_loss).cast(pl.Float64),
        pl.col(Col.verisk_loss).cast(pl.Float64),
    )
    validate(frame)
    return frame
