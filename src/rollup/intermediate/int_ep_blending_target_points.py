from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
)

MODEL = "int_ep_blending_target_points"


def validate(joined_ep_summaries: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "joined_ep_summaries", joined_ep_summaries)
    require_columns(
        MODEL, "joined_ep_summaries", schema, [Col.ep_type, Col.return_period]
    )


def transform(
    joined_ep_summaries: pl.LazyFrame, config: RollupConfig | None = None
) -> pl.LazyFrame:
    validate(joined_ep_summaries)
    config = config or RollupConfig()
    target_predicate = pl.lit(False)
    for point in config.blending.target_points:
        target_predicate = target_predicate | (
            (pl.col(Col.ep_type) == point.ep_type)
            & (pl.col(Col.return_period) == point.return_period)
        )
    frame = joined_ep_summaries.filter(target_predicate)
    validate_output(MODEL, frame)
    return frame
