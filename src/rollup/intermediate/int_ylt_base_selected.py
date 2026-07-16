from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model_validation import (
    collect_lazy_schema,
    require_columns,
    require_dtype_family,
    validate_output,
)

MODEL = "int_ylt_base_selected"


def validate(enriched_ylt: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "enriched_ylt", enriched_ylt)
    require_columns(MODEL, "enriched_ylt", schema, [Col.vendor, Col.base_model])
    require_dtype_family(MODEL, "enriched_ylt", schema, Col.loss, "numeric")


def transform(
    enriched_ylt: pl.LazyFrame, config: RollupConfig | None = None
) -> pl.LazyFrame:
    validate(enriched_ylt)
    config = config or RollupConfig()
    frame = enriched_ylt.with_columns(pl.lit("original").alias(Col.metric)).filter(
        (pl.col(Col.vendor) == pl.col(Col.base_model))
        & (pl.col(Col.loss) >= config.outputs.minimum_event_loss_threshold / 5)
    )
    validate_output(MODEL, frame)
    return frame
