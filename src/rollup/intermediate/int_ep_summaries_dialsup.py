from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
)

MODEL = "int_ep_summaries_dialsup"


def validate(enriched: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "enriched", enriched)
    require_columns(
        MODEL,
        "enriched",
        schema,
        [
            Col.vendor,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.modelled_peril,
            Col.is_dialsup,
        ],
    )


def transform(enriched: pl.LazyFrame) -> pl.LazyFrame:
    validate(enriched)
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected = (
        enriched.filter(pl.col(Col.is_dialsup) == 1)
        .select(*selection_keys, Col.modelled_peril)
        .unique()
    )
    frame = enriched.join(
        selected, on=[*selection_keys, Col.modelled_peril], how="inner"
    )
    validate_output(MODEL, frame)
    return frame
