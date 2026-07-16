from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
)

MODEL = "int_ep_summaries_main"


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
            Col.selection_priority,
        ],
    )


def transform(enriched: pl.LazyFrame) -> pl.LazyFrame:
    validate(enriched)
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_modelled_perils = (
        enriched.select(*selection_keys, Col.modelled_peril, Col.selection_priority)
        .sort([*selection_keys, Col.selection_priority, Col.modelled_peril])
        .unique(subset=selection_keys, keep="first", maintain_order=True)
        .select(*selection_keys, Col.modelled_peril)
    )
    frame = enriched.join(
        selected_modelled_perils, on=[*selection_keys, Col.modelled_peril], how="inner"
    )
    validate_output(MODEL, frame)
    return frame
