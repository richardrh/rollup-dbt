from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_ep_vendor_joined"
JOIN_KEYS = [
    Col.rollup_lob,
    Col.rollup_peril,
    Col.region_peril_id,
    Col.blend_subregion_peril_id,
    Col.base_model,
    Col.ep_type,
    Col.return_period,
]


def validate(ep_selected_main: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "ep_selected_main", ep_selected_main)
    require_columns(MODEL, "ep_selected_main", schema, [Col.vendor, *JOIN_KEYS])
    require_dtype_family(MODEL, "ep_selected_main", schema, Col.loss, "numeric")


def transform(ep_selected_main: pl.LazyFrame) -> pl.LazyFrame:
    validate(ep_selected_main)
    verisk = (
        ep_selected_main.filter(pl.col(Col.vendor) == "verisk")
        .group_by(JOIN_KEYS)
        .agg(pl.col(Col.loss).sum().alias(Col.verisk_loss))
    )
    risklink = (
        ep_selected_main.filter(pl.col(Col.vendor) == "risklink")
        .group_by(JOIN_KEYS)
        .agg(pl.col(Col.loss).sum().alias(Col.risklink_loss))
    )
    frame = risklink.join(verisk, on=JOIN_KEYS, how="full", coalesce=True)
    validate_output(MODEL, frame)
    return frame
