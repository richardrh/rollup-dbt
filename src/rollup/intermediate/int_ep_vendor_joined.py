from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

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


def schema() -> pl.Schema:
    return pl.Schema(
        [
            (str(Col.rollup_lob), pl.String),
            (str(Col.rollup_peril), pl.String),
            (str(Col.region_peril_id), pl.Int64),
            (str(Col.blend_subregion_peril_id), pl.String),
            (str(Col.base_model), pl.String),
            (str(Col.ep_type), pl.String),
            (str(Col.return_period), pl.Int64),
            (str(Col.risklink_loss), pl.Float64),
            (str(Col.verisk_loss), pl.Float64),
        ]
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(ep_selected_main: pl.LazyFrame) -> pl.LazyFrame:
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
    frame = risklink.join(verisk, on=JOIN_KEYS, how="full", coalesce=True).select(
        pl.col(Col.rollup_lob).cast(pl.String),
        pl.col(Col.rollup_peril).cast(pl.String),
        pl.col(Col.region_peril_id).cast(pl.Int64),
        pl.col(Col.blend_subregion_peril_id).cast(pl.String),
        pl.col(Col.base_model).cast(pl.String),
        pl.col(Col.ep_type).cast(pl.String),
        pl.col(Col.return_period).cast(pl.Int64),
        pl.col(Col.risklink_loss).cast(pl.Float64),
        pl.col(Col.verisk_loss).cast(pl.Float64),
    )
    validate(frame)
    return frame
