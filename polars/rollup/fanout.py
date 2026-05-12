"""Hisco fanout projections."""

from __future__ import annotations

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.validate import validate_schema
from rollup.variants import VariantSpec


def fanout_hisco(
    all_factors: pl.LazyFrame,
    variant: VariantSpec,
    *,
    min_loss: float = 0.0,
) -> pl.LazyFrame:
    """Project all_factors into one Hisco fanout variant."""
    out = (
        all_factors
        .filter(pl.col(AF.BASE_MODEL) == variant.vendor.name)
        .select(
            pl.col(AF.MODEL_EVENT_ID).alias(H.MODEL_EVENT_ID),
            pl.col(AF.YEAR_ID).alias(H.MODEL_YEAR),
            pl.col(AF.REQUIRED_CURRENCY).alias(H.CURRENCY_CODE),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_YOA),
            pl.col(variant.loss_metric).alias(H.MODEL_GROSS_LOSS),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_INWARDS_REINSTATEMENT),
            pl.lit(0, dtype=pl.Int64).alias(H.MODEL_EVENT_DAY),
            pl.col(AF.CDS_CAT_CLASS_NAME).alias(H.LOSS_CLASS_NAME),
        )
    )
    if min_loss > 0:
        out = out.filter(pl.col(H.MODEL_GROSS_LOSS) >= min_loss)
    validate_schema(out, F.HISCO_FANOUT, name=f"fanout.{variant.name}")
    return out
