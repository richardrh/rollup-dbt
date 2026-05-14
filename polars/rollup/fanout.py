"""Hisco fanout projections."""

from __future__ import annotations

import polars as pl

from rollup.config import VendorName
from rollup.schemas import frames as F
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import HiscoFanoutCol as H
from rollup.schemas.columns import RefRisklinkEventsCol as RLE
from rollup.validate import validate_schema
from rollup.variants import VariantSpec


def fanout_hisco(
    all_factors: pl.LazyFrame,
    variant: VariantSpec,
    *,
    min_loss: float = 0.0,
    risklink_events: pl.LazyFrame | None = None,
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
            pl.col(AF.CDS_CAT_CLASS_NAME).alias(H.LOSS_CLASS_NAME),
        )
    )
    if variant.vendor.name == VendorName.RISKLINK and risklink_events is not None:
        # January's RiskLink with-day-id fanout used an INNER JOIN on
        # (ModelEventID, ModelYear) to attach day-of-year from flood_rl22_model_events.
        # Rows without a catalogue occurrence are intentionally excluded.
        event_days = risklink_events.select(
            pl.col(RLE.EVENT_ID).alias(H.MODEL_EVENT_ID),
            pl.col(RLE.YEAR).alias(H.MODEL_YEAR),
            pl.col(RLE.DAY).alias(H.MODEL_EVENT_DAY),
        )
        out = out.join(event_days, on=[H.MODEL_EVENT_ID, H.MODEL_YEAR], how="inner")
    else:
        out = out.with_columns(pl.lit(0, dtype=pl.Int64).alias(H.MODEL_EVENT_DAY))

    if min_loss > 0:
        out = out.filter(pl.col(H.MODEL_GROSS_LOSS) >= min_loss)
    out = out.select(
        H.MODEL_EVENT_ID,
        H.MODEL_YEAR,
        H.CURRENCY_CODE,
        H.MODEL_YOA,
        H.MODEL_GROSS_LOSS,
        H.MODEL_INWARDS_REINSTATEMENT,
        H.MODEL_EVENT_DAY,
        H.LOSS_CLASS_NAME,
    )
    validate_schema(out, F.HISCO_FANOUT, name=f"fanout.{variant.name}")
    return out
