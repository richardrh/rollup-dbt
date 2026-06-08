from __future__ import annotations

import polars as pl

from rollup.intermediate.apply_blending import apply_blending
from rollup.intermediate.apply_euws import apply_euws
from rollup.intermediate.apply_forecast import apply_forecast
from rollup.intermediate.apply_fx import apply_fx
from rollup.staging import StagingFrames


def apply_adjustments(enriched: pl.LazyFrame, frames: StagingFrames) -> pl.LazyFrame:
    with_blending = apply_blending(enriched, frames.blending)
    with_fx = apply_fx(with_blending, frames.fx_rates)
    with_forecast = apply_forecast(with_fx, frames.forecast_factors)
    return apply_euws(with_forecast, frames.euws_factors)
