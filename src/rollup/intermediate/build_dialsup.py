from __future__ import annotations

import polars as pl

from rollup.columns import Col


def build_dialsup(adjusted: pl.LazyFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    return adjusted.filter(pl.col(Col.is_dialsup) == 1).select(
        Col.vendor,
        Col.base_model,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.target_currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        pl.lit(dialsup_metric(target_currency)).alias(Col.metric),
        (pl.col(Col.loss) * pl.col(Col.fx_rate) * pl.col(Col.forecast_factor))
        .cast(pl.Float64)
        .alias(Col.loss),
    )


def dialsup_metric(target_currency: str) -> str:
    return f"loss_dialsup_fx_{str(target_currency).upper().lower()}_forecast"
