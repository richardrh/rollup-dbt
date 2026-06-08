from __future__ import annotations

import polars as pl

from rollup.columns import Col


def build_metric_long(adjusted: pl.LazyFrame) -> pl.LazyFrame:
    base = adjusted.select(
        Col.vendor,
        pl.col(Col.vendor).alias(Col.base_model),
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        Col.loss,
        "blended_loss",
        "gbp_loss",
        "forecast_loss",
        "euws_loss",
    )
    return pl.concat(
        [
            _metric(base, Col.loss, "original_ylt_loss"),
            _metric(base, "blended_loss", "blended"),
            _metric(base, "gbp_loss", "gbp"),
            _metric(base, "forecast_loss", "forecast"),
            _metric(base, "euws_loss", "euws_override"),
        ],
        how="vertical",
    )


def _metric(frame: pl.LazyFrame, source_col: str, metric: str) -> pl.LazyFrame:
    return frame.select(
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
        Col.year_id,
        Col.event_id,
        Col.forecast_date,
        Col.is_dialsup,
        pl.lit(metric).alias(Col.metric),
        pl.col(source_col).cast(pl.Float64).alias(Col.loss),
    )
