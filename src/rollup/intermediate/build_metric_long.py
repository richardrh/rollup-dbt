from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.metrics import metric_specs


def build_metric_long(adjusted: pl.LazyFrame, target_currency: str = "GBP") -> pl.LazyFrame:
    metric_columns = [
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
    ]

    base = adjusted.select(
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
        Col.loss,
        "blended_loss",
        "fx_loss",
        "forecast_loss",
        "euws_loss",
    )
    metric_long = pl.concat(
        [
            base.select(
                *metric_columns,
                pl.lit(spec.name).alias(Col.metric),
                pl.col(spec.loss_column).cast(pl.Float64).alias(Col.loss),
            )
            for spec in metric_specs(target_currency)
        ],
        how="vertical",
    )
    return metric_long
