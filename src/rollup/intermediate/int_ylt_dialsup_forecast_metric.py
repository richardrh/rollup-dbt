from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {
                str(Col.vendor): pl.String,
                Col.analysis_id: pl.String,
                Col.modelled_lob: pl.String,
                Col.modelled_peril: pl.String,
                Col.rollup_lob: pl.String,
                Col.rollup_peril: pl.String,
                Col.region_peril_id: pl.Int64,
                Col.blend_subregion_peril_id: pl.String,
                Col.base_model: pl.String,
                Col.selection_priority: pl.Int64,
                Col.is_dialsup: pl.Int64,
                Col.is_euws: pl.Int64,
                Col.cds_cat_class_name: pl.String,
                Col.class_: pl.String,
                Col.office: pl.String,
                Col.currency: pl.String,
                Col.model_code: pl.Int64,
                Col.year_id: pl.Int64,
                Col.event_id: pl.Int64,
                Col.loss: pl.Float64,
                Col.metric: pl.String,
                Col.rnk: pl.Int64,
                Col.rp: pl.Float64,
                Col.rp_bucket: pl.Int32,
                Col.model_event_id: pl.Int64,
                Col.event_day: pl.Int64,
                Col.target_currency: pl.String,
                Col.fx_rate_date: pl.String,
                Col.fx_rate: pl.Float64,
                Col.forecast_date: pl.Date,
            }
        )

    @override
    @classmethod
    def _transform(cls, ylt_with_factors: pl.LazyFrame) -> pl.LazyFrame:
        frame = (
            ylt_with_factors.with_columns(
                (
                    pl.col(Col.loss) / pl.col(Col.fx_rate) * pl.col("_forecast_factor")
                ).alias(Col.loss),
                pl.col(Col.currency).alias(Col.target_currency),
                pl.lit("dialsup_localccy_forecast").alias(Col.metric),
            )
            .drop("_forecast_factor_raw", "_forecast_factor")
            .select(
                Col.vendor,
                Col.analysis_id,
                Col.modelled_lob,
                Col.modelled_peril,
                Col.rollup_lob,
                Col.rollup_peril,
                pl.col(Col.region_peril_id).cast(pl.Int64),
                Col.blend_subregion_peril_id,
                Col.base_model,
                pl.col(Col.selection_priority).cast(pl.Int64),
                pl.col(Col.is_dialsup).cast(pl.Int64),
                pl.col(Col.is_euws).cast(pl.Int64),
                Col.cds_cat_class_name,
                Col.class_,
                Col.office,
                Col.currency,
                pl.col(Col.model_code).cast(pl.Int64),
                pl.col(Col.year_id).cast(pl.Int64),
                pl.col(Col.event_id).cast(pl.Int64),
                pl.col(Col.loss).cast(pl.Float64),
                Col.metric,
                pl.col(Col.rnk).cast(pl.Int64),
                pl.col(Col.rp).cast(pl.Float64),
                pl.col(Col.rp_bucket).cast(pl.Int32),
                pl.col(Col.model_event_id).cast(pl.Int64),
                pl.col(Col.event_day).cast(pl.Int64),
                Col.target_currency,
                Col.fx_rate_date,
                pl.col(Col.fx_rate).cast(pl.Float64),
                pl.col(Col.forecast_date).cast(pl.Date),
            )
        )
        return frame
