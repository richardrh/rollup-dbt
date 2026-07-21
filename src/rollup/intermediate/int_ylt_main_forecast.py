from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame, pl.LazyFrame, pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
                Col.vendor: pl.String,
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
                Col.risklink_blended_contribution: pl.Float64,
                Col.verisk_blended_contribution: pl.Float64,
                Col.uplift_factor_on_base_model: pl.Float64,
                Col.target_currency: pl.String,
                Col.forecast_date: pl.Date,
            }
        )

    @override
    @classmethod
    def _transform(
        cls,
        ylt_localccy: pl.LazyFrame,
        forecast_dates: pl.LazyFrame,
        forecast_factors: pl.LazyFrame,
    ) -> pl.LazyFrame:
        frame = (
            ylt_localccy.join(forecast_dates, how="cross")
            .join(
                forecast_factors,
                on=[Col.class_, Col.office, Col.forecast_date],
                how="left",
            )
            .with_columns(
                (
                    pl.col(Col.loss) * pl.col("_forecast_factor_raw").fill_null(1.0)
                ).alias(Col.loss),
                pl.lit("localccy_forecast").alias(Col.metric),
            )
            .drop("_forecast_factor_raw")
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
                pl.col(Col.risklink_blended_contribution).cast(pl.Float64),
                pl.col(Col.verisk_blended_contribution).cast(pl.Float64),
                pl.col(Col.uplift_factor_on_base_model).cast(pl.Float64),
                Col.target_currency,
                pl.col(Col.forecast_date).cast(pl.Date),
            )
        )
        return frame
