from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame, float]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {
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
                Col.model_event_id: pl.Int64,
                Col.event_day: pl.Int64,
                "_euws_factor_raw": pl.Float64,
                "_localccy_forecast_loss": pl.Float64,
                Col.output_use: pl.String,
            }
        )

    @override
    @classmethod
    def _transform(cls, ylt: pl.LazyFrame, threshold: float) -> pl.LazyFrame:
        threshold_predicate = (
            pl.col(Col.loss).is_not_null()
            if threshold <= 0
            else pl.col(Col.loss) >= threshold
        )
        return (
            ylt.filter((pl.col(Col.metric) != "euws_override") | threshold_predicate)
            .with_columns(
                pl.when(pl.col(Col.metric) == "euws_override")
                .then(pl.lit("cds_main"))
                .otherwise(pl.lit("intermediate_audit"))
                .alias(Col.output_use)
            )
            .select(
                Col.vendor,
                Col.analysis_id,
                Col.modelled_lob,
                Col.modelled_peril,
                Col.rollup_lob,
                Col.rollup_peril,
                Col.region_peril_id,
                Col.blend_subregion_peril_id,
                Col.base_model,
                Col.selection_priority,
                Col.is_dialsup,
                Col.is_euws,
                Col.cds_cat_class_name,
                Col.class_,
                Col.office,
                Col.currency,
                Col.model_code,
                Col.year_id,
                Col.event_id,
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
                pl.col(Col.model_event_id).cast(pl.Int64),
                pl.col(Col.event_day).cast(pl.Int64),
                pl.col("_euws_factor_raw").cast(pl.Float64),
                pl.col("_localccy_forecast_loss").cast(pl.Float64),
                Col.output_use,
            )
        )
