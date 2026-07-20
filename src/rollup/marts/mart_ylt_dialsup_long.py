from __future__ import annotations
import polars as pl
from rollup.model_validation import validate_schema
from rollup.columns import Col

MODEL = "mart_ylt_dialsup_long"


def schema() -> pl.Schema:
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
            Col.model_event_id: pl.Int64,
            Col.event_day: pl.Int64,
            Col.target_currency: pl.String,
            Col.fx_rate_date: pl.String,
            Col.fx_rate: pl.Float64,
            Col.forecast_date: pl.Date,
            Col.output_use: pl.String,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(ylt_dialsup: pl.LazyFrame, threshold: float) -> pl.LazyFrame:
    threshold_predicate = (
        pl.col(Col.loss).is_not_null()
        if threshold <= 0
        else pl.col(Col.loss) >= threshold
    )
    frame = (
        ylt_dialsup.filter(
            (pl.col(Col.metric) == "dialsup_localccy_forecast") & threshold_predicate
        )
        .with_columns(pl.lit("cds_dialsup").alias(Col.output_use))
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
            pl.col(Col.model_event_id).cast(pl.Int64),
            pl.col(Col.event_day).cast(pl.Int64),
            Col.target_currency,
            Col.fx_rate_date,
            pl.col(Col.fx_rate).cast(pl.Float64),
            pl.col(Col.forecast_date).cast(pl.Date),
            Col.output_use,
        )
    )
    validate(frame)
    return frame
