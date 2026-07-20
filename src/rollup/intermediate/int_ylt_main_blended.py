from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_ylt_main_blended"


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
            Col.risklink_blended_contribution: pl.Float64,
            Col.verisk_blended_contribution: pl.Float64,
            Col.uplift_factor_on_base_model: pl.Float64,
        }
    )  # type: ignore[arg-type]


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(ylt: pl.LazyFrame, ep_blending_targets: pl.LazyFrame) -> pl.LazyFrame:
    factors = ep_blending_targets.select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        pl.col(Col.return_period).alias(Col.rp_bucket),
        Col.ep_type,
        Col.risklink_loss,
        Col.verisk_loss,
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.target_loss,
        Col.base_model,
        Col.base_model_loss,
        Col.uplift_factor_on_base_model,
    )
    frame = (
        ylt.join(
            factors,
            on=[
                Col.rollup_lob,
                Col.rollup_peril,
                Col.region_peril_id,
                Col.blend_subregion_peril_id,
                Col.rp_bucket,
                Col.base_model,
            ],
            how="inner",
        )
        .with_columns(
            (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias(
                Col.loss
            ),
            pl.lit("blended").alias(Col.metric),
        )
        .drop(
            Col.ep_type,
            Col.risklink_loss,
            Col.verisk_loss,
            Col.target_loss,
            Col.base_model_loss,
            strict=False,
        )
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
        )
    )
    validate(frame)
    return frame
