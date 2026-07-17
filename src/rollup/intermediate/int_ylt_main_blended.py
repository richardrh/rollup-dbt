from __future__ import annotations
import polars as pl
from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
)

MODEL = "int_ylt_main_blended"


def validate(ylt: pl.LazyFrame, ep_blending_targets: pl.LazyFrame) -> None:
    ylt_schema = collect_lazy_schema(MODEL, "ylt", ylt)
    require_columns(
        MODEL,
        "ylt",
        ylt_schema,
        [
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.rp_bucket,
            Col.base_model,
        ],
    )
    require_dtype_family(MODEL, "ylt", ylt_schema, Col.loss, "numeric")
    schema = collect_lazy_schema(MODEL, "ep_blending_targets", ep_blending_targets)
    require_columns(
        MODEL,
        "ep_blending_targets",
        schema,
        [
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.return_period,
            Col.ep_type,
            Col.risklink_loss,
            Col.verisk_loss,
            Col.risklink_blended_contribution,
            Col.verisk_blended_contribution,
            Col.target_loss,
            Col.base_model,
            Col.base_model_loss,
            Col.uplift_factor_on_base_model,
        ],
    )
    require_dtype_family(
        MODEL, "ep_blending_targets", schema, Col.uplift_factor_on_base_model, "numeric"
    )
    require_dtype_family(
        MODEL, "ep_blending_targets", schema, Col.return_period, "numeric"
    )
    require_dtype_family(MODEL, "ylt", ylt_schema, Col.rp_bucket, "numeric")
    require_join_key_compatible(
        MODEL,
        "ylt",
        ylt_schema,
        "ep_blending_targets",
        schema,
        [
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
        ],
    )


def transform(ylt: pl.LazyFrame, ep_blending_targets: pl.LazyFrame) -> pl.LazyFrame:
    validate(ylt, ep_blending_targets)
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
    diagnostic_cols = [
        Col.risklink_blended_contribution,
        Col.verisk_blended_contribution,
        Col.uplift_factor_on_base_model,
    ]
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
        .select(pl.all().exclude(diagnostic_cols), *diagnostic_cols)
    )
    validate_output(MODEL, frame)
    return frame
