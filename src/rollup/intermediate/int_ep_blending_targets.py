from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
    require_join_key_compatible,
)

MODEL = "int_ep_blending_targets"


def validate(
    target_points: pl.LazyFrame,
    weights: pl.LazyFrame,
) -> None:
    target_schema = collect_lazy_schema(MODEL, "target_points", target_points)
    weight_schema = collect_lazy_schema(MODEL, "weights", weights)
    require_columns(
        MODEL,
        "target_points",
        target_schema,
        [
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.risklink_loss,
            Col.verisk_loss,
        ],
    )
    require_columns(
        MODEL,
        "weights",
        weight_schema,
        [
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.risklink_weight,
            Col.verisk_weight,
        ],
    )
    require_join_key_compatible(
        MODEL,
        "target_points",
        target_schema,
        "weights",
        weight_schema,
        [Col.region_peril_id, Col.blend_subregion_peril_id],
    )
    for input_name, schema in {
        "target_points": target_schema,
        "weights": weight_schema,
    }.items():
        for column in (
            [Col.risklink_loss, Col.verisk_loss]
            if input_name == "target_points"
            else [Col.risklink_weight, Col.verisk_weight]
        ):
            require_dtype_family(MODEL, input_name, schema, column, "numeric")


def transform(
    target_points: pl.LazyFrame,
    weights: pl.LazyFrame,
    config: RollupConfig | None = None,
) -> pl.LazyFrame:
    validate(target_points, weights)
    config = config or RollupConfig()
    frame = (
        target_points.join(
            weights, on=[Col.region_peril_id, Col.blend_subregion_peril_id], how="left"
        )
        .with_columns(
            pl.when(pl.col(Col.base_model) == "risklink")
            .then(pl.col(Col.risklink_loss))
            .otherwise(pl.col(Col.verisk_loss))
            .alias(Col.base_model_loss)
        )
        .filter(pl.col(Col.base_model_loss).is_not_null())
        .with_columns(
            (
                pl.col(Col.risklink_loss).is_not_null()
                & pl.col(Col.verisk_loss).is_not_null()
            ).alias("_has_both_vendor_losses")
        )
        .with_columns(
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(pl.col(Col.risklink_loss) * pl.col(Col.risklink_weight))
            .when(pl.col(Col.base_model) == "risklink")
            .then(pl.col(Col.base_model_loss))
            .otherwise(pl.lit(0.0))
            .alias(Col.risklink_blended_contribution),
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(pl.col(Col.verisk_loss) * pl.col(Col.verisk_weight))
            .when(pl.col(Col.base_model) == "verisk")
            .then(pl.col(Col.base_model_loss))
            .otherwise(pl.lit(0.0))
            .alias(Col.verisk_blended_contribution),
        )
        .with_columns(
            pl.when(pl.col("_has_both_vendor_losses"))
            .then(
                pl.col(Col.risklink_blended_contribution)
                + pl.col(Col.verisk_blended_contribution)
            )
            .otherwise(pl.col(Col.base_model_loss))
            .alias(Col.target_loss)
        )
        .with_columns(
            (pl.col(Col.target_loss) / pl.col(Col.base_model_loss)).alias(
                Col.uplift_factor_on_base_model
            )
        )
        .with_columns(
            pl.col(Col.uplift_factor_on_base_model)
            .clip(
                lower_bound=config.blending.uplift_factor_min,
                upper_bound=config.blending.uplift_factor_max,
            )
            .alias(Col.uplift_factor_on_base_model)
        )
        .drop("_has_both_vendor_losses")
    )
    validate_output(MODEL, frame)
    return frame
