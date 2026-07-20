from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col
from rollup.config import RollupConfig

MODEL = "int_ep_blending_targets"


def schema() -> pl.Schema:
    return pl.Schema(
        [
            (str(Col.rollup_lob), pl.String),
            (str(Col.rollup_peril), pl.String),
            (str(Col.region_peril_id), pl.Int64),
            (str(Col.blend_subregion_peril_id), pl.String),
            (str(Col.base_model), pl.String),
            (str(Col.ep_type), pl.String),
            (str(Col.return_period), pl.Int64),
            (str(Col.risklink_loss), pl.Float64),
            (str(Col.verisk_loss), pl.Float64),
            (str(Col.sub_region_peril), pl.String),
            (str(Col.verisk_weight), pl.Float64),
            (str(Col.risklink_weight), pl.Float64),
            (str(Col.base_model_loss), pl.Float64),
            (str(Col.risklink_blended_contribution), pl.Float64),
            (str(Col.verisk_blended_contribution), pl.Float64),
            (str(Col.target_loss), pl.Float64),
            (str(Col.uplift_factor_on_base_model), pl.Float64),
        ]
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(
    target_points: pl.LazyFrame,
    weights: pl.LazyFrame,
    config: RollupConfig | None = None,
) -> pl.LazyFrame:
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
        .select(
            pl.col(Col.rollup_lob).cast(pl.String),
            pl.col(Col.rollup_peril).cast(pl.String),
            pl.col(Col.region_peril_id).cast(pl.Int64),
            pl.col(Col.blend_subregion_peril_id).cast(pl.String),
            pl.col(Col.base_model).cast(pl.String),
            pl.col(Col.ep_type).cast(pl.String),
            pl.col(Col.return_period).cast(pl.Int64),
            pl.col(Col.risklink_loss).cast(pl.Float64),
            pl.col(Col.verisk_loss).cast(pl.Float64),
            pl.col(Col.sub_region_peril).cast(pl.String),
            pl.col(Col.verisk_weight).cast(pl.Float64),
            pl.col(Col.risklink_weight).cast(pl.Float64),
            pl.col(Col.base_model_loss).cast(pl.Float64),
            pl.col(Col.risklink_blended_contribution).cast(pl.Float64),
            pl.col(Col.verisk_blended_contribution).cast(pl.Float64),
            pl.col(Col.target_loss).cast(pl.Float64),
            pl.col(Col.uplift_factor_on_base_model).cast(pl.Float64),
        )
    )
    validate(frame)
    return frame
