from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col, RawCol
from rollup.config import RollupConfig


def join_ep_summaries(ep_selected_main: pl.LazyFrame) -> pl.LazyFrame:
    join_keys = [
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.ep_type,
        Col.return_period,
    ]
    verisk = (
        ep_selected_main.filter(pl.col(Col.vendor) == "verisk")
        .group_by(join_keys)
        .agg(pl.col(Col.loss).sum().alias(Col.verisk_loss))
    )
    risklink = (
        ep_selected_main.filter(pl.col(Col.vendor) == "risklink")
        .group_by(join_keys)
        .agg(pl.col(Col.loss).sum().alias(Col.risklink_loss))
    )
    return risklink.join(verisk, on=join_keys, how="full", coalesce=True)


def select_ep_blending_target_points(joined_ep_summaries: pl.LazyFrame, config: RollupConfig | None = None) -> pl.LazyFrame:
    config = config or RollupConfig()
    target_predicate = pl.lit(False)
    for point in config.blending.target_points:
        target_predicate = target_predicate | (
            (pl.col(Col.ep_type) == point.ep_type)
            & (pl.col(Col.return_period) == point.return_period)
        )
    return joined_ep_summaries.filter(target_predicate)


def select_blending_factor_seed(seeds: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    if "blending_factors" in seeds:
        return seeds["blending_factors"]
    if "blending_weights" in seeds:
        return seeds["blending_weights"]
    raise KeyError("missing blending seed: expected 'blending_factors' or 'blending_weights'")


def prepare_ep_blending_weights(blending_factors: pl.LazyFrame) -> pl.LazyFrame:
    return blending_factors.select(
        pl.col(RawCol.RegionPerilID).alias(Col.region_peril_id),
        pl.col(RawCol.SubRegionPerilID).alias(Col.blend_subregion_peril_id),
        pl.col(RawCol.SubRegionPeril).alias(Col.sub_region_peril),
        pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
        pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
    )


def calculate_ep_blending_targets(
    target_points: pl.LazyFrame,
    weights: pl.LazyFrame,
    config: RollupConfig | None = None,
) -> pl.LazyFrame:
    config = config or RollupConfig()
    return (
        target_points
        .join(weights, on=[Col.region_peril_id, Col.blend_subregion_peril_id], how="left")
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
            .clip(lower_bound=config.blending.uplift_factor_min, upper_bound=config.blending.uplift_factor_max)
            .alias(Col.uplift_factor_on_base_model)
        )
        .drop("_has_both_vendor_losses")
    )
