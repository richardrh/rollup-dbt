from __future__ import annotations

from collections.abc import Mapping
import logging

import polars as pl

from rollup.columns import Col, RawCol
from rollup.config import BlendingConfig, BlendingTargetPoint


logger = logging.getLogger(__name__)
EP_LOSS_KEYS = [
    Col.rollup_lob,
    Col.rollup_peril,
    Col.region_peril_id,
    Col.base_model,
    Col.ep_type,
    Col.return_period,
]
BLEND_VENDOR_LOSS_COLUMNS = {
    "risklink": Col.risklink_loss,
    "verisk": Col.verisk_loss,
}


def apply_blending(
    enriched: pl.LazyFrame,
    staged_ep: pl.LazyFrame,
    blending: pl.DataFrame,
    config: BlendingConfig,
) -> pl.LazyFrame:
    if blending.is_empty():
        raise ValueError("EP-derived blending requires non-empty blending factors")

    ep_losses = aggregate_ep_losses(staged_ep)
    joined_ep = join_ep_summaries(ep_losses)
    base_model_losses = calculate_base_model_losses(ep_losses)
    targets = calculate_ep_blending_targets(
        joined_ep,
        base_model_losses,
        blending,
        config,
    )
    return apply_ep_blending_to_ylt(enriched, targets, config)


def aggregate_ep_losses(staged_ep: pl.LazyFrame) -> pl.LazyFrame:
    return staged_ep.group_by([*EP_LOSS_KEYS, Col.vendor]).agg(
        pl.col(Col.loss).sum().alias(Col.loss)
    )


def join_ep_summaries(ep_losses: pl.LazyFrame) -> pl.LazyFrame:
    summaries = [
        ep_losses.filter(pl.col(Col.vendor) == vendor)
        .drop(Col.vendor)
        .rename({Col.loss: loss_column})
        for vendor, loss_column in BLEND_VENDOR_LOSS_COLUMNS.items()
    ]
    joined = summaries[0]
    for summary in summaries[1:]:
        joined = joined.join(summary, on=EP_LOSS_KEYS, how="full", coalesce=True)
    return joined


def calculate_base_model_losses(ep_losses: pl.LazyFrame) -> pl.LazyFrame:
    return (
        ep_losses.filter(pl.col(Col.vendor) == pl.col(Col.base_model))
        .drop(Col.vendor)
        .rename({Col.loss: Col.base_model_loss})
    )


def calculate_ep_blending_targets(
    joined_ep: pl.LazyFrame,
    base_model_losses: pl.LazyFrame,
    blending: pl.DataFrame,
    config: BlendingConfig,
) -> pl.LazyFrame:
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    air_col = RawCol.AIRBlend if RawCol.AIRBlend in columns else "verisk_weight"
    rms_col = RawCol.RMSBlend if RawCol.RMSBlend in columns else "risklink_weight"
    sub_region_id_expr = (
        pl.col(RawCol.SubRegionPerilID).cast(pl.String)
        if RawCol.SubRegionPerilID in columns
        else pl.lit(None, dtype=pl.String)
    )
    sub_region_expr = (
        pl.col(RawCol.SubRegionPeril).cast(pl.String)
        if RawCol.SubRegionPeril in columns
        else pl.lit(None, dtype=pl.String)
    )
    weights = blending.lazy()
    if RawCol.SubRegionPerilID in columns:
        weights = filter_selected_subregions(
            weights,
            region_col,
            config.subregion_selection,
        ).sort(RawCol.SubRegionPerilID)
    weights = weights.group_by(region_col).first().select(
        pl.col(region_col).cast(pl.Int64).alias(Col.region_peril_id),
        sub_region_id_expr.alias(Col.sub_region_peril_id),
        sub_region_expr.alias(Col.sub_region_peril),
        pl.col(air_col).cast(pl.Float64).alias(Col.verisk_weight),
        pl.col(rms_col).cast(pl.Float64).alias(Col.risklink_weight),
    )

    target_points = joined_ep.join(
        blending_target_points(config.target_points),
        on=[Col.ep_type, Col.return_period],
        how="inner",
    )
    with_base_model = (
        target_points
        .join(base_model_losses, on=EP_LOSS_KEYS, how="left")
        .join(weights, on=Col.region_peril_id, how="left")
    )
    validate_base_model_losses(with_base_model)
    warn_missing_vendor_losses(with_base_model)
    return (
        with_base_model
        .with_columns(
            pl.when(
                pl.col(Col.risklink_loss).is_null()
                | pl.col(Col.verisk_loss).is_null()
            )
            .then(pl.col(Col.base_model_loss))
            .otherwise(
                (pl.col(Col.verisk_loss) * pl.col(Col.verisk_weight))
                + (pl.col(Col.risklink_loss) * pl.col(Col.risklink_weight))
            )
            .alias(Col.target_loss),
        )
        .with_columns(
            (pl.col(Col.target_loss) / pl.col(Col.base_model_loss))
            .clip(
                lower_bound=config.uplift_factor_min,
                upper_bound=config.uplift_factor_max,
            )
            .alias(Col.uplift_factor_on_base_model)
        )
    )


def apply_ep_blending_to_ylt(
    enriched: pl.LazyFrame,
    targets: pl.LazyFrame,
    config: BlendingConfig,
) -> pl.LazyFrame:
    base_model_only = enriched.filter(
        pl.col(Col.vendor) == pl.col(Col.base_model)
    )
    ranked = (
        base_model_only.with_columns(
            pl.col(Col.loss)
            .rank(method="ordinal", descending=True)
            .over(Col.vendor, Col.modelled_lob, Col.rollup_peril)
            .cast(pl.Int64)
            .alias(Col.rnk)
        )
        .with_columns(
            (vendor_years_expr(config.vendor_years) / pl.col(Col.rnk)).alias(Col.rp)
        )
        .with_columns(
            rp_bucket_expr(config.target_points)
            .cast(pl.Int64)
            .alias(Col.rp_bucket)
        )
    )
    factors = targets.select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        pl.col(Col.return_period).alias(Col.rp_bucket),
        Col.ep_type,
        Col.risklink_loss,
        Col.verisk_loss,
        Col.target_loss,
        Col.base_model,
        Col.base_model_loss,
        Col.uplift_factor_on_base_model,
        Col.sub_region_peril_id,
        Col.sub_region_peril,
    )
    return ranked.join(
        factors,
        on=[
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.rp_bucket,
            Col.base_model,
        ],
        how="inner",
    ).with_columns(
        (pl.col(Col.loss) * pl.col(Col.uplift_factor_on_base_model)).alias("blended_loss")
    )


def filter_selected_subregions(
    weights: pl.LazyFrame,
    region_col: str,
    subregion_selection: Mapping[int, str],
) -> pl.LazyFrame:
    if not subregion_selection:
        return weights
    region_id = "__region_peril_id"
    selected_subregion = "__selected_subregion_peril_id"
    selections = pl.DataFrame(
        {
            region_id: list(subregion_selection),
            selected_subregion: list(subregion_selection.values()),
        },
        schema={region_id: pl.Int64, selected_subregion: pl.String},
    ).lazy()
    return (
        weights.with_columns(pl.col(region_col).cast(pl.Int64).alias(region_id))
        .join(selections, on=region_id, how="left")
        .filter(
            pl.col(selected_subregion).is_null()
            | (pl.col(RawCol.SubRegionPerilID).cast(pl.String) == pl.col(selected_subregion))
        )
        .drop(region_id, selected_subregion)
    )


def blending_target_points(
    target_points: tuple[BlendingTargetPoint, ...],
) -> pl.LazyFrame:
    if not target_points:
        raise ValueError("EP blending requires at least one configured target point")
    return pl.DataFrame(
        {
            Col.ep_type: [point.ep_type for point in target_points],
            Col.return_period: [point.return_period for point in target_points],
        },
        schema={Col.ep_type: pl.String, Col.return_period: pl.Int64},
    ).lazy()


def validate_base_model_losses(frame: pl.LazyFrame) -> None:
    missing = frame.filter(pl.col(Col.base_model_loss).is_null()).select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.base_model,
        Col.ep_type,
        Col.return_period,
    ).collect()
    if missing.is_empty():
        return
    raise ValueError(
        "EP blending target points are missing base-model losses: "
        f"{missing.rows(named=True)}"
    )


def warn_missing_vendor_losses(frame: pl.LazyFrame) -> None:
    missing = frame.filter(
        pl.col(Col.risklink_loss).is_null() | pl.col(Col.verisk_loss).is_null()
    ).select(
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.base_model,
        Col.ep_type,
        Col.return_period,
    ).collect()
    if missing.is_empty():
        return
    logger.warning(
        "EP blending target points are missing vendor losses; "
        "falling back to base-model loss for %d point(s): %s",
        missing.height,
        missing.rows(named=True),
    )


def rp_bucket_expr(target_points: tuple[BlendingTargetPoint, ...]) -> pl.Expr:
    periods = sorted(
        {
            point.return_period
            for point in target_points
            if point.ep_type == "OEP" and point.return_period > 0
        }
    )
    if not periods:
        raise ValueError("EP blending requires at least one positive OEP target point")
    expr: pl.Expr = pl.lit(periods[-1])
    for index in range(len(periods) - 1, -1, -1):
        lower_bucket = 0 if index == 0 else periods[index - 1]
        expr = (
            pl.when(pl.col(Col.rp) < periods[index])
            .then(pl.lit(lower_bucket))
            .otherwise(expr)
        )
    return expr


def vendor_years_expr(vendor_years: Mapping[str, int]) -> pl.Expr:
    years = {
        str(vendor).lower(): float(year_count)
        for vendor, year_count in vendor_years.items()
    }
    return pl.col(Col.vendor).replace_strict(
        years,
        return_dtype=pl.Float64,
    )
