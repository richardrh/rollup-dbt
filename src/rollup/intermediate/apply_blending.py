from __future__ import annotations

from collections.abc import Mapping

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.intermediate.build_enriched_ylt import ENRICHED_YLT_OUTPUT_SCHEMA
from rollup.staging.stage_ep_summaries import STAGED_EP_SUMMARIES_OUTPUT_SCHEMA


BLENDING_INPUT_SCHEMA = ENRICHED_YLT_OUTPUT_SCHEMA
BLENDING_EP_INPUT_SCHEMA = STAGED_EP_SUMMARIES_OUTPUT_SCHEMA
BLENDING_FACTORS_SCHEMA = pa.DataFrameSchema(
    {Col.region_peril_id: pa.Column(pl.Int64, nullable=True)},
    strict=False,
)
RAW_BLENDING_FACTORS_SCHEMA = pa.DataFrameSchema(
    {RawCol.RegionPerilID: pa.Column(pl.Int64, nullable=True)},
    strict=False,
)
BLENDED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        **BLENDING_INPUT_SCHEMA.columns,
        Col.base_model: pa.Column(pl.String, nullable=True),
        Col.rnk: pa.Column(pl.Int64, nullable=True),
        Col.rp: pa.Column(pl.Float64, nullable=True),
        Col.rp_bucket: pa.Column(pl.Int64, nullable=True),
        Col.risklink_loss: pa.Column(pl.Float64, nullable=True),
        Col.verisk_loss: pa.Column(pl.Float64, nullable=True),
        Col.target_loss: pa.Column(pl.Float64, nullable=True),
        Col.base_model_loss: pa.Column(pl.Float64, nullable=True),
        Col.uplift_factor_on_base_model: pa.Column(pl.Float64, nullable=True),
        "blended_loss": pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
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
    vendor_years: Mapping[str, int],
) -> pl.LazyFrame:
    BLENDING_INPUT_SCHEMA.validate(enriched)
    BLENDING_EP_INPUT_SCHEMA.validate(staged_ep)

    if blending.is_empty():
        raise ValueError("EP-derived blending requires non-empty blending factors")

    ep_losses = aggregate_ep_losses(staged_ep)
    joined_ep = join_ep_summaries(ep_losses)
    base_model_losses = calculate_base_model_losses(ep_losses)
    targets = calculate_ep_blending_targets(joined_ep, base_model_losses, blending)
    return apply_ep_blending_to_ylt(enriched, targets, vendor_years)


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
) -> pl.LazyFrame:
    columns = blending.columns
    region_col = RawCol.RegionPerilID if RawCol.RegionPerilID in columns else Col.region_peril_id
    factor_schema = RAW_BLENDING_FACTORS_SCHEMA if RawCol.RegionPerilID in columns else BLENDING_FACTORS_SCHEMA
    factor_schema.validate(blending)
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
        weights = weights.filter(
            (pl.col(region_col) != 216)
            | (pl.col(RawCol.SubRegionPerilID).cast(pl.String) == "216b")
        ).sort(RawCol.SubRegionPerilID)
    weights = weights.group_by(region_col).first().select(
        pl.col(region_col).cast(pl.Int64).alias(Col.region_peril_id),
        sub_region_id_expr.alias(Col.sub_region_peril_id),
        sub_region_expr.alias(Col.sub_region_peril),
        pl.col(air_col).cast(pl.Float64).alias(Col.verisk_weight),
        pl.col(rms_col).cast(pl.Float64).alias(Col.risklink_weight),
    )

    target_points = joined_ep.filter(
        ((pl.col(Col.ep_type) == "AAL") & (pl.col(Col.return_period) == 0))
        | ((pl.col(Col.ep_type) == "OEP") & (pl.col(Col.return_period).is_in([200, 1000])))
    )
    return (
        target_points.filter(
            pl.col(Col.risklink_loss).is_not_null()
            & pl.col(Col.verisk_loss).is_not_null()
        )
        .join(base_model_losses, on=EP_LOSS_KEYS, how="left")
        .join(weights, on=Col.region_peril_id, how="left")
        .with_columns(
            (
                (pl.col(Col.verisk_loss) * pl.col(Col.verisk_weight))
                + (pl.col(Col.risklink_loss) * pl.col(Col.risklink_weight))
            ).alias(Col.target_loss),
        )
        .with_columns(
            (pl.col(Col.target_loss) / pl.col(Col.base_model_loss))
            .clip(lower_bound=0.1, upper_bound=10.0)
            .alias(Col.uplift_factor_on_base_model)
        )
    )


def apply_ep_blending_to_ylt(
    enriched: pl.LazyFrame,
    targets: pl.LazyFrame,
    vendor_years: Mapping[str, int],
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
            (vendor_years_expr(vendor_years) / pl.col(Col.rnk)).alias(Col.rp)
        )
        .with_columns(
            pl.when(pl.col(Col.rp) < 200)
            .then(pl.lit(0))
            .when(pl.col(Col.rp) < 1000)
            .then(pl.lit(200))
            .otherwise(pl.lit(1000))
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


def vendor_years_expr(vendor_years: Mapping[str, int]) -> pl.Expr:
    years = {
        str(vendor).lower(): float(year_count)
        for vendor, year_count in vendor_years.items()
    }
    return pl.col(Col.vendor).replace_strict(
        years,
        return_dtype=pl.Float64,
    )
