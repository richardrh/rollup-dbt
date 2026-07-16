from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model_validation import (
    collect_lazy_schema,
    require_columns,
    require_dtype_family,
    validate_output,
)

MODEL = "int_ylt_ranked"
YLT_RANK_TIE_BREAK_KEYS = [Col.year_id, Col.event_id, Col.analysis_id, Col.model_code]


def validate(ylt: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "ylt", ylt)
    require_columns(
        MODEL,
        "ylt",
        schema,
        [Col.vendor, Col.modelled_lob, Col.rollup_peril, *YLT_RANK_TIE_BREAK_KEYS],
    )
    require_dtype_family(MODEL, "ylt", schema, Col.loss, "numeric")


def transform(ylt: pl.LazyFrame, config: RollupConfig | None = None) -> pl.LazyFrame:
    validate(ylt)
    config = config or RollupConfig()
    partition_keys = [Col.vendor, Col.modelled_lob, Col.rollup_peril]
    ylt = ylt.sort(
        [*partition_keys, Col.loss, *YLT_RANK_TIE_BREAK_KEYS],
        descending=[
            False,
            False,
            False,
            True,
            *(False for _ in YLT_RANK_TIE_BREAK_KEYS),
        ],
    )
    vendor_year_expr = pl.lit(None, dtype=pl.Float64)
    for vendor, years in config.blending.vendor_years.items():
        vendor_year_expr = (
            pl.when(pl.col(Col.vendor) == vendor)
            .then(float(years))
            .otherwise(vendor_year_expr)
        )
    bucket_expr = pl.lit(0)
    for point in sorted(
        (p for p in config.blending.target_points if p.ep_type == "OEP"),
        key=lambda p: p.return_period,
    ):
        bucket_expr = (
            pl.when(pl.col(Col.rp) >= point.return_period)
            .then(point.return_period)
            .otherwise(bucket_expr)
        )
    frame = (
        ylt.with_columns(
            pl.col(Col.loss)
            .cum_count()
            .over(*partition_keys)
            .cast(pl.Int64)
            .alias(Col.rnk)
        )
        .with_columns((vendor_year_expr / pl.col(Col.rnk)).alias(Col.rp))
        .with_columns(bucket_expr.alias(Col.rp_bucket))
    )
    validate_output(MODEL, frame)
    return frame
