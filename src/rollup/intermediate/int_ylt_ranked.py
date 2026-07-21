from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model import PolarsModel

_YLT_RANK_TIE_BREAK_KEYS = [Col.year_id, Col.event_id, Col.analysis_id, Col.model_code]


class Model(PolarsModel[[pl.LazyFrame, RollupConfig]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
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
            }
        )

    @override
    @classmethod
    def _transform(cls, ylt: pl.LazyFrame, config: RollupConfig) -> pl.LazyFrame:
        partition_keys = [Col.vendor, Col.modelled_lob, Col.rollup_peril]
        ylt = ylt.sort(
            [*partition_keys, Col.loss, *_YLT_RANK_TIE_BREAK_KEYS],
            descending=[
                False,
                False,
                False,
                True,
                *(False for _ in _YLT_RANK_TIE_BREAK_KEYS),
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
            )
        )
        return frame
