from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.model import PolarsModel


class Model(PolarsModel[[pl.LazyFrame]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {
                Col.vendor: pl.String,
                Col.analysis_id: pl.String,
                Col.modelled_lob: pl.String,
                Col.modelled_peril: pl.String,
                Col.ep_type: pl.String,
                Col.return_period: pl.Int64,
                Col.loss: pl.Float64,
                Col.rollup_lob: pl.String,
                Col.cds_cat_class_name: pl.String,
                Col.class_: pl.String,
                Col.office: pl.String,
                Col.currency: pl.String,
                Col.rollup_peril: pl.String,
                "region": pl.String,
                "peril": pl.String,
                Col.region_peril_id: pl.Int64,
                Col.blend_subregion_peril_id: pl.String,
                Col.base_model: pl.String,
                Col.selection_priority: pl.Int64,
                Col.is_dialsup: pl.Int64,
                Col.is_euws: pl.Int64,
            }
        )

    @override
    @classmethod
    def _transform(cls, enriched: pl.LazyFrame) -> pl.LazyFrame:
        selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
        selected_modelled_perils = (
            enriched.select(*selection_keys, Col.modelled_peril, Col.selection_priority)
            .sort([*selection_keys, Col.selection_priority, Col.modelled_peril])
            .unique(subset=selection_keys, keep="first", maintain_order=True)
            .select(*selection_keys, Col.modelled_peril)
        )
        return enriched.join(
            selected_modelled_perils,
            on=[*selection_keys, Col.modelled_peril],
            how="inner",
        ).select(
            pl.col(Col.vendor).cast(pl.String),
            pl.col(Col.analysis_id).cast(pl.String),
            pl.col(Col.modelled_lob).cast(pl.String),
            pl.col(Col.modelled_peril).cast(pl.String),
            pl.col(Col.ep_type).cast(pl.String),
            pl.col(Col.return_period).cast(pl.Int64),
            pl.col(Col.loss).cast(pl.Float64),
            pl.col(Col.rollup_lob).cast(pl.String),
            pl.col(Col.cds_cat_class_name).cast(pl.String),
            pl.col(Col.class_).cast(pl.String),
            pl.col(Col.office).cast(pl.String),
            pl.col(Col.currency).cast(pl.String),
            pl.col(Col.rollup_peril).cast(pl.String),
            pl.col("region").cast(pl.String),
            pl.col("peril").cast(pl.String),
            pl.col(Col.region_peril_id).cast(pl.Int64),
            pl.col(Col.blend_subregion_peril_id).cast(pl.String),
            pl.col(Col.base_model).cast(pl.String),
            pl.col(Col.selection_priority).cast(pl.Int64),
            pl.col(Col.is_dialsup).cast(pl.Int64),
            pl.col(Col.is_euws).cast(pl.Int64),
        )
