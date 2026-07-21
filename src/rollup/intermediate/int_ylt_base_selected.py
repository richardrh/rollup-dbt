from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col
from rollup.config import RollupConfig
from rollup.model import PolarsModel


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
            }
        )

    @override
    @classmethod
    def _transform(
        cls, enriched_ylt: pl.LazyFrame, config: RollupConfig
    ) -> pl.LazyFrame:
        frame = (
            enriched_ylt.with_columns(pl.lit("original").alias(Col.metric))
            .filter(
                (pl.col(Col.vendor) == pl.col(Col.base_model))
                & (pl.col(Col.loss) >= config.outputs.minimum_event_loss_threshold / 5)
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
            )
        )
        return frame
