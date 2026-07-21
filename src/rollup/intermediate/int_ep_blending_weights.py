from __future__ import annotations

from typing import override

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model import PolarsModel


class Model(PolarsModel[[dict[str, pl.LazyFrame]]]):
    @override
    @classmethod
    def schema(cls) -> pl.Schema:
        return pl.Schema(
            {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
                Col.region_peril_id: pl.Int64,
                Col.blend_subregion_peril_id: pl.String,
                Col.sub_region_peril: pl.String,
                Col.verisk_weight: pl.Float64,
                Col.risklink_weight: pl.Float64,
            }
        )

    @override
    @classmethod
    def _transform(cls, seeds: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
        if "blending_factors" not in seeds:
            raise ValueError(
                "int_ep_blending_weights: input 'seeds' missing required key "
                "'blending_factors'"
            )
        blending_factors = seeds["blending_factors"]
        return blending_factors.select(
            pl.col(RawCol.RegionPerilID).cast(pl.Int64).alias(Col.region_peril_id),
            pl.col(RawCol.SubRegionPerilID)
            .cast(pl.String)
            .alias(Col.blend_subregion_peril_id),
            pl.col(RawCol.SubRegionPeril).cast(pl.String).alias(Col.sub_region_peril),
            pl.col(RawCol.AIRBlend).cast(pl.Float64).alias(Col.verisk_weight),
            pl.col(RawCol.RMSBlend).cast(pl.Float64).alias(Col.risklink_weight),
        )
