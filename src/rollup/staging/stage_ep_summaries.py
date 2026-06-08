from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.staging.load_sources import StagingFrames


def stage_ep_summaries(frames: StagingFrames) -> pl.LazyFrame:
    lobs = frames.lobs.lazy().select(
        Col.modelled_lob,
        Col.rollup_lob,
        pl.col(Col.class_).cast(pl.String),
        pl.col(Col.office).cast(pl.String),
        pl.col(Col.currency).cast(pl.String),
    )
    perils = frames.perils.lazy().select(
        Col.modelled_peril,
        Col.rollup_peril,
        pl.col(Col.region_peril_id).cast(pl.Int64),
        pl.col(Col.selection_priority).cast(pl.Int64),
        pl.col(Col.is_dialsup).cast(pl.Int64),
    )
    return (
        frames.ep_summaries.lazy()
        .with_columns(
            pl.col(Col.vendor).cast(pl.String).str.to_lowercase(),
            pl.col(Col.analysis_id).cast(pl.String),
            pl.col(Col.return_period).cast(pl.Int64),
            pl.col(Col.loss).cast(pl.Float64),
        )
        .join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
    )
