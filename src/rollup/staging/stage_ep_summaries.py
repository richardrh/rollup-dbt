from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.schemas import require_columns
from rollup.staging.load_sources import EP_SUMMARY_SCHEMA, LOBS_SCHEMA, PERILS_SCHEMA, StagingFrames


STAGED_EP_SUMMARIES_INPUT_SCHEMA = EP_SUMMARY_SCHEMA
STAGED_EP_SUMMARIES_LOBS_INPUT_SCHEMA = LOBS_SCHEMA
STAGED_EP_SUMMARIES_PERILS_INPUT_SCHEMA = PERILS_SCHEMA
STAGED_EP_SUMMARIES_OUTPUT_SCHEMA = pl.Schema(
    {
        Col.vendor: pl.String,
        Col.analysis_id: pl.String,
        Col.modelled_lob: pl.String,
        Col.modelled_peril: pl.String,
        Col.ep_type: pl.String,
        Col.return_period: pl.Int64,
        Col.loss: pl.Float64,
        Col.rollup_lob: pl.String,
        Col.class_: pl.String,
        Col.office: pl.String,
        Col.currency: pl.String,
        Col.rollup_peril: pl.String,
        Col.region_peril_id: pl.Int64,
        Col.selection_priority: pl.Int64,
        Col.is_dialsup: pl.Int64,
    }
)


def stage_ep_summaries(frames: StagingFrames) -> pl.LazyFrame:
    require_columns(frames.ep_summaries, STAGED_EP_SUMMARIES_INPUT_SCHEMA, check_dtypes=False)
    require_columns(frames.lobs, STAGED_EP_SUMMARIES_LOBS_INPUT_SCHEMA, check_dtypes=False)
    require_columns(frames.perils, STAGED_EP_SUMMARIES_PERILS_INPUT_SCHEMA, check_dtypes=False)

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
    staged = (
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
    require_columns(staged, STAGED_EP_SUMMARIES_OUTPUT_SCHEMA)
    return staged
