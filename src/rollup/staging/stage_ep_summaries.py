from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.staging.load_sources import EP_SUMMARY_SCHEMA, LOBS_SCHEMA, PERILS_SCHEMA, StagingFrames


STAGED_EP_SUMMARIES_INPUT_SCHEMA = EP_SUMMARY_SCHEMA
STAGED_EP_SUMMARIES_LOBS_INPUT_SCHEMA = LOBS_SCHEMA
STAGED_EP_SUMMARIES_PERILS_INPUT_SCHEMA = PERILS_SCHEMA
STAGED_EP_SUMMARIES_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=False),
        Col.analysis_id: pa.Column(pl.String, nullable=False),
        Col.modelled_lob: pa.Column(pl.String, nullable=False),
        Col.modelled_peril: pa.Column(pl.String, nullable=False),
        Col.ep_type: pa.Column(pl.String, nullable=False),
        Col.return_period: pa.Column(pl.Int64, nullable=False),
        Col.loss: pa.Column(pl.Float64, nullable=False),
        Col.rollup_lob: pa.Column(pl.String, nullable=False),
        Col.class_: pa.Column(pl.String, nullable=False),
        Col.office: pa.Column(pl.String, nullable=False),
        Col.currency: pa.Column(pl.String, nullable=False),
        Col.rollup_peril: pa.Column(pl.String, nullable=False),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=False),
        Col.selection_priority: pa.Column(pl.Int64, nullable=False),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=False),
    },
    strict=False,
)


def stage_ep_summaries(frames: StagingFrames) -> pl.LazyFrame:
    STAGED_EP_SUMMARIES_INPUT_SCHEMA.validate(frames.ep_summaries)
    STAGED_EP_SUMMARIES_LOBS_INPUT_SCHEMA.validate(frames.lobs)
    STAGED_EP_SUMMARIES_PERILS_INPUT_SCHEMA.validate(frames.perils)

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
    return staged
