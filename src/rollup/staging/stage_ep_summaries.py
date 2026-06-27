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
        pl.col(Col.blend_subregion_peril_id).cast(pl.String),
        pl.col(Col.base_model).cast(pl.String).str.to_lowercase(),
        pl.col(Col.selection_priority).cast(pl.Int64),
        pl.col(Col.is_dialsup).cast(pl.Int64),
        pl.col(Col.is_euws).cast(pl.Int64),
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
        .with_columns(pl.col(Col.selection_priority).fill_null(99))
    )
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_candidates = staged.select(
        *selection_keys,
        Col.modelled_peril,
        Col.selection_priority,
    ).unique()
    selected_priorities = selected_candidates.group_by(selection_keys).agg(
        pl.col(Col.selection_priority).min()
    )
    selected_modelled_perils = (
        selected_candidates.join(
            selected_priorities,
            on=[*selection_keys, Col.selection_priority],
            how="inner",
        )
        .sort([*selection_keys, Col.selection_priority, Col.modelled_peril])
        .group_by(selection_keys, maintain_order=True)
        .first()
        .select(*selection_keys, Col.modelled_peril)
    )
    selected = staged.join(
        selected_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )
    return selected
