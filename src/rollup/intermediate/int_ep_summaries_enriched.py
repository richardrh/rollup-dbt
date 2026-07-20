from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_ep_summaries_enriched"


def schema() -> pl.Schema:
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


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(
    ep_summaries: pl.LazyFrame, seeds: dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    if "lobs" not in seeds:
        raise ValueError(f"{MODEL}: input 'seeds' missing required key 'lobs'")
    if "perils" not in seeds:
        raise ValueError(f"{MODEL}: input 'seeds' missing required key 'perils'")
    lobs = seeds["lobs"].select(
        Col.modelled_lob,
        Col.rollup_lob,
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
    )
    perils = seeds["perils"].select(
        Col.modelled_peril,
        Col.rollup_peril,
        "region",
        "peril",
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.selection_priority,
        Col.is_dialsup,
        Col.is_euws,
    )
    frame = (
        ep_summaries.join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
        .with_columns(pl.col(Col.selection_priority).fill_null(99))
        .select(
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
    )
    validate(frame)
    return frame
