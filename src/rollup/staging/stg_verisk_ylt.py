from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "stg_verisk_ylt"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.vendor: pl.String,
            Col.analysis_id: pl.String,
            Col.modelled_peril: pl.String,
            Col.modelled_lob: pl.String,
            Col.model_code: pl.Int64,
            Col.year_id: pl.Int64,
            Col.event_id: pl.Int64,
            Col.loss: pl.Float64,
        }
    )


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(raw_ylt: pl.LazyFrame) -> pl.LazyFrame:
    frame = raw_ylt.filter(
        pl.col(RawCol.CatalogTypeCode).cast(pl.String).str.strip_chars() == "STC"
    ).select(
        pl.lit("verisk").alias(Col.vendor),
        pl.col(RawCol.Analysis)
        .cast(pl.String)
        .str.strip_chars()
        .alias(Col.analysis_id),
        pl.col(RawCol.Analysis)
        .cast(pl.String)
        .str.strip_chars()
        .alias(Col.modelled_peril),
        pl.col(RawCol.ExposureAttribute)
        .cast(pl.String)
        .str.strip_chars()
        .alias(Col.modelled_lob),
        pl.col(RawCol.ModelCode).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.YearID).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
    )
    validate(frame)
    return frame
