from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "stg_verisk_ylt"


def validate(raw_ylt: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "raw_ylt", raw_ylt)
    require_columns(
        MODEL,
        "raw_ylt",
        schema,
        [RawCol.CatalogTypeCode, RawCol.Analysis, RawCol.ExposureAttribute],
    )
    for column in [RawCol.ModelCode, RawCol.YearID, RawCol.EventID]:
        require_dtype_family(MODEL, "raw_ylt", schema, column, "integer")
    require_dtype_family(MODEL, "raw_ylt", schema, RawCol.GroundUpLoss, "numeric")


def transform(raw_ylt: pl.LazyFrame) -> pl.LazyFrame:
    validate(raw_ylt)
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
    validate_output(MODEL, frame)
    return frame
