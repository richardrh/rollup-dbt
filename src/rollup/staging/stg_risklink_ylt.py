from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col, RawCol

MODEL = "stg_risklink_ylt"


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
    frame = raw_ylt.select(
        pl.lit("risklink").alias(Col.vendor),
        pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
        pl.lit(None).cast(pl.String).alias(Col.modelled_peril),
        pl.lit(None).cast(pl.String).alias(Col.modelled_lob),
        pl.lit(None).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.yearid).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.eventid).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
    )
    validate(frame)
    return frame
