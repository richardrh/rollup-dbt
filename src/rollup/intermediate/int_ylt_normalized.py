from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_ylt_normalized"


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


def transform(verisk_ylt: pl.LazyFrame, risklink_ylt: pl.LazyFrame) -> pl.LazyFrame:
    frame = pl.concat([verisk_ylt, risklink_ylt], how="vertical").select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_peril,
        Col.modelled_lob,
        pl.col(Col.model_code).cast(pl.Int64),
        pl.col(Col.year_id).cast(pl.Int64),
        pl.col(Col.event_id).cast(pl.Int64),
        pl.col(Col.loss).cast(pl.Float64),
    )
    validate(frame)
    return frame
