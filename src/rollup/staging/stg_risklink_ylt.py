from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "stg_risklink_ylt"


def validate(raw_ylt: pl.LazyFrame) -> None:
    schema = collect_lazy_schema(MODEL, "raw_ylt", raw_ylt)
    require_columns(MODEL, "raw_ylt", schema, [RawCol.anlsid])
    for column in [RawCol.yearid, RawCol.eventid]:
        require_dtype_family(MODEL, "raw_ylt", schema, column, "integer")
    require_dtype_family(MODEL, "raw_ylt", schema, RawCol.loss, "numeric")


def transform(raw_ylt: pl.LazyFrame) -> pl.LazyFrame:
    validate(raw_ylt)
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
    validate_output(MODEL, frame)
    return frame
