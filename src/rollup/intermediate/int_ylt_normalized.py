from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_dtype_family,
)

MODEL = "int_ylt_normalized"
_COLUMNS = [
    Col.vendor,
    Col.analysis_id,
    Col.modelled_peril,
    Col.modelled_lob,
    Col.model_code,
    Col.year_id,
    Col.event_id,
    Col.loss,
]


def validate(verisk_ylt: pl.LazyFrame, risklink_ylt: pl.LazyFrame) -> None:
    for name, frame in {"verisk_ylt": verisk_ylt, "risklink_ylt": risklink_ylt}.items():
        schema = collect_lazy_schema(MODEL, name, frame)
        require_columns(MODEL, name, schema, _COLUMNS)
        require_dtype_family(MODEL, name, schema, Col.loss, "numeric")
        for column in [Col.year_id, Col.event_id]:
            require_dtype_family(MODEL, name, schema, column, "integer")


def transform(verisk_ylt: pl.LazyFrame, risklink_ylt: pl.LazyFrame) -> pl.LazyFrame:
    validate(verisk_ylt, risklink_ylt)
    frame = pl.concat([verisk_ylt, risklink_ylt], how="vertical")
    validate_output(MODEL, frame)
    return frame
