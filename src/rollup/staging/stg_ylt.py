from __future__ import annotations
# mypy: ignore-errors

import polars as pl

from rollup.columns import Col, RawCol
from rollup.pipeline_utils import _verisk_string


def normalize_ylt(ylt_frames: dict[str, pl.LazyFrame]) -> pl.LazyFrame:
    verisk = ylt_frames["verisk"].filter(_verisk_string(RawCol.CatalogTypeCode) == "STC").select(
        pl.lit("verisk").alias(Col.vendor),
        _verisk_string(RawCol.Analysis).alias(Col.analysis_id),
        _verisk_string(RawCol.Analysis).alias(Col.modelled_peril),
        _verisk_string(RawCol.ExposureAttribute).alias(Col.modelled_lob),
        pl.col(RawCol.ModelCode).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.YearID).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
    )

    risklink = ylt_frames["risklink"].select(
        pl.lit("risklink").alias(Col.vendor),
        pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
        pl.lit(None).cast(pl.String).alias(Col.modelled_peril),
        pl.lit(None).cast(pl.String).alias(Col.modelled_lob),
        pl.lit(None).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.yearid).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.eventid).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
    )

    return pl.concat([verisk, risklink], how="vertical")
