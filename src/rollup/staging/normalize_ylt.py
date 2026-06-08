from __future__ import annotations

import polars as pl

from rollup.columns import Col, RawCol
from rollup.schemas import require_columns
from rollup.staging.load_sources import RISKLINK_YLT_SCHEMA, VERISK_YLT_SCHEMA, StagingFrames


NORMALIZE_VERISK_INPUT_SCHEMA = VERISK_YLT_SCHEMA
NORMALIZE_RISKLINK_INPUT_SCHEMA = RISKLINK_YLT_SCHEMA
NORMALIZED_YLT_SCHEMA = pl.Schema(
    {
        Col.vendor: pl.String,
        Col.analysis_id: pl.String,
        Col.modelled_lob: pl.String,
        Col.modelled_peril: pl.String,
        Col.model_code: pl.Int64,
        Col.year_id: pl.Int64,
        Col.event_id: pl.Int64,
        Col.loss: pl.Float64,
    }
)
NORMALIZE_YLT_OUTPUT_SCHEMA = NORMALIZED_YLT_SCHEMA


def normalize_ylt(frames: StagingFrames) -> pl.LazyFrame:
    require_columns(frames.verisk_ylt, NORMALIZE_VERISK_INPUT_SCHEMA, check_dtypes=False)
    require_columns(frames.risklink_ylt, NORMALIZE_RISKLINK_INPUT_SCHEMA, check_dtypes=False)

    verisk = frames.verisk_ylt.filter(pl.col(RawCol.CatalogTypeCode) == "STC").select(
        pl.lit("verisk").alias(Col.vendor),
        pl.col(RawCol.Analysis).cast(pl.String).alias(Col.analysis_id),
        pl.col(RawCol.ExposureAttribute).cast(pl.String).alias(Col.modelled_lob),
        pl.col(RawCol.Analysis).cast(pl.String).alias(Col.modelled_peril),
        pl.col(RawCol.ModelCode).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.YearID).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.EventID).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.GroundUpLoss).cast(pl.Float64).alias(Col.loss),
    )
    risklink = frames.risklink_ylt.select(
        pl.lit("risklink").alias(Col.vendor),
        pl.col(RawCol.anlsid).cast(pl.String).alias(Col.analysis_id),
        pl.lit(None).cast(pl.String).alias(Col.modelled_lob),
        pl.lit(None).cast(pl.String).alias(Col.modelled_peril),
        pl.lit(None).cast(pl.Int64).alias(Col.model_code),
        pl.col(RawCol.yearid).cast(pl.Int64).alias(Col.year_id),
        pl.col(RawCol.eventid).cast(pl.Int64).alias(Col.event_id),
        pl.col(RawCol.loss).cast(pl.Float64).alias(Col.loss),
    )
    normalized = pl.concat([verisk, risklink], how="vertical")
    require_columns(normalized, NORMALIZE_YLT_OUTPUT_SCHEMA)
    return normalized
