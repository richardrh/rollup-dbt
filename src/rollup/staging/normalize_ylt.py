from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col, RawCol
from rollup.staging.load_sources import RISKLINK_YLT_SCHEMA, VERISK_YLT_SCHEMA, StagingFrames


NORMALIZE_VERISK_INPUT_SCHEMA = VERISK_YLT_SCHEMA
NORMALIZE_RISKLINK_INPUT_SCHEMA = RISKLINK_YLT_SCHEMA
NORMALIZED_YLT_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=False),
        Col.analysis_id: pa.Column(pl.String, nullable=False),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.model_code: pa.Column(pl.Int64, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=True),
        Col.event_id: pa.Column(pl.Int64, nullable=True),
        Col.loss: pa.Column(pl.Float64, nullable=True),
    },
    strict=False,
)
NORMALIZE_YLT_OUTPUT_SCHEMA = NORMALIZED_YLT_SCHEMA


def normalize_ylt(frames: StagingFrames) -> pl.LazyFrame:
    NORMALIZE_VERISK_INPUT_SCHEMA.validate(frames.verisk_ylt)
    NORMALIZE_RISKLINK_INPUT_SCHEMA.validate(frames.risklink_ylt)

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
    return normalized
