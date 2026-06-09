from __future__ import annotations

import polars as pl
import pandera.polars as pa

from rollup.columns import Col
from rollup.staging.normalize_ylt import NORMALIZED_YLT_SCHEMA
from rollup.staging.stage_ep_summaries import STAGED_EP_SUMMARIES_OUTPUT_SCHEMA


ENRICHED_YLT_INPUT_SCHEMA = NORMALIZED_YLT_SCHEMA
ENRICHED_EP_INPUT_SCHEMA = STAGED_EP_SUMMARIES_OUTPUT_SCHEMA
ENRICHED_YLT_OUTPUT_SCHEMA = pa.DataFrameSchema(
    {
        Col.vendor: pa.Column(pl.String, nullable=False),
        Col.analysis_id: pa.Column(pl.String, nullable=False),
        Col.modelled_lob: pa.Column(pl.String, nullable=True),
        Col.modelled_peril: pa.Column(pl.String, nullable=True),
        Col.model_code: pa.Column(pl.Int64, nullable=True),
        Col.year_id: pa.Column(pl.Int64, nullable=False),
        Col.event_id: pa.Column(pl.Int64, nullable=False),
        Col.loss: pa.Column(pl.Float64, nullable=False),
        Col.rollup_lob: pa.Column(pl.String, nullable=False),
        Col.rollup_peril: pa.Column(pl.String, nullable=False),
        Col.region_peril_id: pa.Column(pl.Int64, nullable=False),
        Col.class_: pa.Column(pl.String, nullable=False),
        Col.office: pa.Column(pl.String, nullable=False),
        Col.currency: pa.Column(pl.String, nullable=False),
        Col.selection_priority: pa.Column(pl.Int64, nullable=False),
        Col.is_dialsup: pa.Column(pl.Int64, nullable=False),
    },
    strict=False,
)


def build_enriched_ylt(normalized_ylt: pl.LazyFrame, staged_ep: pl.LazyFrame) -> pl.LazyFrame:
    ENRICHED_YLT_INPUT_SCHEMA.validate(normalized_ylt)
    ENRICHED_EP_INPUT_SCHEMA.validate(staged_ep)

    ep_keys = staged_ep.select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        Col.region_peril_id,
        Col.class_,
        Col.office,
        Col.currency,
        Col.selection_priority,
        Col.is_dialsup,
    ).unique()
    verisk_keys = ep_keys.filter(pl.col(Col.vendor) == "verisk").drop(Col.analysis_id)
    risklink_keys = ep_keys.filter(pl.col(Col.vendor) == "risklink").drop(
        [Col.modelled_lob, Col.modelled_peril]
    )

    verisk = normalized_ylt.filter(pl.col(Col.vendor) == "verisk").join(
        verisk_keys,
        on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
        how="inner",
    )
    risklink = normalized_ylt.filter(pl.col(Col.vendor) == "risklink").join(
        risklink_keys,
        on=[Col.vendor, Col.analysis_id],
        how="inner",
    )
    enriched = pl.concat([verisk, risklink], how="diagonal_relaxed")
    return enriched
