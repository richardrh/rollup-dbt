from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.schemas import require_columns
from rollup.staging.normalize_ylt import NORMALIZED_YLT_SCHEMA
from rollup.staging.stage_ep_summaries import STAGED_EP_SUMMARIES_OUTPUT_SCHEMA


ENRICHED_YLT_INPUT_SCHEMA = NORMALIZED_YLT_SCHEMA
ENRICHED_EP_INPUT_SCHEMA = STAGED_EP_SUMMARIES_OUTPUT_SCHEMA
ENRICHED_YLT_OUTPUT_SCHEMA = pl.Schema(
    {
        Col.vendor: pl.String,
        Col.analysis_id: pl.String,
        Col.modelled_lob: pl.String,
        Col.modelled_peril: pl.String,
        Col.model_code: pl.Int64,
        Col.year_id: pl.Int64,
        Col.event_id: pl.Int64,
        Col.loss: pl.Float64,
        Col.rollup_lob: pl.String,
        Col.rollup_peril: pl.String,
        Col.region_peril_id: pl.Int64,
        Col.class_: pl.String,
        Col.office: pl.String,
        Col.currency: pl.String,
        Col.selection_priority: pl.Int64,
        Col.is_dialsup: pl.Int64,
    }
)


def build_enriched_ylt(normalized_ylt: pl.LazyFrame, staged_ep: pl.LazyFrame) -> pl.LazyFrame:
    require_columns(normalized_ylt, ENRICHED_YLT_INPUT_SCHEMA)
    require_columns(staged_ep, ENRICHED_EP_INPUT_SCHEMA)

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
    require_columns(enriched, ENRICHED_YLT_OUTPUT_SCHEMA)
    return enriched
