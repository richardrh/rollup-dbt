from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    require_columns,
    require_join_key_compatible,
    validate_output,
)

MODEL = "int_ylt_enriched"


def validate(normalized_ylt: pl.LazyFrame, ep_summary: pl.LazyFrame) -> None:
    ylt_schema = collect_lazy_schema(MODEL, "normalized_ylt", normalized_ylt)
    ep_schema = collect_lazy_schema(MODEL, "ep_summary", ep_summary)
    require_columns(
        MODEL,
        "normalized_ylt",
        ylt_schema,
        [
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.model_code,
            Col.year_id,
            Col.event_id,
            Col.loss,
        ],
    )
    require_columns(
        MODEL,
        "ep_summary",
        ep_schema,
        [
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        ],
    )
    require_join_key_compatible(
        MODEL,
        "normalized_ylt",
        ylt_schema,
        "ep_summary",
        ep_schema,
        [Col.vendor, Col.modelled_lob, Col.modelled_peril],
    )
    require_join_key_compatible(
        MODEL,
        "normalized_ylt",
        ylt_schema,
        "ep_summary",
        ep_schema,
        [Col.vendor, Col.analysis_id],
    )


def transform(normalized_ylt: pl.LazyFrame, ep_summary: pl.LazyFrame) -> pl.LazyFrame:
    validate(normalized_ylt, ep_summary)
    verisk_keys = (
        ep_summary.filter(pl.col(Col.vendor) == "verisk")
        .select(
            Col.vendor,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    verisk = (
        normalized_ylt.filter(pl.col(Col.vendor) == "verisk")
        .join(
            verisk_keys,
            on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
            how="inner",
        )
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
            Col.model_code,
            Col.year_id,
            Col.event_id,
            Col.loss,
        )
    )
    risklink_lookup = (
        ep_summary.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
        )
        .unique()
    )
    risklink = (
        normalized_ylt.filter(pl.col(Col.vendor) == "risklink")
        .drop(Col.modelled_lob, Col.modelled_peril)
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.rollup_lob,
            Col.rollup_peril,
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
            Col.cds_cat_class_name,
            Col.class_,
            Col.office,
            Col.currency,
            Col.model_code,
            Col.year_id,
            Col.event_id,
            Col.loss,
        )
    )
    frame = pl.concat([verisk, risklink], how="vertical")
    validate_output(MODEL, frame)
    return frame
