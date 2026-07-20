from __future__ import annotations

import polars as pl
from rollup.model_validation import validate_schema

from rollup.columns import Col

MODEL = "int_ylt_enriched"


def schema() -> pl.Schema:
    return pl.Schema(
        {  # type: ignore[arg-type]  # Polars accepts StrEnum keys.
            Col.vendor: pl.String,
            Col.analysis_id: pl.String,
            Col.modelled_lob: pl.String,
            Col.modelled_peril: pl.String,
            Col.rollup_lob: pl.String,
            Col.rollup_peril: pl.String,
            Col.region_peril_id: pl.Int64,
            Col.blend_subregion_peril_id: pl.String,
            Col.base_model: pl.String,
            Col.selection_priority: pl.Int64,
            Col.is_dialsup: pl.Int64,
            Col.is_euws: pl.Int64,
            Col.cds_cat_class_name: pl.String,
            Col.class_: pl.String,
            Col.office: pl.String,
            Col.currency: pl.String,
            Col.model_code: pl.Int64,
            Col.year_id: pl.Int64,
            Col.event_id: pl.Int64,
            Col.loss: pl.Float64,
        }
    )  # type: ignore[arg-type]


def validate(frame: pl.LazyFrame) -> None:
    validate_schema(MODEL, schema(), frame)


def transform(normalized_ylt: pl.LazyFrame, ep_summary: pl.LazyFrame) -> pl.LazyFrame:
    key_cols = [
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
    ]
    output_cols = [
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        *key_cols,
        Col.model_code,
        Col.year_id,
        Col.event_id,
        Col.loss,
    ]
    verisk_keys = (
        ep_summary.filter(pl.col(Col.vendor) == "verisk")
        .select(Col.vendor, Col.modelled_lob, Col.modelled_peril, *key_cols)
        .unique()
    )
    verisk = (
        normalized_ylt.filter(pl.col(Col.vendor) == "verisk")
        .join(
            verisk_keys,
            on=[Col.vendor, Col.modelled_lob, Col.modelled_peril],
            how="inner",
        )
        .select(*output_cols)
    )
    risklink_lookup = (
        ep_summary.filter(pl.col(Col.vendor) == "risklink")
        .select(
            Col.vendor, Col.analysis_id, Col.modelled_lob, Col.modelled_peril, *key_cols
        )
        .unique()
    )
    risklink = (
        normalized_ylt.filter(pl.col(Col.vendor) == "risklink")
        .drop(Col.modelled_lob, Col.modelled_peril)
        .join(risklink_lookup, on=[Col.vendor, Col.analysis_id], how="inner")
        .select(*output_cols)
    )
    frame = pl.concat([verisk, risklink], how="vertical").select(
        Col.vendor,
        Col.analysis_id,
        Col.modelled_lob,
        Col.modelled_peril,
        Col.rollup_lob,
        Col.rollup_peril,
        pl.col(Col.region_peril_id).cast(pl.Int64),
        Col.blend_subregion_peril_id,
        Col.base_model,
        pl.col(Col.selection_priority).cast(pl.Int64),
        pl.col(Col.is_dialsup).cast(pl.Int64),
        pl.col(Col.is_euws).cast(pl.Int64),
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
        pl.col(Col.model_code).cast(pl.Int64),
        pl.col(Col.year_id).cast(pl.Int64),
        pl.col(Col.event_id).cast(pl.Int64),
        pl.col(Col.loss).cast(pl.Float64),
    )
    validate(frame)
    return frame
