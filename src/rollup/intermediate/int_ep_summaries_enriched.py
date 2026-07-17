from __future__ import annotations

import polars as pl

from rollup.columns import Col
from rollup.model_validation import (
    collect_lazy_schema,
    validate_output,
    require_columns,
    require_join_key_compatible,
    require_dtype_family,
    validate_mapping_key,
)

MODEL = "int_ep_summaries_enriched"


def validate(ep_summaries: pl.LazyFrame, seeds: dict[str, pl.LazyFrame]) -> None:
    validate_mapping_key(MODEL, "seeds", seeds, "lobs")
    validate_mapping_key(MODEL, "seeds", seeds, "perils")
    lobs = seeds["lobs"]
    perils = seeds["perils"]
    ep_schema = collect_lazy_schema(MODEL, "ep_summaries", ep_summaries)
    lobs_schema = collect_lazy_schema(MODEL, "seeds.lobs", lobs)
    perils_schema = collect_lazy_schema(MODEL, "seeds.perils", perils)
    require_columns(
        MODEL,
        "ep_summaries",
        ep_schema,
        [
            Col.vendor,
            Col.analysis_id,
            Col.modelled_lob,
            Col.modelled_peril,
            Col.ep_type,
            Col.return_period,
            Col.loss,
        ],
    )
    require_dtype_family(MODEL, "ep_summaries", ep_schema, Col.return_period, "integer")
    require_dtype_family(MODEL, "ep_summaries", ep_schema, Col.loss, "numeric")
    require_columns(
        MODEL,
        "seeds.lobs",
        lobs_schema,
        [Col.rollup_lob, Col.cds_cat_class_name, Col.class_, Col.office, Col.currency],
    )
    require_columns(
        MODEL,
        "seeds.perils",
        perils_schema,
        [
            Col.rollup_peril,
            "region",
            "peril",
            Col.region_peril_id,
            Col.blend_subregion_peril_id,
            Col.base_model,
            Col.selection_priority,
            Col.is_dialsup,
            Col.is_euws,
        ],
    )
    require_join_key_compatible(
        MODEL, "ep_summaries", ep_schema, "seeds.lobs", lobs_schema, [Col.modelled_lob]
    )
    require_join_key_compatible(
        MODEL,
        "ep_summaries",
        ep_schema,
        "seeds.perils",
        perils_schema,
        [Col.modelled_peril],
    )


def transform(
    ep_summaries: pl.LazyFrame, seeds: dict[str, pl.LazyFrame]
) -> pl.LazyFrame:
    validate(ep_summaries, seeds)
    lobs = seeds["lobs"].select(
        Col.modelled_lob,
        Col.rollup_lob,
        Col.cds_cat_class_name,
        Col.class_,
        Col.office,
        Col.currency,
    )
    perils = seeds["perils"].select(
        Col.modelled_peril,
        Col.rollup_peril,
        "region",
        "peril",
        Col.region_peril_id,
        Col.blend_subregion_peril_id,
        Col.base_model,
        Col.selection_priority,
        Col.is_dialsup,
        Col.is_euws,
    )
    frame = (
        ep_summaries.join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
        .with_columns(pl.col(Col.selection_priority).fill_null(99))
    )
    validate_output(MODEL, frame)
    return frame
