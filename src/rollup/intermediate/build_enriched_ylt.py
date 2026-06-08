from __future__ import annotations

import polars as pl

from rollup.columns import Col


def build_enriched_ylt(normalized_ylt: pl.LazyFrame, staged_ep: pl.LazyFrame) -> pl.LazyFrame:
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
    return pl.concat([verisk, risklink], how="diagonal_relaxed")
