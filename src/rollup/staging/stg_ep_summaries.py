from __future__ import annotations
# mypy: ignore-errors

from pathlib import Path

import polars as pl

from rollup.columns import Col

def load_ep_summaries(data_root: Path | str = "data") -> pl.LazyFrame:
    folder = Path(data_root) / "ep_summaries"
    paths = sorted(folder.rglob("*.long.csv"))
    if not paths:
        return pl.LazyFrame()

    frames: list[pl.LazyFrame] = []
    for path in paths:
        vendor = _vendor_from_ep_summary_path(path)
        frames.append(pl.scan_csv(path).with_columns(pl.lit(vendor).alias(Col.vendor)))
    return pl.concat(frames, how="diagonal_relaxed")


def _vendor_from_ep_summary_path(path: Path) -> str:
    for part in path.parts:
        lower = part.lower()
        if lower in {"risklink", "verisk"}:
            return lower
        if lower in {"vendor=risklink", "vendor=verisk"}:
            return lower.split("=", 1)[1]
    raise ValueError(f"EP summary file is not under a recognised vendor folder: {path}")


def enrich_ep_summaries(
    ep_summaries: pl.LazyFrame,
    seeds: dict[str, pl.LazyFrame],
) -> pl.LazyFrame:
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

    return (
        ep_summaries.join(lobs, on=Col.modelled_lob, how="left")
        .join(perils, on=Col.modelled_peril, how="left")
        .with_columns(pl.col(Col.selection_priority).fill_null(99))
    )


def select_main_ep_summaries(enriched: pl.LazyFrame) -> pl.LazyFrame:
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_modelled_perils = _select_modelled_perils_by_priority(enriched, selection_keys)
    return enriched.join(
        selected_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )


def select_dialsup_ep_summaries(enriched: pl.LazyFrame) -> pl.LazyFrame:
    selection_keys = [Col.vendor, Col.rollup_lob, Col.rollup_peril]
    selected_dialsup_modelled_perils = _select_dialsup_modelled_perils(enriched, selection_keys)
    return enriched.join(
        selected_dialsup_modelled_perils,
        on=[*selection_keys, Col.modelled_peril],
        how="inner",
    )


def _select_modelled_perils_by_priority(
    enriched: pl.LazyFrame,
    selection_keys: list[str],
) -> pl.LazyFrame:
    return (
        enriched.select(
        *selection_keys,
        Col.modelled_peril,
        Col.selection_priority,
        )
        .sort([*selection_keys, Col.selection_priority, Col.modelled_peril])
        .unique(subset=selection_keys, keep="first", maintain_order=True)
        .select(*selection_keys, Col.modelled_peril)
    )


def _select_dialsup_modelled_perils(
    enriched: pl.LazyFrame,
    selection_keys: list[str],
) -> pl.LazyFrame:
    return enriched.filter(pl.col(Col.is_dialsup) == 1).select(*selection_keys, Col.modelled_peril).unique()
