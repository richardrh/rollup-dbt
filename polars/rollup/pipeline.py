"""Pipeline orchestrator."""

from __future__ import annotations

import logging
from dataclasses import replace

import polars as pl

from rollup import config
from rollup.audit import audit_long, audit_wide
from rollup.config import VendorName
from rollup.fanout import fanout_hisco
from rollup.metrics.dialsup import add_dialsup
from rollup.metrics.main_chain import add_main_metrics
from rollup.schemas import frames as F
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.seeds import Seeds
from rollup.stages.factors import (
    attach_currency,
    attach_euws,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
    validate_fx_coverage,
)
from rollup.stages.staging import (
    filter_valid_analyses,
    load_raw_risklink_ylt,
    load_raw_verisk_ylt,
    normalize_risklink_ylt,
    normalize_verisk_ylt,
)
from rollup.validate import validate_schema
from rollup.variants import VariantSpec, build_variants, forecast_dates_from_seed, forecast_tags


log = logging.getLogger("rollup.pipeline")

_AUDIT_SUBDIR = "debug"
_AUDIT_WIDE_FILE = "audit_wide.parquet"
_AUDIT_LONG_FILE = "audit_long.parquet"

_AE_MATCH_TMP = "_ae_match"
_ORPHAN_COUNT = "orphans"
_TOTAL_COUNT = "total"


def count_event_id_orphans(
    ylt: pl.LazyFrame,
    air_events: pl.LazyFrame,
    *,
    vendor_filter: VendorName = VendorName.VERISK,
) -> int:
    """Count Verisk-style event IDs that are absent from the air_events seed."""
    ae = air_events.select(
        pl.col(AE.YEAR).alias(Y.YEAR_ID),
        pl.col(AE.EVENT_ID),
        pl.col(AE.MODEL_ID).alias(Y.MODEL_CODE),
    ).with_columns(pl.lit(True).alias(_AE_MATCH_TMP))

    joined = (
        ylt.filter(pl.col(Y.VENDOR) == vendor_filter)
        .join(ae, on=[Y.YEAR_ID, Y.EVENT_ID, Y.MODEL_CODE], how="left")
    )
    collected = joined.select(
        pl.len().alias(_TOTAL_COUNT),
        pl.col(_AE_MATCH_TMP).is_null().sum().alias(_ORPHAN_COUNT),
    ).collect()
    total = collected[_TOTAL_COUNT].item()
    orphans = collected[_ORPHAN_COUNT].item()
    if orphans:
        log.warning(
            f"event catalogue validation incomplete for vendor={vendor_filter}: "
            f"{orphans:,} / {total:,} YLT rows did not match "
            "data/seeds/validation/air_events.csv. Calculations continue, "
            "but ModelEventDay remains 0 and AIR event metadata is not validated. "
            "Provide air_events.csv to validate/enrich event IDs."
        )
    else:
        log.info(f"event-id check ({vendor_filter}): {total:,}/{total:,} rows matched air_events")
    return orphans


def build_all_factors(cfg: config.Config, seeds: Seeds) -> pl.LazyFrame:
    """Build the all-factors LazyFrame from staging through metrics."""
    verisk = cfg.vendor(VendorName.VERISK)
    risklink = cfg.vendor(VendorName.RISKLINK)
    tags = forecast_tags(forecast_dates_from_seed(seeds))
    n_sim: dict[VendorName, int] = {
        VendorName.VERISK: verisk.n_simulations,
        VendorName.RISKLINK: risklink.n_simulations,
    }
    log.info(f"forecast tags from seed: {tags}")
    analyses = filter_valid_analyses(seeds.analyses, seeds.valid_analyses)

    rl_norm = normalize_risklink_ylt(
        load_raw_risklink_ylt(risklink.ylt_dir, glob=risklink.ylt_glob),
        analyses, seeds.perils, seeds.lobs,
    )
    vk_norm = normalize_verisk_ylt(
        load_raw_verisk_ylt(verisk.ylt_dir, glob=verisk.ylt_glob),
        analyses, seeds.perils, seeds.lobs,
    )
    ylt = pl.concat([rl_norm, vk_norm], how="vertical")
    log.info("staging: normalised YLTs concatenated")
    count_event_id_orphans(ylt, seeds.air_events, vendor_filter=VendorName.VERISK)

    all_factors = (
        ylt
        .pipe(attach_currency, seeds.fx_rates)
        .pipe(attach_forecast_factors, seeds.forecast_factors, tags)
        .pipe(attach_rank, n_sim=n_sim)
        .pipe(attach_euws, seeds.euws_rate_factors, seeds.euws_rank_overrides)
        .pipe(attach_uplift, seeds.blending_weights, n_sim=n_sim)
        .pipe(add_main_metrics, tags)
        .pipe(add_dialsup, tags[0])
    )
    log.info(f"metrics: {3 + 2 * len(tags)} derived loss columns + 1 dialsup column")
    validate_schema(all_factors, F.ALL_FACTORS, name="all_factors", strict=False)
    return all_factors


def run(
    cfg: config.Config,
    *,
    dump_interim: bool = False,
    blending_weights: pl.LazyFrame | None = None,
) -> None:
    """Run the pipeline end-to-end. One parquet per fan-out variant."""
    from rollup.seeds import load_all

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    seeds = load_all(cfg.seeds_dir)
    if blending_weights is not None:
        seeds = replace(seeds, blending_weights=blending_weights)
    validate_fx_coverage(seeds.fx_rates)
    fc_dates = forecast_dates_from_seed(seeds)
    tags = forecast_tags(fc_dates)
    variants = build_variants(fc_dates, cfg.vendors)
    log.info(f"plan: {len(variants)} Hisco variants across {len(cfg.vendors)} vendors")

    all_factors = build_all_factors(cfg, seeds)

    fanout_lfs = [fanout_hisco(all_factors, variant, min_loss=cfg.min_loss) for variant in variants]
    long_lf = audit_long(all_factors, tags, min_loss=cfg.min_loss)
    wide_lf = audit_wide(all_factors, tags) if dump_interim else None
    if cfg.min_loss > 0:
        log.info(f"min_loss filter: dropping rows where loss < {cfg.min_loss}")

    for lf, variant in zip(fanout_lfs, variants, strict=True):
        out_path = cfg.output_dir / f"{variant.name}.parquet"
        rows = _write_lazy_parquet(lf, out_path)
        log.info(f"fanout: wrote {variant.name}.parquet ({rows:,} rows)")

    long_path = cfg.output_dir / "mts_tbl_ylt_combined_all_factors.parquet"
    rows = _write_lazy_parquet(long_lf, long_path)
    log.info(f"wrote {long_path.name} ({rows:,} rows)")

    if dump_interim:
        debug_dir = cfg.output_dir / _AUDIT_SUBDIR
        debug_dir.mkdir(parents=True, exist_ok=True)
        _write_lazy_parquet(wide_lf, debug_dir / _AUDIT_WIDE_FILE)
        _write_lazy_parquet(long_lf, debug_dir / _AUDIT_LONG_FILE)
        log.info(f"audit: wrote {debug_dir / _AUDIT_WIDE_FILE}")
        log.info(f"audit: wrote {debug_dir / _AUDIT_LONG_FILE}")


def _write_lazy_parquet(lf: pl.LazyFrame, path: Path) -> int:
    """Write a LazyFrame directly to parquet and return the written row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path)
    return pl.scan_parquet(path).select(pl.len()).collect().item()
