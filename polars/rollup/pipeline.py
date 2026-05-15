"""Linear dbt-style pipeline orchestrator.

The flow is intentionally visible in this file:

    seeds + raw data -> staging -> intermediate -> marts -> reports/outputs

Model functions build Polars LazyFrames. This orchestrator decides where to
collect for validation, final parquet writes, audit outputs, and report files.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field, replace
from pathlib import Path

import polars as pl

from rollup import config
from rollup.audit import audit_long, audit_wide
from rollup.config import VendorName
from rollup.intermediate import (
    add_dialsup,
    add_main_metrics,
    attach_currency,
    attach_euws,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
    validate_fx_coverage,
)
from rollup.io.report_writer import write_report
from rollup.marts import VariantSpec, build_variants, fanout_hisco, forecast_dates_from_seed, forecast_tags
from rollup.reports import build_report
from rollup.schemas import frames as F
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RefAirEventsCol as AE
from rollup.schemas.columns import RefRisklinkEventsCol as RLE
from rollup.seeds import Seeds
from rollup.staging import (
    effective_analyses_for_run,
    load_raw_risklink_ylt,
    load_raw_verisk_ylt,
    normalize_risklink_ylt,
    normalize_verisk_ylt,
    validate_one_peril_per_rollup_lob,
)
from rollup.validate import validate_schema


log = logging.getLogger("rollup.pipeline")

_AUDIT_SUBDIR = "debug"
_AUDIT_WIDE_FILE = "audit_wide.parquet"
_AUDIT_LONG_FILE = "audit_long.parquet"

_AE_MATCH_TMP = "_ae_match"
_ORPHAN_COUNT = "orphans"
_TOTAL_COUNT = "total"


@dataclass
class RawModels:
    """Raw vendor scans attached before any staging joins are applied."""

    risklink_ylt: pl.LazyFrame | None = None
    verisk_ylt: pl.LazyFrame | None = None


@dataclass
class StagingModels:
    """Typed staging LazyFrames produced from seeds and raw vendor YLT scans."""

    valid_analyses: pl.LazyFrame | None = None
    risklink_ylt: pl.LazyFrame | None = None
    verisk_ylt: pl.LazyFrame | None = None
    ylt: pl.LazyFrame | None = None


@dataclass
class IntermediateModels:
    """Business-calculation LazyFrames built from staging models."""

    all_factors: pl.LazyFrame | None = None


@dataclass(frozen=True)
class MartModels:
    """Output-shaped LazyFrames ready for materialisation."""

    variants: list[VariantSpec]
    fanouts: list[pl.LazyFrame]
    audit_long: pl.LazyFrame
    audit_wide: pl.LazyFrame | None = None


@dataclass(frozen=True)
class CollectedMarts:
    """Eager DataFrames collected at the named mart output boundary."""

    fanouts: list[pl.DataFrame]
    audit_long: pl.DataFrame
    audit_wide: pl.DataFrame | None


@dataclass
class PipelineContext:
    """Mutable dataflow state for the dbt-style pipeline orchestration.

    The context is intentionally only a set of typed namespaces. It has no
    build/run methods; ``run()`` wires each LazyFrame transformation explicitly.
    """

    cfg: config.Config
    seeds: Seeds | None = None
    raw: RawModels = field(default_factory=RawModels)
    staging: StagingModels = field(default_factory=StagingModels)
    intermediate: IntermediateModels = field(default_factory=IntermediateModels)
    marts: MartModels | None = None


def count_event_id_orphans(
    ylt: pl.LazyFrame,
    air_events: pl.LazyFrame,
    *,
    vendor_filter: VendorName = VendorName.VERISK,
) -> int:
    """Count Verisk-style event IDs that are absent from the air_events seed."""
    ae = air_events.select(
        pl.col(AE.YEAR).alias(Y.YEAR_ID),
        pl.col(AE.EVENT).alias(Y.EVENT_ID),
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
            "data/seeds/validation/verisk_events.parquet. Calculations continue, "
            "but ModelEventDay cannot be enriched and AIR event metadata is not validated. "
            "Provide verisk_events.parquet to validate/enrich event IDs."
        )
    else:
        log.info(f"event-id check ({vendor_filter}): {total:,}/{total:,} rows matched air_events")
    return orphans


def count_risklink_event_id_orphans(
    ylt: pl.LazyFrame,
    risklink_events: pl.LazyFrame,
) -> int:
    """Count RiskLink event/year pairs absent from the risklink_events seed."""
    events = risklink_events.select(
        pl.col(RLE.YEAR).alias(Y.YEAR_ID),
        pl.col(RLE.EVENT_ID),
    ).with_columns(pl.lit(True).alias(_AE_MATCH_TMP))

    joined = (
        ylt.filter(pl.col(Y.VENDOR) == VendorName.RISKLINK)
        .join(events, on=[Y.YEAR_ID, Y.EVENT_ID], how="left")
    )
    collected = joined.select(
        pl.len().alias(_TOTAL_COUNT),
        pl.col(_AE_MATCH_TMP).is_null().sum().alias(_ORPHAN_COUNT),
    ).collect()
    total = collected[_TOTAL_COUNT].item()
    orphans = collected[_ORPHAN_COUNT].item()
    if orphans:
        log.warning(
            "event catalogue validation incomplete for vendor=risklink: "
            f"{orphans:,} / {total:,} YLT rows did not match "
            "data/seeds/validation/risklink_flood22_model_events.parquet. "
            "Calculations continue, but RiskLink event metadata is not validated."
        )
    else:
        log.info(f"event-id check (risklink): {total:,}/{total:,} rows matched risklink_events")
    return orphans


def build_staging(cfg: config.Config, seeds: Seeds) -> StagingModels:
    """Build staging models from raw vendor inputs and seed dimensions."""
    verisk = cfg.vendor(VendorName.VERISK)
    risklink = cfg.vendor(VendorName.RISKLINK)
    analyses = effective_analyses_for_run(cfg.seeds_dir, seeds.analyses, seeds.valid_analyses)
    raw_risklink_ylt = load_raw_risklink_ylt(risklink.ylt_dir, glob=risklink.ylt_glob)
    raw_verisk_ylt = load_raw_verisk_ylt(verisk.ylt_dir, glob=verisk.ylt_glob)

    rl_norm = normalize_risklink_ylt(
        raw_risklink_ylt,
        analyses,
        seeds.perils,
        seeds.lobs,
    )
    vk_norm = normalize_verisk_ylt(
        raw_verisk_ylt,
        analyses,
        seeds.perils,
        seeds.lobs,
    )
    ylt = pl.concat([rl_norm, vk_norm], how="vertical")
    log.info("staging: normalised YLTs concatenated")
    return StagingModels(
        valid_analyses=analyses,
        risklink_ylt=rl_norm,
        verisk_ylt=vk_norm,
        ylt=ylt,
    )


def validate_staging(staging: StagingModels, seeds: Seeds) -> None:
    """Collect only the explicit staging validation checks."""
    if staging.ylt is None:
        raise ValueError("staging.ylt must be attached before staging validation")
    validate_one_peril_per_rollup_lob(staging.ylt)
    count_event_id_orphans(staging.ylt, seeds.air_events, vendor_filter=VendorName.VERISK)
    count_risklink_event_id_orphans(staging.ylt, seeds.risklink_events)


def build_intermediate(
    cfg: config.Config,
    seeds: Seeds,
    staging: StagingModels,
    tags: list[str],
) -> IntermediateModels:
    """Build intermediate factor and metric models from staging models."""
    if staging.ylt is None:
        raise ValueError("staging.ylt must be attached before building intermediate models")
    n_sim: dict[VendorName, int] = {
        VendorName.VERISK: cfg.vendor(VendorName.VERISK).n_simulations,
        VendorName.RISKLINK: cfg.vendor(VendorName.RISKLINK).n_simulations,
    }
    log.info(f"forecast tags from seed: {tags}")

    all_factors = (
        staging.ylt
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
    return IntermediateModels(all_factors=all_factors)


def build_marts(
    cfg: config.Config,
    seeds: Seeds,
    intermediate: IntermediateModels,
    variants: list[VariantSpec],
    tags: list[str],
    *,
    dump_interim: bool,
) -> MartModels:
    """Build mart LazyFrames for Hisco fanout and audit outputs."""
    if intermediate.all_factors is None:
        raise ValueError("intermediate.all_factors must be attached before building marts")
    all_factors = intermediate.all_factors.cache()

    fanouts = [
        fanout_hisco(
            all_factors,
            variant,
            min_loss=cfg.min_loss,
            air_events=seeds.air_events,
            risklink_events=seeds.risklink_events,
        )
        for variant in variants
    ]
    long_lf = audit_long(all_factors, tags, min_loss=cfg.min_loss)
    wide_lf = audit_wide(all_factors, tags) if dump_interim else None
    if cfg.min_loss > 0:
        log.info(f"min_loss filter: dropping rows where loss < {cfg.min_loss}")

    return MartModels(
        variants=variants,
        fanouts=fanouts,
        audit_long=long_lf,
        audit_wide=wide_lf,
    )


def collect_marts(marts: MartModels) -> CollectedMarts:
    """Named collection boundary for final mart parquet outputs."""
    plan_lfs: list[pl.LazyFrame] = list(marts.fanouts)
    long_idx = len(plan_lfs)
    plan_lfs.append(marts.audit_long)
    wide_idx = None
    if marts.audit_wide is not None:
        wide_idx = len(plan_lfs)
        plan_lfs.append(marts.audit_wide)

    collected = pl.collect_all(plan_lfs)
    return CollectedMarts(
        fanouts=collected[:len(marts.variants)],
        audit_long=collected[long_idx],
        audit_wide=collected[wide_idx] if wide_idx is not None else None,
    )


def write_marts(cfg: config.Config, marts: MartModels, collected: CollectedMarts) -> None:
    """Write collected mart outputs to parquet sinks."""
    for df, variant in zip(collected.fanouts, marts.variants, strict=True):
        out_path = cfg.output_dir / f"{variant.name}.parquet"
        df.write_parquet(out_path)
        log.info(f"fanout: wrote {variant.name}.parquet ({df.height:,} rows)")

    long_path = cfg.output_dir / "mts_tbl_ylt_combined_all_factors.parquet"
    collected.audit_long.write_parquet(long_path)
    log.info(f"wrote {long_path.name} ({collected.audit_long.height:,} rows)")


def write_debug_outputs(cfg: config.Config, collected: CollectedMarts) -> None:
    """Write optional debug/audit output artifacts."""
    if collected.audit_wide is None:
        return

    debug_dir = cfg.output_dir / _AUDIT_SUBDIR
    debug_dir.mkdir(parents=True, exist_ok=True)
    collected.audit_wide.write_parquet(debug_dir / _AUDIT_WIDE_FILE)
    collected.audit_long.write_parquet(debug_dir / _AUDIT_LONG_FILE)
    log.info(f"audit: wrote {debug_dir / _AUDIT_WIDE_FILE}")
    log.info(f"audit: wrote {debug_dir / _AUDIT_LONG_FILE}")


def write_reports(
    cfg: config.Config,
    intermediate: IntermediateModels,
    variants: list[VariantSpec],
) -> None:
    """Collect and write presentation report artifacts after mart outputs."""
    if intermediate.all_factors is None:
        raise ValueError("intermediate.all_factors must be attached before writing reports")
    try:
        report = build_report(intermediate.all_factors, variants)
        write_report(report, cfg.output_dir)
    except Exception as e:
        log.error(f"report: failed to generate end-of-run summary ({type(e).__name__}: {e})")


def _load_seeds(cfg: config.Config, blending_weights: pl.LazyFrame | None) -> Seeds:
    """Load seed references and apply optional scenario seed overrides."""
    from rollup.seeds import load_all

    seeds = load_all(cfg.seeds_dir)
    if blending_weights is not None:
        # Runtime/scenario runs may inject reviewed blending weights without
        # writing a CSV. Only the blending_weights seed frame is replaced; every
        # other seed remains the validated frame loaded from cfg.seeds_dir.
        seeds = replace(seeds, blending_weights=blending_weights)
    validate_fx_coverage(seeds.fx_rates)
    return seeds


def run(
    cfg: config.Config,
    *,
    dump_interim: bool = False,
    blending_weights: pl.LazyFrame | None = None,
) -> None:
    """Run the pipeline end-to-end. One parquet per fan-out variant."""
    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    ctx = PipelineContext(cfg=cfg)

    ctx.seeds = _load_seeds(cfg, blending_weights)

    # Forecast dates are the explicit planning collection boundary: variants and
    # metric column names must be known before the final lazy mart plans are built.
    fc_dates = forecast_dates_from_seed(ctx.seeds)
    tags = forecast_tags(fc_dates)
    variants = build_variants(fc_dates, cfg.vendors)
    log.info(f"plan: {len(variants)} Hisco variants across {len(cfg.vendors)} vendors")

    ctx.staging.valid_analyses = effective_analyses_for_run(
        cfg.seeds_dir,
        ctx.seeds.analyses,
        ctx.seeds.valid_analyses,
    )

    risklink = cfg.vendor(VendorName.RISKLINK)
    verisk = cfg.vendor(VendorName.VERISK)

    ctx.raw.risklink_ylt = load_raw_risklink_ylt(risklink.ylt_dir, glob=risklink.ylt_glob)
    ctx.raw.verisk_ylt = load_raw_verisk_ylt(verisk.ylt_dir, glob=verisk.ylt_glob)

    ctx.staging.risklink_ylt = normalize_risklink_ylt(
        ctx.raw.risklink_ylt,
        ctx.staging.valid_analyses,
        ctx.seeds.perils,
        ctx.seeds.lobs,
    )
    ctx.staging.verisk_ylt = normalize_verisk_ylt(
        ctx.raw.verisk_ylt,
        ctx.staging.valid_analyses,
        ctx.seeds.perils,
        ctx.seeds.lobs,
    )
    ctx.staging.ylt = pl.concat([ctx.staging.risklink_ylt, ctx.staging.verisk_ylt], how="vertical")
    log.info("staging: normalised YLTs concatenated")

    validate_staging(ctx.staging, ctx.seeds)

    ctx.intermediate = build_intermediate(cfg, ctx.seeds, ctx.staging, tags)
    ctx.marts = build_marts(cfg, ctx.seeds, ctx.intermediate, variants, tags, dump_interim=dump_interim)

    collected = collect_marts(ctx.marts)
    write_marts(cfg, ctx.marts, collected)
    write_debug_outputs(cfg, collected)
    write_reports(cfg, ctx.intermediate, variants)


def _write_lazy_parquet(lf: pl.LazyFrame, path: Path) -> int:
    """Write a LazyFrame directly to parquet and return the written row count."""
    path.parent.mkdir(parents=True, exist_ok=True)
    lf.sink_parquet(path)
    return pl.scan_parquet(path).select(pl.len()).collect().item()
