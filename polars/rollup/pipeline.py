"""Orchestrator. Build a single cached `all_factors` LazyFrame, then fan out
into per-(vendor, forecast_date, flavor) Hisco parquet files in one optimized pass.
"""

from __future__ import annotations

import logging
import sys
from collections.abc import Sequence
from datetime import date
from typing import NamedTuple

import polars as pl

from rollup import config
from rollup.config import Flavor, Vendor, VendorName
from rollup.metrics.dialsup import add_dialsup
from rollup.schemas import frames as F
from rollup.chain import (
    CHAIN,
    CHAIN_BASE,
    audit_layout_cols,
    col_after,
    DIALSUP_COL,
    factor_col_for,
    main_loss_col,
)
from rollup.schemas.columns import (
    AllFactorsCol as AF,
    HiscoFanoutCol as H,
    MetricCol as M,
    NormalizedYltCol as Y,
    RefAirEventsCol as AE,
    RefForecastFactorsCol as FF,
)
from rollup.seeds import Seeds
from rollup.stages.factors import (
    attach_currency,
    attach_euws,
    attach_fagross,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
    validate_fx_coverage,
)
from rollup.stages.staging import (
    apply_rollup_scope,
    load_raw_risklink_ylt,
    load_raw_verisk_ylt,
    normalize_risklink_ylt,
    normalize_verisk_ylt,
)
from rollup.validate import validate_schema


log = logging.getLogger("rollup.pipeline")

_AUDIT_SUBDIR     = "debug"
_AUDIT_WIDE_FILE  = "audit_wide.parquet"
_AUDIT_LONG_FILE  = "audit_long.parquet"

# Working / temporary column names — exist only inside count_event_id_orphans.
_AE_MATCH_TMP    = "_ae_match"
_ORPHAN_COUNT    = "orphans"
_TOTAL_COUNT     = "total"

# Output column names that form part of the external contract for
# mts_tbl_ylt_combined_all_factors.parquet — kept as constants so any
# rename is a single-line edit traced through every reference.
_LOSS_RAW_COL    = "loss_raw"
_METRIC_NAME_COL = "metric_name"
_METRIC_VALUE_COL = "value"


def forecast_tags(forecast_dates: Sequence[date]) -> list[str]:
    """`[date(2026,1,1), date(2026,7,1)] → ['202601', '202607']`."""
    return sorted(set(d.strftime("%Y%m") for d in forecast_dates))


# --------------------------------------------------------------------------- #
# Variant spec                                                                #
# --------------------------------------------------------------------------- #

class VariantSpec(NamedTuple):
    """One Hisco fan-out output, as a typed triple.

    Forecast dates come from the `forecast_factors` seed at pipeline start
    — they're data, not code.
    """
    vendor:        Vendor
    forecast_date: date
    flavor:        Flavor

    @property
    def forecast_tag(self) -> str:
        """`date(2026, 1, 1)` → `"202601"`. Used in the output filename and
        in the `loss_uplifted_..._{year}` metric column names."""
        return self.forecast_date.strftime("%Y%m")

    @property
    def name(self) -> str:
        """Output filename (no extension).

        MAIN    → ``HiscoAIR_202601_main``   (includes forecast tag)
        DIALSUP → ``HiscoAIR_dialsup``       (no forecast tag — one file per vendor)
        """
        match self.flavor:
            case Flavor.MAIN:    return f"Hisco{self.vendor.hisco_label}_{self.forecast_tag}_{self.flavor.value}"
            case Flavor.DIALSUP: return f"Hisco{self.vendor.hisco_label}_{self.flavor.value}"

    @property
    def loss_metric(self) -> str:
        """The column in `all_factors` that feeds `Hisco.ModelGrossLoss`.

        MAIN    → final cumulative chain column (per `chain.CHAIN`).
        DIALSUP → ``"dialsup"`` — currency-converted raw loss, no factors applied.
        """
        match self.flavor:
            case Flavor.MAIN:    return main_loss_col(self.forecast_tag)
            case Flavor.DIALSUP: return DIALSUP_COL


def build_variants(
    forecast_dates: Sequence[date],
    vendors: Sequence[Vendor],
) -> list[VariantSpec]:
    """Build the set of Hisco fan-out outputs.

    For ``Flavor.MAIN``: one variant per (vendor × forecast_date).
    For ``Flavor.DIALSUP``: one variant per vendor only — the dialsup column
    is ``loss / rate_to_gbp``, independent of forecast date, so there is no
    reason to write one file per date.

    ``forecast_dates`` come from the ``forecast_factors`` seed at runtime;
    ``vendors`` from ``config.resolve().vendors``. Each vendor's own
    ``flavors`` tuple controls which Hisco outputs it emits.
    """
    unique_tags: list[str] = []
    seen_tags: set[str] = set()
    for d in forecast_dates:
        tag = d.strftime("%Y%m")
        if tag not in seen_tags:
            seen_tags.add(tag)
            unique_tags.append(tag)
    variants: list[VariantSpec] = []
    for v in vendors:
        for f in v.flavors:
            if f == Flavor.DIALSUP:
                if unique_tags:
                    variants.append(VariantSpec(
                        vendor=v,
                        forecast_date=forecast_dates[0],
                        flavor=f,
                    ))
            else:
                for tag in unique_tags:
                    for d in forecast_dates:
                        if d.strftime("%Y%m") == tag:
                            variants.append(VariantSpec(vendor=v, forecast_date=d, flavor=f))
                            break
    return variants


def forecast_dates_from_seed(seeds: Seeds) -> list[date]:
    """Distinct forecast dates carried by the forecast_factors seed."""
    return (
        seeds.forecast_factors
        .select(pl.col(FF.FORECAST_DATE))
        .unique()
        .sort(FF.FORECAST_DATE)
        .collect()
        .to_series()
        .to_list()
    )


# --------------------------------------------------------------------------- #
# Stage placeholders — implement in rollup/stages/*.py                        #
# --------------------------------------------------------------------------- #

def count_event_id_orphans(
    ylt: pl.LazyFrame,
    air_events: pl.LazyFrame,
    *,
    vendor_filter: VendorName = VendorName.VERISK,
) -> int:
    """Count (year_id, event_id, model_code) triples in the YLT that are
    NOT present in `air_events`. Logs a warning if any orphans are found
    and returns the count.

    This is observation-only, not a guard — orphans don't abort the run
    because the rollup math doesn't depend on `air_events`. The count is
    surfaced so a downstream check (e.g. cron alert) can act on it.
    """
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
    total   = collected[_TOTAL_COUNT].item()
    orphans = collected[_ORPHAN_COUNT].item()
    if orphans:
        log.warning(
            f"event-id orphans for vendor={vendor_filter}: "
            f"{orphans:,} / {total:,} YLT rows have no match in air_events"
        )
    else:
        log.info(f"event-id check ({vendor_filter}): {total:,}/{total:,} rows matched air_events")
    return orphans


def build_all_factors(cfg: config.Config, seeds: Seeds) -> pl.LazyFrame:
    """Factor chain: staging → factors → metrics, composed with .pipe().

    Each .pipe() call is one stage; the function name is the stage name.
    See stages/factors.py for the 5-step recipe to add a new factor.
    """
    verisk   = cfg.vendor(VendorName.VERISK)
    risklink = cfg.vendor(VendorName.RISKLINK)
    tags     = forecast_tags(forecast_dates_from_seed(seeds))
    n_sim: dict[VendorName, int] = {
        VendorName.VERISK:   verisk.n_simulations,
        VendorName.RISKLINK: risklink.n_simulations,
    }
    log.info(f"forecast tags from seed: {tags}")

    rl_norm = normalize_risklink_ylt(
        load_raw_risklink_ylt(risklink.ylt_dir, glob=risklink.ylt_glob),
        seeds.analyses, seeds.perils, seeds.lobs,
    )
    vk_norm = normalize_verisk_ylt(
        load_raw_verisk_ylt(verisk.ylt_dir, glob=verisk.ylt_glob),
        seeds.analyses, seeds.perils, seeds.lobs,
    )
    ylt = pl.concat([rl_norm, vk_norm], how="vertical")
    log.info("staging: normalised YLTs concatenated")
    count_event_id_orphans(ylt, seeds.air_events, vendor_filter=VendorName.VERISK)

    all_factors = (
        ylt
        .pipe(apply_rollup_scope,      seeds.rollup_scope)                  # drop rows not in official scope
        .pipe(attach_currency,         seeds.fx_rates)
        .pipe(attach_forecast_factors, seeds.forecast_factors, tags)
        .pipe(attach_rank,            n_sim=n_sim)                         # must precede attach_euws
        .pipe(attach_euws,             seeds.euws_rate_factors, seeds.euws_rank_overrides)
        .pipe(attach_fagross,          seeds.fineart_adjustments)
        .pipe(attach_uplift,           seeds.blending_weights, n_sim=n_sim)
        .pipe(_compute_metrics,        tags)                                # needs all attach_* outputs
        .pipe(add_dialsup)                                                  # needs _compute_metrics output
    )
    log.info(f"metrics: {3 + 3 * len(tags)} derived loss columns + 1 dialsup column")
    validate_schema(all_factors, F.ALL_FACTORS, name="all_factors", strict=False)
    return all_factors


def _compute_metrics(ylt: pl.LazyFrame, tags: Sequence[str]) -> pl.LazyFrame:
    """Year-invariant chain (uplift → cap → fx) + year-tagged chain per tag.

    The year-tagged chain is driven by `chain.CHAIN` — each stage multiplies
    its `factor_col` into the previous cumulative column. Adding a stage =
    one entry in CHAIN, no edits here.
    """
    # Year-invariant chain
    ylt = ylt.with_columns(
        (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR))       .alias(M.LOSS_UPLIFTED),
        (pl.col(Y.LOSS) * pl.col(AF.UPLIFT_FACTOR_CAPPED)).alias(M.LOSS_UPLIFTED_CAPPED),
    ).with_columns(
        (pl.col(M.LOSS_UPLIFTED_CAPPED) / pl.col(AF.RATE_TO_GBP)).alias(CHAIN_BASE),
    )
    # Year-tagged chain — walk the registry per stage, batching all tags into
    # one with_columns per stage. This reduces plan-node overhead from N×M to M
    # (3 stages × N tags → 3 with_columns calls). Correctness is preserved
    # because within each stage the inputs are the previous stage's outputs,
    # which were materialised in the preceding with_columns call, not in the
    # same one. Across tags the expressions are independent — they only share
    # the year-invariant CHAIN_BASE column and the per-tag factor columns.
    prev_for: dict[str, str] = {tag: CHAIN_BASE for tag in tags}
    for stage_name, stage in CHAIN.items():
        exprs = [
            (pl.col(prev_for[tag]) * pl.col(factor_col_for(stage, tag))).alias(col_after(stage_name, tag))
            for tag in tags
        ]
        ylt = ylt.with_columns(exprs)
        for tag in tags:
            prev_for[tag] = col_after(stage_name, tag)
    return ylt


# --------------------------------------------------------------------------- #
# Interim audit dumps                                                         #
#                                                                             #
# Two artefacts, written under `<output_dir>/debug/` when `--dump-interim` is #
# set. Both are projections of `all_factors` — no extra computation.          #
#                                                                             #
#   audit_wide.parquet                                                        #
#     One row per YLT event, columns ordered so the factor chain reads        #
#     left-to-right: raw loss → uplift → capped → localccy → (f_{year} →      #
#     loss_...) → (euws → loss_..._euws) → (fa → loss_..._euws_fagross) →     #
#     dialsup. Every factor sits next to the metric it produces — you can     #
#     literally read across one row and verify each multiplication.           #
#                                                                             #
#   audit_long.parquet                                                        #
#     Same identity columns + one row per (metric_name, value). For pivot-    #
#     table analysis against january's excel EP summaries.                    #
# --------------------------------------------------------------------------- #

_IDENTITY_COLS: tuple[str, ...] = (
    AF.VENDOR, AF.LOB_ID, AF.MODELLED_LOB, AF.ROLLUP_LOB, AF.LOB_TYPE,
    AF.CDS_CAT_CLASS_NAME,
    AF.REGION_PERIL_ID, AF.MODELLED_REGION_PERIL,
    AF.PERIL_NAME, AF.REGION, AF.PERIL_FAMILY,
    AF.YEAR_ID, AF.EVENT_ID, AF.MODEL_EVENT_ID, AF.MODEL_CODE,
    AF.RNK, AF.RP, AF.RP_BUCKET,
    AF.RL_PROPORTION, AF.VK_PROPORTION, AF.BASE_MODEL,
)


def _metric_cols_for(tags: Sequence[str]) -> list[str]:
    """Every metric column this pipeline produces — driven by `chain.CHAIN`."""
    cols: list[str] = [
        Y.LOSS,
        M.LOSS_UPLIFTED, M.LOSS_UPLIFTED_CAPPED, M.LOSS_UPLIFTED_CAPPED_LOCALCCY,
    ]
    for stage_name in CHAIN:
        cols += [col_after(stage_name, t) for t in tags]
    cols.append(DIALSUP_COL)
    return cols


def audit_wide(all_factors: pl.LazyFrame, tags: Sequence[str]) -> pl.LazyFrame:
    """One row per event, columns ordered so the factor chain reads left-to-right.

    Layout: [identity] [raw loss] [year-invariant chain: uplift + cap + fx]
    [year-tagged chain via `chain.audit_layout_cols`] [dialsup per tag].

    The year-tagged section is registry-driven — adding a `ChainStage` to
    `chain.CHAIN` automatically extends the audit layout. No edits here.
    """
    seen: set[str] = set(_IDENTITY_COLS)
    cols: list[pl.Expr] = [pl.col(c) for c in _IDENTITY_COLS]
    cols.append(pl.col(Y.LOSS).alias(_LOSS_RAW_COL))

    # Year-invariant chain: uplift → cap → fx  (blend factors already in _IDENTITY_COLS)
    cols += [
        pl.col(AF.UPLIFT_FACTOR), pl.col(AF.UPLIFT_FACTOR_CAPPED),
        pl.col(M.LOSS_UPLIFTED), pl.col(M.LOSS_UPLIFTED_CAPPED),
        pl.col(AF.REQUIRED_CURRENCY), pl.col(AF.RATE_TO_GBP),
        pl.col(CHAIN_BASE),
    ]

    # Year-tagged chain — driven by the registry, not by hand-listed columns
    for c in audit_layout_cols(list(tags)):
        if c not in seen:
            cols.append(pl.col(c))
            seen.add(c)

    # Dialsup sensitivity — single column (no per-tag emission; formula is tag-independent)
    cols.append(pl.col(DIALSUP_COL))

    return all_factors.select(cols).sort(
        [Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID, Y.YEAR_ID, Y.EVENT_ID],
    )


def audit_long(
    all_factors: pl.LazyFrame,
    tags: Sequence[str],
    *,
    min_loss: float = 0.0,
) -> pl.LazyFrame:
    """Identity columns + one row per (metric_name, value). Pivot-friendly.

    `min_loss > 0` drops rows where the metric value is below the threshold.
    Applied per-row (not per-event), so a metric row at 500 is dropped even
    if the same event has another metric > 1000.
    """
    metric_cols = _metric_cols_for(tags)
    out = (
        all_factors
        .select(*[pl.col(c) for c in _IDENTITY_COLS], *[pl.col(c) for c in metric_cols])
        .unpivot(
            on=metric_cols,
            index=list(_IDENTITY_COLS),
            variable_name=_METRIC_NAME_COL,
            value_name=_METRIC_VALUE_COL,
        )
    )
    if min_loss > 0:
        out = out.filter(pl.col(_METRIC_VALUE_COL) >= min_loss)
    return out.sort([Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID, Y.YEAR_ID, Y.EVENT_ID, _METRIC_NAME_COL])


def fanout_hisco(
    all_factors: pl.LazyFrame,
    variant: VariantSpec,
    *,
    min_loss: float = 0.0,
) -> pl.LazyFrame:
    """Project all_factors → one Hisco variant.

    Filters to this variant's vendor and picks the loss-metric column
    implied by the flavor. Both FAGROSS and DIALSUP columns are built
    dynamically in `build_all_factors` per forecast tag.

    `min_loss > 0` drops rows where the variant's `ModelGrossLoss` is below
    the threshold — small-loss events that bloat parquet size without
    contributing meaningfully to the EP curve.
    """
    out = (
        all_factors
        .filter(pl.col(AF.BASE_MODEL) == variant.vendor.name)
        .select(
            pl.col(AF.MODEL_EVENT_ID).alias(H.MODEL_EVENT_ID),
            pl.col(AF.YEAR_ID).alias(H.MODEL_YEAR),
            pl.col(AF.REQUIRED_CURRENCY).alias(H.CURRENCY_CODE),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_YOA),
            pl.col(variant.loss_metric).alias(H.MODEL_GROSS_LOSS),
            pl.lit(0, dtype=pl.Int32).alias(H.MODEL_INWARDS_REINSTATEMENT),
            pl.lit(0, dtype=pl.Int64).alias(H.MODEL_EVENT_DAY),
            pl.col(AF.CDS_CAT_CLASS_NAME).alias(H.LOSS_CLASS_NAME),
        )
    )
    if min_loss > 0:
        out = out.filter(pl.col(H.MODEL_GROSS_LOSS) >= min_loss)
    validate_schema(out, F.HISCO_FANOUT, name=f"fanout.{variant.name}")
    return out


# --------------------------------------------------------------------------- #
# Entry point                                                                 #
# --------------------------------------------------------------------------- #

def run(cfg: config.Config, *, dump_interim: bool = False) -> None:
    """Run the pipeline end-to-end. One parquet per fan-out variant.

    When `dump_interim=True`, also writes `audit_wide.parquet` and
    `audit_long.parquet` under `<output_dir>/debug/` — one row per event with
    every factor and metric side by side, for read-across verification.
    """
    from rollup.seeds import load_all

    cfg.output_dir.mkdir(parents=True, exist_ok=True)

    seeds     = load_all(cfg.seeds_dir)
    validate_fx_coverage(seeds.fx_rates)   # fast: small seed; aborts early on misconfiguration
    fc_dates  = forecast_dates_from_seed(seeds)
    tags      = forecast_tags(fc_dates)
    variants  = build_variants(fc_dates, cfg.vendors)
    log.info(f"plan: {len(variants)} Hisco variants across {len(cfg.vendors)} "
             f"vendors × {len(fc_dates)} forecast dates × flavours")

    all_factors = build_all_factors(cfg, seeds).cache()

    # Build each lazy plan once; reuse the same reference where the same
    # output appears in two places (audit_long is both the production
    # combined_factors_long output and the --dump-interim debug copy).
    fanout_lfs   = [fanout_hisco(all_factors, v, min_loss=cfg.min_loss) for v in variants]
    long_lf      = audit_long(all_factors, tags, min_loss=cfg.min_loss)
    wide_lf      = audit_wide(all_factors, tags) if dump_interim else None
    if cfg.min_loss > 0:
        log.info(f"min_loss filter: dropping rows where loss < {cfg.min_loss}")

    plan_lfs: list[pl.LazyFrame] = list(fanout_lfs)
    long_idx = len(plan_lfs)
    plan_lfs.append(long_lf)
    wide_idx = None
    if wide_lf is not None:
        wide_idx = len(plan_lfs)
        plan_lfs.append(wide_lf)

    collected = pl.collect_all(plan_lfs)

    fanout_dfs = collected[:len(variants)]
    long_df    = collected[long_idx]
    wide_df    = collected[wide_idx] if wide_idx is not None else None

    for df, variant in zip(fanout_dfs, variants, strict=True):
        out_path = cfg.output_dir / f"{variant.name}.parquet"
        df.write_parquet(out_path)
        log.info(f"fanout: wrote {variant.name}.parquet ({df.height:,} rows)")

    long_path = cfg.output_dir / "mts_tbl_ylt_combined_all_factors.parquet"
    long_df.write_parquet(long_path)
    log.info(f"wrote {long_path.name} ({long_df.height:,} rows)")

    if dump_interim:
        debug_dir = cfg.output_dir / _AUDIT_SUBDIR
        debug_dir.mkdir(parents=True, exist_ok=True)
        wide_df.write_parquet(debug_dir / _AUDIT_WIDE_FILE)
        long_df.write_parquet(debug_dir / _AUDIT_LONG_FILE)
        log.info(f"audit: wrote {debug_dir / _AUDIT_WIDE_FILE}")
        log.info(f"audit: wrote {debug_dir / _AUDIT_LONG_FILE}")
