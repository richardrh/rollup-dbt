"""Intermediate factor models.

Each factor is one function: takes a LazyFrame + the seed(s) it needs,
returns the LazyFrame with one or more new factor columns attached. The
composition in `pipeline.build_intermediate` calls them in order; the
cumulative factor chain is visible in the output column names.

==============================================================================
ADDING A NEW FACTOR — the 5-step recipe
==============================================================================

Say you want to add a `broker_commission_factor`. Do this:

  1. Add the seed: `data/seeds/broker_commissions.csv` with columns
     (lob_id, commission_factor). Add a `SeedSpec` in `rollup/seeds.py` +
     a `pl.Schema` in `rollup/schemas/frames.py` + a `StrEnum` in
     `rollup/schemas/columns.py`.

  2. Add a factor scalar to `AllFactorsCol`:
         BROKER_COMMISSION_FACTOR = "broker_commission_factor"

  3. Write `attach_broker_commission(ylt, broker_commissions)` below. Keep
     it small — a select + a left join + a fill_null(1.0).

  4. Call it from `pipeline.build_intermediate` in the right order (before
     metrics; usually: after EUWS, before dialsup).

  5. Pick a column-name suffix (e.g. `_brokercomm`). In the metrics loop
     inside `rollup.intermediate.metrics`, chain the new factor in:
         loss_..._{tag}_euws_brokercomm =
           loss_..._{tag}_euws * broker_commission_factor
     Update `audit_wide`'s ordering so the factor sits next to the metric
     it produced.

That's it. No other files need to change.
==============================================================================
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import polars as pl

from rollup.chain import forecast_factor_col
from rollup.config import CurrencyCode, VendorName
from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import BlendingWeightsCol as BW
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.schemas.columns import RefEuwsRankOverridesCol as EO
from rollup.schemas.columns import RefEuwsRateFactorsCol as EU
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefFxRatesCol as FX


log = logging.getLogger("rollup.intermediate.factors")

# Working / temporary column names — exist only inside one stage. Kept as
# module-level constants so every reference is traceable from `pl.col(...)`.
_EUWS_OVERRIDE_TMP   = "_euws_override"
_VK_AAL_TMP          = "_vk_aal"
_RL_AAL_TMP          = "_rl_aal"
_BLENDED_AAL_TMP     = "_blended_aal"
_BASE_MODEL_AAL_TMP  = "_base_model_aal"
_SPECIFIC_VK_PROP    = "_specific_vk_proportion"
_SPECIFIC_RL_PROP    = "_specific_rl_proportion"
_GENERIC_VK_PROP     = "_generic_vk_proportion"
_GENERIC_RL_PROP     = "_generic_rl_proportion"
_SPECIFIC_BASE_MODEL = "_specific_base_model"
_GENERIC_BASE_MODEL  = "_generic_base_model"

# Return-period bucket thresholds — adapted from jan-rollup's
# int_vw_funnel_ylt_combined_ranked_bucketed.
#   rp < 200            → bucket 0     (AAL)
#   200 ≤ rp < 1000     → bucket 200   (1-in-200)
#   1000 ≤ rp < 10000   → bucket 1000  (1-in-1000)
#   rp ≥ 10000          → bucket 10000 (1-in-10000 tail)


def _rp_bucket_expr(rp_col: str) -> pl.Expr:
    return (
        pl.when(pl.col(rp_col) < 200)
          .then(pl.lit(0))
          .when(pl.col(rp_col) < 1000)
          .then(pl.lit(200))
          .when(pl.col(rp_col) < 10_000)
          .then(pl.lit(1000))
          .otherwise(pl.lit(10_000))
    )


def _resolve_n_sim(n_sim: dict[VendorName, int] | None) -> dict[VendorName, int]:
    return n_sim or {VendorName.VERISK: 10_000, VendorName.RISKLINK: 100_000}


def _with_rank_and_rp_bucket(
    ylt: pl.LazyFrame,
    *,
    n_sim: dict[VendorName, int] | None = None,
) -> pl.LazyFrame:
    """Ensure rnk, rp, and rp_bucket columns exist for blending lookup."""
    n_sim_resolved = _resolve_n_sim(n_sim)
    schema_names = set(ylt.collect_schema().names())

    out = ylt
    if AF.RNK not in schema_names:
        out = out.with_columns(
            pl.col(Y.LOSS).rank(method="ordinal", descending=True)
            .over([Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID])
            .alias(AF.RNK),
        )

    if AF.RP not in schema_names:
        n_sim_vk = float(n_sim_resolved[VendorName.VERISK])
        n_sim_rl = float(n_sim_resolved[VendorName.RISKLINK])
        out = out.with_columns(
            pl.when(pl.col(Y.VENDOR) == VendorName.VERISK)
            .then(pl.lit(n_sim_vk) / pl.col(AF.RNK))
            .otherwise(pl.lit(n_sim_rl) / pl.col(AF.RNK))
            .cast(pl.Float64)
            .alias(AF.RP),
        )

    if AF.RP_BUCKET not in schema_names:
        out = out.with_columns(_rp_bucket_expr(AF.RP).cast(pl.Int64).alias(AF.RP_BUCKET))

    return out


class MissingFxRateError(RuntimeError):
    """A row required an FX rate that was not present in `fx_rates.csv`."""


def validate_fx_coverage(fx_rates: pl.LazyFrame) -> None:
    """Check that `fx_rates` contains a GBP rate for every `CurrencyCode` member.

    Call once at pipeline startup (before building the factor chain) so a
    misconfigured seed is caught immediately — not mid-computation after an
    expensive YLT join has already materialised.

    Raises `MissingFxRateError` if any member of the closed `CurrencyCode`
    enum has no row with `target_currency=GBP`.
    """
    have = set(
        fx_rates
        .filter(pl.col(FX.TARGET_CURRENCY) == CurrencyCode.GBP)
        .select(pl.col(FX.CURRENCY_CODE))
        .collect()
        .to_series()
        .to_list()
    )
    missing = {c.value for c in CurrencyCode} - have
    if missing:
        raise MissingFxRateError(
            f"fx_rates.csv has no GBP rate for currencies: {sorted(missing)}. "
            f"Add one row per missing code (currency_code,target_currency=GBP,rate_date,rate)."
        )


def attach_currency(ylt: pl.LazyFrame, fx_rates: pl.LazyFrame) -> pl.LazyFrame:
    """Surface the LOB's local currency as `required_currency`, then join fx_rates
    to attach `rate_to_gbp`.

    `currency` rides on every NormalizedYlt row from the lobs join in staging —
    the seed owner is the source of truth (`lobs.csv::currency`). This used to
    derive currency at runtime from substring matches on `cds_cat_class_name`;
    renaming a class for branding silently broke FX, so the rule was moved
    onto the seed.

    Add a row to `fx_rates.csv` for every currency code that appears in
    `lobs.csv`, otherwise `validate_fx_coverage` (called at pipeline startup)
    will abort the run before any computation begins.
    """
    ylt = ylt.with_columns(pl.col(Y.CURRENCY).alias(AF.REQUIRED_CURRENCY))
    fx_to_gbp = (
        fx_rates
        .filter(pl.col(FX.TARGET_CURRENCY) == CurrencyCode.GBP)
        .select(
            pl.col(FX.CURRENCY_CODE).alias(AF.REQUIRED_CURRENCY),
            pl.col(FX.RATE).alias(AF.RATE_TO_GBP),
        )
    )
    out = ylt.join(fx_to_gbp, on=AF.REQUIRED_CURRENCY, how="left")
    log.info("currency: required_currency taken from lobs.currency; rate_to_gbp attached")
    return out.with_columns(pl.col(AF.RATE_TO_GBP).cast(pl.Float64))


def _forecast_factor_expr(tag: str) -> pl.Expr:
    y_, m_ = int(tag[:4]), int(tag[4:6])
    return (
        pl.col(FF.FACTOR)
        .filter(
            (pl.col(FF.FORECAST_DATE).dt.year() == y_)
            & (pl.col(FF.FORECAST_DATE).dt.month() == m_)
        )
        .first()
        .cast(pl.Float64)
        .alias(forecast_factor_col(tag))
    )


def attach_forecast_factors(
    ylt: pl.LazyFrame,
    forecast_factors: pl.LazyFrame,
    tags: Sequence[str],
) -> pl.LazyFrame:
    """One `f_{tag}` column per tag in `tags`. Tags come from
    `forecast_dates_from_seed(seeds)` at runtime — add a date to the seed,
    a new column appears here automatically.

    Joins on (office, lob_class) which ride on the NormalizedYlt frame from
    staging — no separate lobs join required here. Missing (office,class)
    in the seed → factor 1.0 (multiplicative pass-through, documented).
    """
    if not tags:
        return ylt

    factor_cols = [forecast_factor_col(tag) for tag in tags]
    factor_exprs = [_forecast_factor_expr(tag) for tag in tags]
    ff_wide = (
        forecast_factors
        .group_by(FF.OFFICE, FF.CLASS)
        .agg(factor_exprs)
        .rename({FF.CLASS: Y.LOB_CLASS})
    )

    ylt = (
        ylt
        .join(ff_wide, on=[Y.OFFICE, Y.LOB_CLASS], how="left")
        .with_columns([pl.col(col).fill_null(1.0) for col in factor_cols])
    )
    log.info(f"forecast: {len(tags)} factor columns attached — {[forecast_factor_col(t) for t in tags]}")
    return ylt

def attach_rank(
    ylt: pl.LazyFrame,
    *,
    n_sim: dict[VendorName, int] | None = None,
) -> pl.LazyFrame:
    """`rnk`, `rp`, and `rp_bucket` within (vendor, lob_id, region_peril_id).

    Ordered by loss DESC.

    `rnk` is used by `attach_euws` for rank-threshold overrides.

    `rp_bucket` selects the correct blending weight bucket per event:
      rp < 200   → 0
      200 ≤ rp < 1000  → 200  (1-in-200)
      1000 ≤ rp < 10000 → 1000 (1-in-1000)
      rp ≥ 10000 → 10000 (1-in-10000 tail)

    `rp` is derived from each row vendor's n_sim divided by rank:
      verisk:  n_sim['verisk'] / rnk  (typically 10_000)
      risklink: n_sim['risklink'] / rnk (typically 100_000)

    This is the same logic as jan-rollup's
    `int_vw_funnel_ylt_combined_ranked_bucketed`.

    `n_sim` defaults to {VERISK: 10_000, RISKLINK: 100_000}.
    """
    return _with_rank_and_rp_bucket(ylt, n_sim=n_sim)


def attach_euws(
    ylt: pl.LazyFrame,
    euws_rate_factors: pl.LazyFrame,
    euws_rank_overrides: pl.LazyFrame,
) -> pl.LazyFrame:
    """Per-event EUWS factor. Rank-threshold overrides come from
    `euws_rank_overrides` seed — add a row there to bypass EUWS for a
    new LOB without changing code. Missing events → 1.0."""
    euws = euws_rate_factors.select(
        pl.col(EU.MODEL_EVENT_ID),
        pl.col(EU.OCC_YEAR),
        pl.col(EU.FACTOR).alias(AF.EUWS_FACTOR),
    )
    overrides = euws_rank_overrides.select(
        pl.col(EO.ROLLUP_LOB),
        pl.col(EO.MAX_RANK),
        pl.col(EO.FACTOR).alias(_EUWS_OVERRIDE_TMP),
    )
    out = (
        ylt
        .join(euws, left_on=[Y.EVENT_ID, Y.YEAR_ID], right_on=[EU.MODEL_EVENT_ID, EU.OCC_YEAR], how="left")
        .join(overrides, on=Y.ROLLUP_LOB, how="left")
        .with_columns(
            pl.when(
                pl.col(_EUWS_OVERRIDE_TMP).is_not_null() &
                (pl.col(AF.RNK) <= pl.col(EO.MAX_RANK))
            )
            .then(pl.col(_EUWS_OVERRIDE_TMP))
            .otherwise(pl.col(AF.EUWS_FACTOR).fill_null(1.0))
            .alias(AF.EUWS_FACTOR)
            .cast(pl.Float64),
        )
        .drop(_EUWS_OVERRIDE_TMP, EO.MAX_RANK)
    )
    log.info("euws: factor attached, rank overrides applied from seed")
    return out


def _blend_weights_by_peril_bucket(blending_weights: pl.LazyFrame) -> pl.LazyFrame:
    """Read wide blend weights into the runtime peril/RP bucket shape."""
    return (
        blending_weights
        .select(
            pl.col(BW.PERIL_ID).alias(Y.REGION_PERIL_ID),
            pl.col(BW.RETURN_PERIOD).alias(AF.RP_BUCKET),
            pl.col(BW.SUB_PERIL),
            pl.col(BW.VERISK_WEIGHT).alias(AF.VK_PROPORTION),
            pl.col(BW.RISKLINK_WEIGHT).alias(AF.RL_PROPORTION),
        )
    )


def _base_model_by_peril(blending_weights: pl.LazyFrame) -> pl.LazyFrame:
    return (
        blending_weights
        .select(
            pl.col(BW.PERIL_ID).alias(Y.REGION_PERIL_ID),
            pl.col(BW.SUB_PERIL),
            pl.col(BW.BASE_MODEL),
        )
        .unique(subset=[Y.REGION_PERIL_ID, BW.SUB_PERIL])
    )


def _attach_blend_inputs(ylt: pl.LazyFrame, blending_weights: pl.LazyFrame) -> pl.LazyFrame:
    weights = _blend_weights_by_peril_bucket(blending_weights)
    specific_weights = (
        weights
        .filter(pl.col(BW.SUB_PERIL).is_not_null())
        .rename({
            BW.SUB_PERIL: Y.MODELLED_REGION_PERIL,
            AF.VK_PROPORTION: _SPECIFIC_VK_PROP,
            AF.RL_PROPORTION: _SPECIFIC_RL_PROP,
        })
    )
    generic_weights = (
        weights
        .filter(pl.col(BW.SUB_PERIL).is_null())
        .select(Y.REGION_PERIL_ID, AF.RP_BUCKET, AF.VK_PROPORTION, AF.RL_PROPORTION)
        .rename({
            AF.VK_PROPORTION: _GENERIC_VK_PROP,
            AF.RL_PROPORTION: _GENERIC_RL_PROP,
        })
    )

    base_models = _base_model_by_peril(blending_weights)
    specific_base_models = (
        base_models
        .filter(pl.col(BW.SUB_PERIL).is_not_null())
        .rename({BW.SUB_PERIL: Y.MODELLED_REGION_PERIL, BW.BASE_MODEL: _SPECIFIC_BASE_MODEL})
    )
    generic_base_models = (
        base_models
        .filter(pl.col(BW.SUB_PERIL).is_null())
        .select(Y.REGION_PERIL_ID, BW.BASE_MODEL)
        .rename({BW.BASE_MODEL: _GENERIC_BASE_MODEL})
    )

    return (
        ylt
        .join(specific_weights, on=[Y.REGION_PERIL_ID, AF.RP_BUCKET, Y.MODELLED_REGION_PERIL], how="left")
        .join(generic_weights, on=[Y.REGION_PERIL_ID, AF.RP_BUCKET], how="left")
        .join(specific_base_models, on=[Y.REGION_PERIL_ID, Y.MODELLED_REGION_PERIL], how="left")
        .join(generic_base_models, on=Y.REGION_PERIL_ID, how="left")
        .with_columns(
            pl.coalesce(pl.col(_SPECIFIC_VK_PROP), pl.col(_GENERIC_VK_PROP), pl.lit(0.5))
              .cast(pl.Float64)
              .alias(AF.VK_PROPORTION),
            pl.coalesce(pl.col(_SPECIFIC_RL_PROP), pl.col(_GENERIC_RL_PROP), pl.lit(0.5))
              .cast(pl.Float64)
              .alias(AF.RL_PROPORTION),
            pl.coalesce(pl.col(_SPECIFIC_BASE_MODEL), pl.col(_GENERIC_BASE_MODEL), pl.col(Y.VENDOR))
              .cast(pl.String)
              .alias(AF.BASE_MODEL),
        )
        .drop(
            _SPECIFIC_VK_PROP, _GENERIC_VK_PROP,
            _SPECIFIC_RL_PROP, _GENERIC_RL_PROP,
            _SPECIFIC_BASE_MODEL, _GENERIC_BASE_MODEL,
        )
    )


def _attach_vendor_aals(
    ylt: pl.LazyFrame,
    n_sim: dict[VendorName, int],
) -> pl.LazyFrame:
    group = [Y.LOB_ID, Y.REGION_PERIL_ID]
    n_sim_vk = float(n_sim[VendorName.VERISK])
    n_sim_rl = float(n_sim[VendorName.RISKLINK])
    return ylt.with_columns(
        (pl.when(pl.col(Y.VENDOR) == VendorName.VERISK).then(pl.col(Y.LOSS)).otherwise(0.0)
           .sum().over(group) / n_sim_vk).alias(_VK_AAL_TMP),
        (pl.when(pl.col(Y.VENDOR) == VendorName.RISKLINK).then(pl.col(Y.LOSS)).otherwise(0.0)
           .sum().over(group) / n_sim_rl).alias(_RL_AAL_TMP),
    )


def _attach_uplift_factor(ylt: pl.LazyFrame) -> pl.LazyFrame:
    return (
        ylt
        .with_columns(
            (pl.col(AF.VK_PROPORTION) * pl.col(_VK_AAL_TMP) +
             pl.col(AF.RL_PROPORTION) * pl.col(_RL_AAL_TMP)).alias(_BLENDED_AAL_TMP),
            pl.when(pl.col(AF.BASE_MODEL) == VendorName.RISKLINK)
              .then(pl.col(_RL_AAL_TMP))
              .otherwise(pl.col(_VK_AAL_TMP))
              .alias(_BASE_MODEL_AAL_TMP),
            pl.col(Y.EVENT_ID).alias(AF.MODEL_EVENT_ID),
        )
        .with_columns(
            pl.when(pl.col(_BASE_MODEL_AAL_TMP) > 0)
              .then(pl.col(_BLENDED_AAL_TMP) / pl.col(_BASE_MODEL_AAL_TMP))
              .otherwise(pl.lit(1.0, dtype=pl.Float64))
              .cast(pl.Float64)
              .alias(AF.UPLIFT_FACTOR),
        )
        .with_columns(
            pl.col(AF.UPLIFT_FACTOR).clip(0.1, 10.0).alias(AF.UPLIFT_FACTOR_CAPPED),
        )
    )


def attach_uplift(
    ylt: pl.LazyFrame,
    blending_weights: pl.LazyFrame,
    *,
    n_sim: dict[VendorName, int] | None = None,
) -> pl.LazyFrame:
    """Blend proportions, base_model, and uplift_factor per (lob_id, region_peril_id).

    uplift_factor = blended_AAL / base_model_AAL, where:
        blended_AAL    = vk_proportion × vk_AAL + rl_proportion × rl_AAL
        base_model     = from blending_weights.base_model (per peril_id).
        base_model_AAL = AAL of whichever vendor is named as base_model

    AAL per (vendor, lob_id, region_peril_id) = sum(loss) / n_sim for that vendor.
    uplift_factor is capped 0.1–10× into uplift_factor_capped.
    Falls back to 1.0 when the base model has no events for a group.

    `n_sim` is keyed by `VendorName` enum. Defaults to
    {VERISK: 10_000, RISKLINK: 100_000} when omitted.

    Implementation: AAL is computed with window functions
    (`.sum().over(group)`) rather than a `group_by → join-back` collapse —
    the aggregate is broadcast to every event row in one pass.

    Blend-weights resolution:
        blending_weights is wide-format (peril_id, return_period, sub_peril,
        base_model, verisk_weight, risklink_weight). The join uses both peril_id
        and the event's rank-derived rp_bucket. `sub_peril=None` rows match any
        event (the YLT has no sub-peril dim today).

    Falls back to 0.5/0.5 blend and 1.0 uplift when blending_weights is empty.
    """
    n_sim_resolved = _resolve_n_sim(n_sim)
    out = (
        _with_rank_and_rp_bucket(ylt, n_sim=n_sim_resolved)
        .pipe(_attach_blend_inputs, blending_weights)
        .pipe(_attach_vendor_aals, n_sim_resolved)
        .pipe(_attach_uplift_factor)
        .drop(_VK_AAL_TMP, _RL_AAL_TMP, _BLENDED_AAL_TMP, _BASE_MODEL_AAL_TMP)
    )
    log.info("uplift: rp_bucket proportions + base_model from seed; AAL via window functions")
    return out
