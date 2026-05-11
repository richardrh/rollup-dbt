"""Factor stages.

Each factor is one function: takes a LazyFrame + the seed(s) it needs,
returns the LazyFrame with one or more new factor columns attached. The
composition in `pipeline.build_all_factors` calls them in order; the
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

  4. Call it from `pipeline.build_all_factors` in the right order (before
     metrics; usually: after fa_gross, before dialsup).

  5. Pick a column-name suffix (e.g. `_brokercomm`). In the metrics loop
     inside `build_all_factors`, chain the new factor in:
         loss_..._{tag}_euws_fagross_brokercomm =
           loss_..._{tag}_euws_fagross * broker_commission_factor
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
from rollup.schemas.columns import RefFineartAdjCol as FA
from rollup.schemas.columns import RefForecastFactorsCol as FF
from rollup.schemas.columns import RefFxRatesCol as FX


log = logging.getLogger("rollup.factors")

# Working / temporary column names — exist only inside one stage. Kept as
# module-level constants so every reference is traceable from `pl.col(...)`.
_EUWS_OVERRIDE_TMP   = "_euws_override"
_VK_AAL_TMP          = "_vk_aal"
_RL_AAL_TMP          = "_rl_aal"
_BLENDED_AAL_TMP     = "_blended_aal"
_BASE_MODEL_AAL_TMP  = "_base_model_aal"

# Return-period bucket thresholds — adapted from jan-rollup's
# int_vw_funnel_ylt_combined_ranked_bucketed. This pipeline collapses all
# tail events at rp >= 1000 into the 1-in-1000 blending tier, per the current
# seed contract: 0=AAL, 200=1-in-200, 1000=1-in-1000.


def _rp_bucket_expr(rp_col: str) -> pl.Expr:
    return (
        pl.when(pl.col(rp_col) < 200)
          .then(pl.lit(0))
          .when(pl.col(rp_col) < 1000)
          .then(pl.lit(200))
          .otherwise(pl.lit(1000))
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
    """Derives `required_currency` from `cds_cat_class_name`, then joins
    fx_rates to attach `rate_to_gbp`.

    Currency derivation: `cds_cat_class_name` MUST contain ` UK ` or ` EU `
    as a space-padded substring. UK → GBP, EU → EUR, anything else → GBP.
    Add a row to `fx_rates.csv` for every currency code in `CurrencyCode`,
    otherwise `validate_fx_coverage` (called at pipeline startup) will abort
    the run before any computation begins.
    """
    ylt = ylt.with_columns(
        pl.when(pl.col(Y.CDS_CAT_CLASS_NAME).str.contains(" UK "))
          .then(pl.lit(CurrencyCode.GBP))
          .when(pl.col(Y.CDS_CAT_CLASS_NAME).str.contains(" EU "))
          .then(pl.lit(CurrencyCode.EUR))
          .otherwise(pl.lit(CurrencyCode.GBP))
          .alias(AF.REQUIRED_CURRENCY),
    )
    fx_to_gbp = (
        fx_rates
        .filter(pl.col(FX.TARGET_CURRENCY) == CurrencyCode.GBP)
        .select(
            pl.col(FX.CURRENCY_CODE).alias(AF.REQUIRED_CURRENCY),
            pl.col(FX.RATE).alias(AF.RATE_TO_GBP),
        )
    )
    out = ylt.join(fx_to_gbp, on=AF.REQUIRED_CURRENCY, how="left")
    log.info("currency: required_currency derived from CDS class; rate_to_gbp attached")
    return out.with_columns(pl.col(AF.RATE_TO_GBP).cast(pl.Float64))


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
    for tag in tags:
        y_, m_ = int(tag[:4]), int(tag[4:6])
        col = forecast_factor_col(tag)
        ff = (
            forecast_factors
            .filter(
                (pl.col(FF.FORECAST_DATE).dt.year()  == y_) &
                (pl.col(FF.FORECAST_DATE).dt.month() == m_)
            )
            .select(
                pl.col(FF.OFFICE),
                pl.col(FF.CLASS).alias(Y.LOB_CLASS),
                pl.col(FF.FACTOR).alias(col).cast(pl.Float64),
            )
        )
        ylt = (
            ylt.join(ff, on=[Y.OFFICE, Y.LOB_CLASS], how="left")
               .with_columns(pl.col(col).fill_null(1.0))
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
      rp ≥ 1000 → 1000 (1-in-1000)

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


def attach_fagross(ylt: pl.LazyFrame, fineart_adjustments: pl.LazyFrame) -> pl.LazyFrame:
    """Fine-art gross-to-net `aal_factor` + `tail_factor` per
    (lob_id, region_peril_id). Non-FA rows get factor=1.0 via fill_null."""
    fa = fineart_adjustments.select(
        pl.col(FA.LOB_ID),
        pl.col(FA.REGION_PERIL_ID),
        pl.col(FA.AAL_FACTOR).alias(AF.FA_GROSS_AAL_FACTOR),
        pl.col(FA.TAIL_FACTOR).alias(AF.FA_GROSS_TAIL_FACTOR),
    )
    out = (
        ylt.join(fa, on=[Y.LOB_ID, Y.REGION_PERIL_ID], how="left")
           .with_columns(
               pl.col(AF.FA_GROSS_AAL_FACTOR).fill_null(1.0).cast(pl.Float64),
               pl.col(AF.FA_GROSS_TAIL_FACTOR).fill_null(1.0).cast(pl.Float64),
           )
    )
    log.info("fa_gross: aal + tail factors attached (1.0 for non-FA rows)")
    return out


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
                         The derive_blending_weights step sets this to
                         "risklink" for FL perils and "verisk" otherwise.
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
        blending_weights is long-format (peril_id, return_period, sub_peril,
        vendor, base_model, weight). The join uses both peril_id and the
        event's rank-derived rp_bucket. `sub_peril=None` rows match any event
        (the YLT has no sub-peril dim today).

    Falls back to 0.5/0.5 blend and 1.0 uplift when blending_weights is empty.
    """
    n_sim_resolved = _resolve_n_sim(n_sim)
    n_sim_vk = float(n_sim_resolved[VendorName.VERISK])
    n_sim_rl = float(n_sim_resolved[VendorName.RISKLINK])

    ylt = _with_rank_and_rp_bucket(ylt, n_sim=n_sim_resolved)

    def _vendor_weights(v: VendorName, alias: AF) -> pl.LazyFrame:
        return (
            blending_weights
            .filter(pl.col(BW.VENDOR) == v)
            .select(
                pl.col(BW.PERIL_ID).alias(Y.REGION_PERIL_ID),
                pl.col(BW.RETURN_PERIOD).alias(AF.RP_BUCKET),
                pl.col(BW.WEIGHT).alias(alias),
            )
        )

    def _base_model_lookup() -> pl.LazyFrame:
        return (
            blending_weights
            .select(
                pl.col(BW.PERIL_ID).alias(Y.REGION_PERIL_ID),
                pl.col(BW.BASE_MODEL),
            )
            .unique(subset=[Y.REGION_PERIL_ID])
        )

    blend_per_peril = (
        _vendor_weights(VendorName.VERISK,   AF.VK_PROPORTION)
        .join(
            _vendor_weights(VendorName.RISKLINK, AF.RL_PROPORTION),
            on=[Y.REGION_PERIL_ID, AF.RP_BUCKET], how="full", coalesce=True,
        )
    )
    base_model_lookup = _base_model_lookup()

    group = [Y.LOB_ID, Y.REGION_PERIL_ID]

    out = (
        ylt
        .join(blend_per_peril, on=[Y.REGION_PERIL_ID, AF.RP_BUCKET], how="left")
        .join(base_model_lookup, on=Y.REGION_PERIL_ID, how="left")
        .with_columns(
            pl.col(AF.VK_PROPORTION).fill_null(0.5).cast(pl.Float64),
            pl.col(AF.RL_PROPORTION).fill_null(0.5).cast(pl.Float64),
            pl.col(AF.BASE_MODEL).fill_null(pl.col(Y.VENDOR)).cast(pl.String),
            (pl.when(pl.col(Y.VENDOR) == VendorName.VERISK).then(pl.col(Y.LOSS)).otherwise(0.0)
               .sum().over(group) / n_sim_vk).alias(_VK_AAL_TMP),
            (pl.when(pl.col(Y.VENDOR) == VendorName.RISKLINK).then(pl.col(Y.LOSS)).otherwise(0.0)
               .sum().over(group) / n_sim_rl).alias(_RL_AAL_TMP),
        )
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
        .drop(_VK_AAL_TMP, _RL_AAL_TMP, _BLENDED_AAL_TMP, _BASE_MODEL_AAL_TMP)
    )
    log.info("uplift: rp_bucket proportions + base_model from seed; AAL via window functions")
    return out
