"""Report model: end-of-run EP summary at target return periods.

For every Hisco fan-out variant the pipeline produces, compute the headline
actuarial summary at three grains:

    * total          — one row per (variant, ep_type, rp)
    * rollup_lob     — one row per (variant, rollup_lob, ep_type, rp)
    * peril          — one row per (variant, peril_name, ep_type, rp)

The function returns a long DataFrame; the writer in
`rollup.io.report_writer` turns it into the operator-facing CSV/xlsx.

EP semantics — kept consistent with `rollup.staging.ep`:

    AAL  = sum(metric) / n_sim
    AEP  = aggregate-per-year (sum)  ranked DESC, rp = floor(n_sim / rank)
    OEP  = max-per-year (max)        ranked DESC, rp = floor(n_sim / rank)

`n_sim` is per-vendor (Verisk 10 000, RiskLink 100 000 by default). Each
variant is restricted to its vendor's rows via the same `base_model` filter
that the Hisco fan-out uses, so vendor N is unambiguous within a variant.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence

import polars as pl

from rollup.schemas.columns import AllFactorsCol as AF
from rollup.schemas.columns import EpType
from rollup.marts.variants import VariantSpec


log = logging.getLogger("rollup.report")


DEFAULT_TARGET_RETURN_PERIODS: tuple[int, ...] = (100, 200, 500, 1000)


# Output column names — module-level so callers (writers, tests) share one source.
REPORT_VARIANT     = "variant"
REPORT_GRAIN       = "grain"
REPORT_GROUP_KEY   = "group_key"
REPORT_EP_TYPE     = "ep_type"
REPORT_RP          = "rp"
REPORT_VALUE       = "value"

GRAIN_TOTAL      = "total"
GRAIN_ROLLUP_LOB = "rollup_lob"
GRAIN_PERIL      = "peril"

# (grain_name, group_col_or_None). None ⇒ aggregate across everything.
_GRAINS: tuple[tuple[str, str | None], ...] = (
    (GRAIN_TOTAL,      None),
    (GRAIN_ROLLUP_LOB, AF.ROLLUP_LOB),
    (GRAIN_PERIL,      AF.PERIL_NAME),
)

_PER_YEAR_LOSS = "_per_year_loss"


def build_report(
    all_factors: pl.LazyFrame,
    variants: Sequence[VariantSpec],
    *,
    target_return_periods: Sequence[int] = DEFAULT_TARGET_RETURN_PERIODS,
) -> pl.DataFrame:
    """Long-format DataFrame with one row per (variant, grain, group_key, ep_type, rp)."""
    rps = sorted(set(target_return_periods))
    pieces: list[pl.DataFrame] = []
    for variant in variants:
        per_variant = (
            all_factors
            .filter(pl.col(AF.BASE_MODEL) == variant.vendor.name)
            .select(
                pl.col(AF.YEAR_ID),
                pl.col(AF.ROLLUP_LOB),
                pl.col(AF.PERIL_NAME),
                pl.col(variant.loss_metric).alias(REPORT_VALUE),
            )
        )
        for grain_name, group_col in _GRAINS:
            df = _ep_at_grain(per_variant, group_col, n_sim=variant.vendor.n_simulations, rps=rps)
            pieces.append(
                df.with_columns(
                    pl.lit(variant.name).alias(REPORT_VARIANT),
                    pl.lit(grain_name).alias(REPORT_GRAIN),
                ).select(
                    REPORT_VARIANT, REPORT_GRAIN, REPORT_GROUP_KEY,
                    REPORT_EP_TYPE, REPORT_RP, REPORT_VALUE,
                )
            )

    out = pl.concat(pieces, how="vertical").sort(
        REPORT_VARIANT, REPORT_GRAIN, REPORT_GROUP_KEY, REPORT_EP_TYPE, REPORT_RP,
    )
    log.info(f"report: {out.height:,} rows across {len(variants)} variants × {len(_GRAINS)} grains")
    return out


def _ep_at_grain(
    per_variant: pl.LazyFrame,
    group_col: str | None,
    *,
    n_sim: int,
    rps: Sequence[int],
) -> pl.DataFrame:
    """Compute AAL + AEP/OEP for one variant at one grain.

    Returns columns: group_key, ep_type, rp, value.
    """
    # Bring grain to a canonical name so the rest of the function has one shape.
    if group_col is None:
        per_variant = per_variant.with_columns(pl.lit("").alias(REPORT_GROUP_KEY))
    else:
        per_variant = per_variant.with_columns(pl.col(group_col).alias(REPORT_GROUP_KEY))

    # Per (group, year) aggregates: AEP=sum, OEP=max — one pass over events.
    per_year = (
        per_variant
        .group_by([REPORT_GROUP_KEY, AF.YEAR_ID])
        .agg(
            pl.sum(REPORT_VALUE).alias(EpType.AEP),
            pl.max(REPORT_VALUE).alias(EpType.OEP),
        )
    )

    # AAL: sum_per_year(AEP) / n_sim, one row per group.
    aal = (
        per_year
        .group_by(REPORT_GROUP_KEY)
        .agg((pl.sum(EpType.AEP) / pl.lit(float(n_sim))).alias(REPORT_VALUE))
        .with_columns(
            pl.lit(EpType.AAL).alias(REPORT_EP_TYPE),
            pl.lit(0, dtype=pl.Int64).alias(REPORT_RP),
        )
        .select(REPORT_GROUP_KEY, REPORT_EP_TYPE, REPORT_RP, REPORT_VALUE)
    )

    # Tail: rank within (group, ep_type) DESC by value, derive rp, keep target rps.
    tail = (
        per_year
        .unpivot(
            on=[EpType.AEP, EpType.OEP],
            index=[REPORT_GROUP_KEY, AF.YEAR_ID],
            variable_name=REPORT_EP_TYPE,
            value_name=REPORT_VALUE,
        )
        .sort(
            [REPORT_GROUP_KEY, REPORT_EP_TYPE, REPORT_VALUE, AF.YEAR_ID],
            descending=[False, False, True, False],
        )
        .with_columns(
            pl.int_range(1, pl.len() + 1)
              .over([REPORT_GROUP_KEY, REPORT_EP_TYPE])
              .cast(pl.Int64)
              .alias("_rank"),
        )
        .with_columns(
            (pl.lit(float(n_sim)) / pl.col("_rank")).floor().cast(pl.Int64).alias(REPORT_RP),
        )
        .filter(pl.col(REPORT_RP).is_in(list(rps)))
        # Multiple ranks can floor to the same RP (e.g. rank 4 and rank 5
        # both yield rp=2 when n_sim=10). Conventionally the EP value at that
        # threshold is the worst-case observation — i.e. the lowest rank's value.
        .group_by([REPORT_GROUP_KEY, REPORT_EP_TYPE, REPORT_RP])
        .agg(pl.max(REPORT_VALUE).alias(REPORT_VALUE))
        .select(REPORT_GROUP_KEY, REPORT_EP_TYPE, REPORT_RP, REPORT_VALUE)
    )

    return pl.concat([aal.collect(), tail.collect()], how="vertical")
