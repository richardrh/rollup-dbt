"""EP-curve from YLT: AEP + OEP + AAL per (vendor, lob, peril).

Port of the dbt `ep_curve_from_ylt.sql` macro.
"""

from __future__ import annotations

import polars as pl

from rollup.schemas import frames as F
from rollup.schemas.columns import EpCurveCol as EP
from rollup.schemas.columns import EpType
from rollup.schemas.columns import NormalizedYltCol as Y
from rollup.validate import validate_schema


DEFAULT_RETURN_PERIODS: list[int] = [
    10000, 5000, 2000, 1000, 500, 250, 200, 150, 100, 50, 30, 20, 10, 5,
]

# Grouping key + dims that are constant within a key (first() == any()).
_KEY  = [Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID]
_DIMS = [Y.ROLLUP_LOB, Y.PERIL_NAME, Y.REGION, Y.PERIL_FAMILY, Y.CDS_CAT_CLASS_NAME]


def ep_curve_from_ylt(
    ylt: pl.LazyFrame,
    n_simulations: int,
    *,
    target_return_periods: list[int] | None = None,
) -> pl.LazyFrame:
    """Produce AEP + OEP + AAL from a NORMALIZED_YLT LazyFrame.

    AEP/OEP rows have rank_num∈[1..N] and return_period∈target_return_periods.
    AAL rows have rank_num=0 and return_period=0.
    """
    validate_schema(ylt, F.NORMALIZED_YLT, name="ep.ylt_input")
    rps = target_return_periods or DEFAULT_RETURN_PERIODS

    # Per-year aggregates: compute AEP (sum) and OEP (max) in one pass.
    per_year = (
        ylt
        .group_by([*_KEY, Y.YEAR_ID])
        .agg(
            pl.sum(Y.LOSS).alias(EpType.AEP),
            pl.max(Y.LOSS).alias(EpType.OEP),
            *[pl.first(c).alias(c) for c in _DIMS],
        )
    )

    # Unpivot AEP/OEP into long form, then assign deterministic row-order ranks
    # within (key, ep_type). Ties sort by year_id so repeated runs produce the
    # same return-period assignment.
    aep_oep = (
        per_year
        .unpivot(
            on=[EpType.AEP, EpType.OEP],
            index=[*_KEY, Y.YEAR_ID, *_DIMS],
            variable_name=EP.EP_TYPE,
            value_name=EP.ANNUAL_LOSS,
        )
        .sort([*_KEY, EP.EP_TYPE, EP.ANNUAL_LOSS, Y.YEAR_ID], descending=[False, False, False, False, True, False])
        .with_columns(
            pl.int_range(1, pl.len() + 1)
                .over([*_KEY, EP.EP_TYPE])
                .cast(pl.Int64)
                .alias(EP.RANK_NUM),
        )
        .with_columns(
            (pl.lit(n_simulations, dtype=pl.Float64) / pl.col(EP.RANK_NUM))
                .floor().cast(pl.Int64).alias(EP.RETURN_PERIOD),
        )
        .filter(pl.col(EP.RETURN_PERIOD).is_in(rps))
    )

    # AAL: total_loss / n_sims, one row per key. Rank/return-period fixed at 0.
    aal = (
        ylt
        .group_by(_KEY)
        .agg(
            *[pl.first(c).alias(c) for c in _DIMS],
            (pl.sum(Y.LOSS) / n_simulations).alias(EP.ANNUAL_LOSS),
        )
        .with_columns(
            pl.lit(EpType.AAL).alias(EP.EP_TYPE),
            pl.lit(0, dtype=pl.Int64).alias(EP.RANK_NUM),
            pl.lit(0, dtype=pl.Int64).alias(EP.RETURN_PERIOD),
        )
    )

    projection = [pl.col(n) for n in (*_KEY, *_DIMS, EP.EP_TYPE, EP.RANK_NUM, EP.RETURN_PERIOD, EP.ANNUAL_LOSS)]
    out = pl.concat([aep_oep.select(projection), aal.select(projection)])

    validate_schema(out, F.EP_CURVE, name="ep.ep_curve_output")
    return out
