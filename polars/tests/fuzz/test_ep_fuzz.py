"""Property-based tests for ``rollup.staging.ep``.

Targets ``ep_curve_from_ylt``.

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.

NOTE: When running with ``-n N`` (pytest-xdist), Hypothesis's shared-random
state can cause flaky failures in tests using ``st.sets()`` of dates.
Run single-threaded (``-q`` without ``-n``) for deterministic CI results.
"""

from __future__ import annotations

import polars as pl
import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

import rollup.schemas.frames as F
from rollup.schemas.columns import (
    EpCurveCol as EP,
    EpType,
    NormalizedYltCol as Y,
)
from rollup.staging.ep import DEFAULT_RETURN_PERIODS, ep_curve_from_ylt
from rollup.validate import validate_schema

from .strategies import lazyframe_from_schema


_N_SIMULATIONS = 10_000


# ---------------------------------------------------------------------------
# Schema invariant
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=10, max_rows=100))
@settings(max_examples=30)
def test_ep_curve_output_matches_schema(ylt: pl.LazyFrame) -> None:
    """EP curve output schema matches ``EP_CURVE`` exactly."""
    out = ep_curve_from_ylt(ylt, n_simulations=_N_SIMULATIONS)
    validate_schema(out, F.EP_CURVE, name="ep_curve_fuzz")
    assert out.collect_schema() == F.EP_CURVE


# ---------------------------------------------------------------------------
# AAL row presence
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=10, max_rows=100))
@settings(max_examples=30)
def test_ep_curve_aal_row_present_per_group(ylt: pl.LazyFrame) -> None:
    """For every distinct (vendor, lob_id, region_peril_id) in the YLT,
    an AAL row (rank_num=0, ep_type='AAL', return_period=0) is present in the
    EP curve output."""
    out = ep_curve_from_ylt(ylt, n_simulations=_N_SIMULATIONS).collect()
    ylt_df = ylt.collect()

    expected_keys = (
        ylt_df.select([Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID])
        .unique()
        .sort([Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID])
    )

    aal_rows = out.filter(
        (pl.col(EP.EP_TYPE) == EpType.AAL) &
        (pl.col(EP.RANK_NUM) == 0) &
        (pl.col(EP.RETURN_PERIOD) == 0)
    ).select([EP.VENDOR, EP.LOB_ID, EP.REGION_PERIL_ID]).unique().sort(
        [EP.VENDOR, EP.LOB_ID, EP.REGION_PERIL_ID]
    )

    # Every key from the input YLT should appear as an AAL row.
    for row in expected_keys.iter_rows(named=True):
        match = aal_rows.filter(
            (pl.col(EP.VENDOR) == row[Y.VENDOR]) &
            (pl.col(EP.LOB_ID) == row[Y.LOB_ID]) &
            (pl.col(EP.REGION_PERIL_ID) == row[Y.REGION_PERIL_ID])
        )
        assert match.height == 1, (
            f"Missing AAL row for group ({row[Y.VENDOR]!r}, "
            f"{row[Y.LOB_ID]}, {row[Y.REGION_PERIL_ID]})"
        )


# ---------------------------------------------------------------------------
# Monotonicity: loss is non-increasing as rank_num decreases
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=20, max_rows=100))
@settings(max_examples=20, suppress_health_check=[HealthCheck.data_too_large, HealthCheck.too_slow])
def test_ep_curve_rp_is_monotone(ylt: pl.LazyFrame) -> None:
    """For every (vendor, lob_id, region_peril_id, ep_type) group, LOSS
    is monotone non-increasing as RANK_NUM increases (higher rank = more rare
    event = higher loss). RANK_NUM=0 is the AAL row — excluded from the
    monotonicity check.

    Monotonicity is expected from ordinal ranking applied to losses in
    descending order, filtered to target return periods only.
    """
    out = ep_curve_from_ylt(ylt, n_simulations=_N_SIMULATIONS).collect()

    # Only AEP and OEP rows carry the monotonicity property; AAL rows are skipped.
    curve_rows = out.filter(
        (pl.col(EP.EP_TYPE) != EpType.AAL) &
        (pl.col(EP.RANK_NUM) > 0)
    )

    group_cols = [EP.VENDOR, EP.LOB_ID, EP.REGION_PERIL_ID, EP.EP_TYPE]
    for group_key, group_df in curve_rows.group_by(group_cols):
        # Sort by RANK_NUM ascending (1, 2, 3 ... = most frequent first).
        sorted_group = group_df.sort(EP.RANK_NUM)
        losses = sorted_group[EP.LOSS].to_list()
        # Losses should be non-increasing (lower rank_num → rarer event → higher loss).
        # Since rank 1 is the highest-loss event and rank N is the lowest, losses
        # sorted by rank ascending must be non-increasing.
        for i in range(len(losses) - 1):
            assert losses[i] >= losses[i + 1] - 1e-9, (
                f"Non-monotone EP curve in group {group_key}: "
                f"loss at rank {i+1}={losses[i]} < loss at rank {i+2}={losses[i+1]}"
            )


# ---------------------------------------------------------------------------
# Return periods are a subset of target return periods
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=10, max_rows=100))
@settings(max_examples=30)
def test_ep_curve_return_periods_are_valid(ylt: pl.LazyFrame) -> None:
    """Non-AAL rows in the EP curve have return_period values drawn from the
    default target return periods list, or return_period=0 for AAL rows."""
    out = ep_curve_from_ylt(ylt, n_simulations=_N_SIMULATIONS).collect()

    valid_rps = set(DEFAULT_RETURN_PERIODS) | {0}
    actual_rps = set(out[EP.RETURN_PERIOD].unique().to_list())

    unexpected = actual_rps - valid_rps
    assert not unexpected, (
        f"EP curve contains unexpected return periods: {sorted(unexpected)}"
    )
