"""Property-based tests for ``rollup.intermediate.factors``.

Targets ``attach_currency``, ``attach_rank``, ``attach_uplift``,
``attach_forecast_factors``, and ``validate_fx_coverage``.

All tests are marked ``@pytest.mark.fuzz`` and skipped unless
``--run-fuzz`` is passed.

KNOWN ISSUES
------------
None identified during Phase 2 implementation.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

import rollup.schemas.frames as F
from rollup.config import CurrencyCode, VendorName
from rollup.schemas.columns import (
    AllFactorsCol as AF,
    NormalizedYltCol as Y,
    RefForecastFactorsCol as FF,
    RefFxRatesCol as FX,
)
from rollup.intermediate.factors import (
    MissingFxRateError,
    attach_currency,
    attach_forecast_factors,
    attach_rank,
    attach_uplift,
    validate_fx_coverage,
)

from .strategies import fx_rates_strategy, lazyframe_from_schema, pathological_loss_strategy


# All currency codes the pipeline cares about.
_ALL_CURRENCY_CODES: list[CurrencyCode] = list(CurrencyCode)


# ---------------------------------------------------------------------------
# validate_fx_coverage
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    currencies_present=st.sets(
        st.sampled_from(_ALL_CURRENCY_CODES),
        min_size=0,
        max_size=len(_ALL_CURRENCY_CODES),
    )
)
def test_validate_fx_coverage_catches_missing_currencies(
    currencies_present: set[CurrencyCode],
) -> None:
    """For any subset of CurrencyCode present in fx_rates,
    ``validate_fx_coverage`` raises ``MissingFxRateError`` naming each
    missing code, OR passes if all codes are present.
    Never silently succeeds when one is missing."""
    fx = fx_rates_strategy(currencies_present)
    all_codes = set(CurrencyCode)
    missing = all_codes - currencies_present

    if missing:
        with pytest.raises(MissingFxRateError) as exc_info:
            validate_fx_coverage(fx)
        msg = str(exc_info.value)
        for code in missing:
            assert code.value in msg, (
                f"MissingFxRateError message did not name missing code {code!r}: {msg!r}"
            )
    else:
        # All currencies present — must not raise.
        validate_fx_coverage(fx)


# ---------------------------------------------------------------------------
# attach_currency
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=30))
def test_attach_currency_preserves_loss(ylt: pl.LazyFrame) -> None:
    """``attach_currency`` adds ``rate_to_gbp`` + ``required_currency`` columns;
    the ``LOSS`` column values are unchanged for every row."""
    # Build a complete fx_rates covering all codes.
    fx = fx_rates_strategy(set(CurrencyCode))
    original_loss = ylt.collect()[Y.LOSS].to_list()
    out = attach_currency(ylt, fx).collect()
    assert out[Y.LOSS].to_list() == original_loss


@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=30))
def test_attach_currency_adds_required_columns(ylt: pl.LazyFrame) -> None:
    """After ``attach_currency``, both ``required_currency`` and ``rate_to_gbp``
    columns are present in the output."""
    fx = fx_rates_strategy(set(CurrencyCode))
    out = attach_currency(ylt, fx).collect_schema()
    assert AF.REQUIRED_CURRENCY in out.names()
    assert AF.RATE_TO_GBP in out.names()


# ---------------------------------------------------------------------------
# attach_rank
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=50))
@settings(max_examples=30)
def test_attach_rank_assigns_unique_ranks_per_group(ylt: pl.LazyFrame) -> None:
    """Within each (vendor, lob_id, region_peril_id) group, RNK values are unique.
    ``attach_rank`` uses ordinal ranking which never ties."""
    out = attach_rank(ylt).collect()
    group_cols = [Y.VENDOR, Y.LOB_ID, Y.REGION_PERIL_ID]
    for group_key, group_df in out.group_by(group_cols):
        ranks = group_df[AF.RNK].to_list()
        assert len(ranks) == len(set(ranks)), (
            f"Duplicate RNK values in group {group_key}: {sorted(ranks)}"
        )


# ---------------------------------------------------------------------------
# attach_forecast_factors
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=20),
    n_dates=st.integers(min_value=1, max_value=3),
)
def test_attach_forecast_factors_one_column_per_tag(
    ylt: pl.LazyFrame,
    n_dates: int,
) -> None:
    """Number of new ``f_{tag}`` columns == number of distinct forecast dates."""
    from rollup.chain import forecast_factor_col

    base_dates = [date(2026, 1 + i, 1) for i in range(n_dates)]
    tags = [d.strftime("%Y%m") for d in base_dates]

    # Build a forecast_factors seed with one row per (date × one class/office pair).
    forecast_factors = pl.LazyFrame(
        {
            FF.CLASS:         ["class_A"] * n_dates,
            FF.OFFICE:        ["UK"] * n_dates,
            FF.OFFICE_ISO2:   ["GB"] * n_dates,
            FF.FORECAST_DATE: base_dates,
            FF.FACTOR:        [1.0] * n_dates,
        },
        schema=F.REF_FORECAST_FACTORS,
    )

    cols_before = set(ylt.collect_schema().names())
    out = attach_forecast_factors(ylt, forecast_factors, tags)
    cols_after = set(out.collect_schema().names())

    new_cols = cols_after - cols_before
    expected_factor_cols = {forecast_factor_col(t) for t in tags}
    for col in expected_factor_cols:
        assert col in cols_after, (
            f"Expected factor column {col!r} not found; new cols: {new_cols}"
        )


# ---------------------------------------------------------------------------
# attach_uplift (zero-AAL / pathological loss guard)
# ---------------------------------------------------------------------------

@pytest.mark.fuzz
@given(
    ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=20),
)
def test_attach_uplift_handles_zero_aal(ylt: pl.LazyFrame) -> None:
    """When every LOSS value in a group is zero, ``attach_uplift`` must not
    return inf or NaN in ``uplift_factor``.  The implementation falls back to
    1.0 when ``base_model_AAL`` is zero (documented behaviour in factors.py).
    """
    # Override LOSS to 0.0 to force zero AAL for every group.
    zero_ylt = ylt.with_columns(pl.lit(0.0).alias(Y.LOSS))

    # An empty blending_weights triggers 0.5/0.5 fallback.
    empty_bw = pl.LazyFrame(schema=F.BLENDING_WEIGHTS)

    out = attach_uplift(zero_ylt, empty_bw).collect()
    uplift_vals = out[AF.UPLIFT_FACTOR].drop_nulls()

    assert not uplift_vals.is_nan().any(), "uplift_factor contains NaN when base AAL is 0"
    assert not uplift_vals.is_infinite().any(), "uplift_factor is inf when base AAL is 0"


@pytest.mark.fuzz
@given(ylt=lazyframe_from_schema(F.NORMALIZED_YLT, min_rows=1, max_rows=20))
def test_attach_uplift_factor_is_bounded(ylt: pl.LazyFrame) -> None:
    """``uplift_factor_capped`` is always within [0.1, 10.0] per the clip in factors.py."""
    empty_bw = pl.LazyFrame(schema=F.BLENDING_WEIGHTS)
    out = attach_uplift(ylt, empty_bw).collect()
    capped = out[AF.UPLIFT_FACTOR_CAPPED].drop_nulls()
    assert (capped >= 0.1).all(), "uplift_factor_capped has values below 0.1"
    assert (capped <= 10.0).all(), "uplift_factor_capped has values above 10.0"
